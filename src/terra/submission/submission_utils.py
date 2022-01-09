import collections.abc
import copy
import datetime
from typing import List

import pytz
from firecloud import api as fapi
from firecloud.errors import FireCloudServerError

from ..table_utils import add_one_set

local_tz = pytz.timezone('US/Eastern')

"""
Example workflow config.
{'deleted': False,
 'inputs': {'Dummy.bai': 'this.aligned_bai', 'Dummy.bam': 'this.aligned_bam'},
 'methodConfigVersion': 3,
 'methodRepoMethod': {'methodUri': 'dockstore://github.com%2Fbroadinstitute%2Flong-read-pipelines%2FDummy/sh_dummy',
                      'sourceRepo': 'dockstore',
                      'methodPath': 'github.com/broadinstitute/long-read-pipelines/Dummy',
                      'methodVersion': 'sh_dummy'},
 'name': 'Dummy',
 'namespace': 'broad-firecloud-dsde-methods',
 'outputs': {},
 'prerequisites': {},
 'rootEntityType': 'clr-flowcell'}
"""


def _ready_for_reanalysis(submission_metadata: dict) -> bool:
    if 'Submitted' == submission_metadata['status']:
        if 'Running' in submission_metadata['workflowStatuses']:
            return False
        if 'Failed' in submission_metadata['workflowStatuses']:
            return True

    if 'Done' == submission_metadata['status']:
        return 'Succeeded' not in submission_metadata['workflowStatuses']


def analyzable_entities(ns: str, ws: str, workflow_name: str, etype: str, enames: List[str]) -> List[str]:
    """
    Given a homogeneous (in terms of etype) list of entities, return a sub-list of them who are not being analyzed,
    and/or haven't been successfully analyzed before.
    :param ns: namespace
    :param ws: workspace
    :param workflow_name: workflow name
    :param etype: entity type
    :param enames: list of entity names (assumed to have the same etype)
    :return: list of running jobs (as dict's) optionally filtered
    """
    response = fapi.list_submissions(ns, ws)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)

    jobs = response.json()
    filtered_down_jobs = [job for job in jobs
                          if workflow_name == job['methodConfigurationName']
                          and etype == job['submissionEntity']['entityType']]

    seen_entities = set([job['submissionEntity']['entityName'] for job in filtered_down_jobs])
    failed = set([job['submissionEntity']['entityName'] for job in filtered_down_jobs
                  if _ready_for_reanalysis(job)])

    s = set(enames)
    fresh = s.difference(seen_entities)
    redo = s.intersection(failed)

    return list(fresh.union(redo))


def verify_before_submit(ns: str, ws: str, workflow_name: str,
                         etype: str, enames: List[str],
                         use_callcache: bool,
                         batch_type_name: str or None, expression: str or None) -> None:
    """
    For a list of entities, conditionally submit a job: if the entity isn't being analyzed already.

    When there are multiple entities given in enames, one can specify an expression for batch submission.
    For example, say etype is 'sample', and enames are samples to be analysed with workflow BLAH.
    BLAH is configured in a way such that its root entity is 'sample', i.e. same as etype.
    In this case, "expression" can simply be "this.samples".
    This is intuitive if one has configured workflows on Terra whose root entity is "sample_set", but some inputs
    takes attributes of individual 'sample's.
    :param ns:
    :param ws:
    :param workflow_name:
    :param etype:
    :param enames:
    :param use_callcache:
    :param batch_type_name: type name of the resulting set, when batch submission mode is turned on
    :param expression: if not None, will submit all entities given in enames in one batch
                       Note that this will create a dummy set, for the purpose of batch submission.
    :return:
    """
    if 1 == len(enames) or expression is None:
        for e in analyzable_entities(ns, ws, workflow_name, etype, enames):
            response = fapi.create_submission(wnamespace=ns, workspace=ws, cnamespace=ns,
                                              config=workflow_name,
                                              entity=e,
                                              etype=etype,
                                              use_callcache=use_callcache)
            if response.ok:
                print(f"Submitted {e} submitted for analysis with {workflow_name}.")
            else:
                print(f"Failed to submit {e} for analysis with {workflow_name} due to \n {response.json()}")
    else:
        assert batch_type_name is not None, "When submitting in batching mode, batch_type_name must be specified"

        now_str = datetime.datetime.now(tz=local_tz).strftime("%Y-%m-%dT%H-%M-%S")
        dummy_set_name_following_terra_convention = f'{workflow_name}_{now_str}_lrmaCU'
        add_one_set(ns, ws,
                    etype=batch_type_name,
                    ename=dummy_set_name_following_terra_convention,
                    member_type=etype,
                    members=analyzable_entities(ns, ws, workflow_name, etype, enames),
                    attributes=None)
        response = fapi.create_submission(ns, ws, cnamespace=ns, config=workflow_name,
                                          entity=dummy_set_name_following_terra_convention, etype=batch_type_name,
                                          expression=expression,
                                          use_callcache=use_callcache)
        if not response.ok:
            raise FireCloudServerError(response.status_code, response.text)


def _update_config(old_config: dict, new_config: dict) -> dict:

    for k, v in new_config.items():
        if isinstance(v, collections.abc.Mapping):
            old_config[k] = _update_config(old_config.get(k, {}), v)
        else:
            old_config[k] = v
    return old_config


def change_workflow_config(ns: str, ws: str, workflow_name: str,
                           new_root_entity_type: str = None,
                           new_input_names_and_values: dict = None,
                           new_branch: str = None) -> dict:
    """
    Supporting common--but currently limited--scenarios where one wants to update a config of a workflow.
    Note that does not many any effort in making sure the new configurations make sense.
    That is, one could potentially mismatch the workflow with wrong root entities,
    and/or providing non-existent branches.
    It's the user's responsibility to make sure the input values are correct.

    The old config is returned, in case this is a one-time change and one wants to immediately revert back the config,
    once something is done with the new config (e.g. run an one-off analysis).
    If this indeed is the case, checkout restore_workflow_config(...) in this module.
    :param ns:
    :param ws:
    :param workflow_name:
    :param new_root_entity_type: when one wants to re-configure a workflow's root entity
    :param new_input_names_and_values: when one wants to re-configure some input values, and/or add new input values
    :param new_branch: when one wants to switch to a different branch, where supposedly the workflow is updated.
    :return:
    """
    if new_root_entity_type is None \
            and new_input_names_and_values is None \
            and new_branch is None:
        raise ValueError(f"Requesting to change config of workflow: {workflow_name}, but not changing anything.")

    response = fapi.get_workspace_config(ns, ws, ns, workflow_name)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)
    current_config = copy.deepcopy(response.json())

    updated = copy.deepcopy(current_config)
    if new_root_entity_type is not None:
        updated = _update_config(updated, {'rootEntityType': new_root_entity_type})
    if new_input_names_and_values is not None:
        updated_inputs = copy.deepcopy(updated['inputs'])
        updated_inputs.update(new_input_names_and_values)
        updated = _update_config(updated, {'inputs': updated_inputs})
    if new_branch is not None:
        updated_wdl_version = copy.deepcopy(updated['methodRepoMethod'])
        updated_wdl_version['methodVersion'] = new_branch
        updated_wdl_version['methodUri'] = '/'.join(updated_wdl_version['methodUri'].split('/')[:-1]) + '/' + new_branch
        updated = _update_config(updated, {'methodRepoMethod': updated_wdl_version})
    updated['methodConfigVersion'] = updated['methodConfigVersion'] + 1  # don't forget this

    response = fapi.update_workspace_config(ns, ws, ns,
                                            configname=workflow_name, body=updated)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)

    return current_config


def restore_workflow_config(ns: str, ws: str, workflow_name: str, old_config: dict) -> None:
    """
    Restore a config of the workflow to an old value.
    :param ns:
    :param ws:
    :param workflow_name:
    :param old_config:
    :return:
    """

    to_upload = copy.deepcopy(old_config)
    response = fapi.get_workspace_config(ns, ws, ns, workflow_name)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)
    to_upload['methodConfigVersion'] = response.json()['methodConfigVersion'] + 1

    response = fapi.update_workspace_config(ns, ws, ns,
                                            configname=workflow_name, body=to_upload)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)


