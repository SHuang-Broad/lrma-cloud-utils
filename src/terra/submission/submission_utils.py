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


def _update_config(to_be_updated: dict, new_config: dict, chain: List[str]) -> None:

    for k, v in new_config:
        if isinstance(v, dict):
            _update_config(v)
        else:
            new_config[k] = v


def change_workflow_config(ns: str, ws: str, workflow_name: str,
                           new_config: dict,
                           ) -> None:
    response = fapi.get_workspace_config(ns, ws, ns, workflow_name)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)

    to_be_updated = copy.deepcopy(response.json())
    _update_config(to_be_updated, new_config, chain=list())

    response = fapi.update_workspace_config(ns, ws, ns,
                                            configname=workflow_name, body=to_be_updated)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)

