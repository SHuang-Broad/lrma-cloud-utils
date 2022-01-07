import datetime
from typing import List

import pytz
from firecloud import api as fapi
from firecloud.errors import FireCloudServerError

from src.terra.table_utils import add_one_set

local_tz = pytz.timezone('US/Eastern')


def __no_success_analysis(submission_metadata: dict) -> bool:
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
    failed_entities = set([job['submissionEntity']['entityName'] for job in filtered_down_jobs
                          if __no_success_analysis(job)])
    s = set(enames)
    fresh_entities = s - failed_entities
    return list(s.intersection(failed_entities).union(fresh_entities))


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
                                          expression=expression)
        if not response.ok:
            raise FireCloudServerError(response.status_code, response.text)

