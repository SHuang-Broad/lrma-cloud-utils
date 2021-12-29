from firecloud import api as fapi
from firecloud.errors import FireCloudServerError
from typing import List


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


def verify_before_submit(ns: str, ws: str, workflow_name: str, etype: str, enames: List[str],
                         use_callcache: bool) -> None:
    """
    For a list of entities, conditionally submit a job: if the entity isn't being analyzed already.
    """
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

