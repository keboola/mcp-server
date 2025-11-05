# ### INJECTED_CODE ####
# ### QUERY DATA FUNCTION ####
import os
import time

import httpx
import pandas as pd


def query_data(query: str) -> pd.DataFrame:
    branch_id = os.environ.get('BRANCH_ID')
    workspace_id = os.environ.get('WORKSPACE_ID')
    token = os.environ.get('KBC_TOKEN')
    kbc_url = os.environ.get('KBC_URL')

    if not branch_id or not workspace_id or not token or not kbc_url:
        raise RuntimeError('Missing required environment variables: BRANCH_ID, WORKSPACE_ID, KBC_TOKEN, KBC_URL.')

    query_service_url = kbc_url.replace('connection.', 'query.', 1).rstrip('/') + '/api/v1'
    headers = {
        'X-StorageAPI-Token': token,
        'Accept': 'application/json',
    }

    timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=None)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    with httpx.Client(timeout=timeout, limits=limits) as client:
        response = client.post(
            f'{query_service_url}/branches/{branch_id}/workspaces/{workspace_id}/queries',
            json={'statements': [query]},
            headers=headers,
        )
        response.raise_for_status()
        submission = response.json()
        job_id = submission.get('queryJobId')
        if not job_id:
            raise RuntimeError('Query Service did not return a job identifier.')

        start_ts = time.monotonic()
        while True:
            status_response = client.get(
                f'{query_service_url}/queries/{job_id}',
                headers=headers,
            )
            status_response.raise_for_status()
            job_info = status_response.json()
            status = job_info.get('status')
            if status in {'completed', 'failed', 'canceled'}:
                break
            if time.monotonic() - start_ts > 300:  # 5 minutes
                raise TimeoutError(f'Timed out waiting for query "{job_id}" to finish.')
            time.sleep(1)

        statements = job_info.get('statements') or []
        if not statements:
            raise RuntimeError('Query Service returned no statements for the executed query.')
        statement_id = statements[0]['id']

        results_response = client.get(
            f'{query_service_url}/queries/{job_id}/{statement_id}/results',
            headers=headers,
        )
        results_response.raise_for_status()
        results = results_response.json()

        if results.get('status') != 'completed':
            raise ValueError(f'Error when executing query "{query}": {results.get("message")}.')

        columns = [col['name'] for col in results.get('columns', [])]
        data_rows = [{col_name: value for col_name, value in zip(columns, row)} for row in results.get('data', [])]
        return pd.DataFrame(data_rows)


# ### END_OF_INJECTED_CODE ####
