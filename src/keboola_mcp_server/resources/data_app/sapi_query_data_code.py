# ### INJECTED_CODE ####
# ### QUERY DATA FUNCTION ####
import os

import httpx
import pandas as pd


def query_data(query: str) -> pd.DataFrame:
    branch_id = os.environ.get('BRANCH_ID')
    workspace_id = os.environ.get('WORKSPACE_ID')
    kbc_url = os.environ.get('KBC_URL')
    token = os.environ.get('KBC_TOKEN')

    if not branch_id or not workspace_id or not kbc_url or not token:
        raise RuntimeError('Missing required environment variables: BRANCH_ID, WORKSPACE_ID, KBC_URL, KBC_TOKEN.')

    timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=None)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)

    with httpx.Client(timeout=timeout, limits=limits) as client:
        response = client.post(
            f'{kbc_url}/v2/storage/branch/{branch_id}/workspaces/{workspace_id}/query',
            json={'query': query},
            headers={
                'X-StorageAPI-Token': token,
                'Accept': 'application/json',
            },
        )
        response.raise_for_status()
        response_json = response.json()
        if response_json.get('status') == 'error':
            raise ValueError(f'Error when executing query "{query}": {response_json.get("message")}.')
        return pd.DataFrame(response_json['data']['rows'])


# ### END_OF_INJECTED_CODE ####
