import json
import requests


def getAccessToken(config):
    params = f"?client={config['client_id']}&company={config['company_id']}&user={config['user']}&password={config['password']}"
    url = f"https://api.defontana.com/api/Auth"

    response = requests.get(f"{url}{params}")

    if response.status_code == 200:
        return response.json()['access_token']
    else:
        return None
