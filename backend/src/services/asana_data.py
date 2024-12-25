import requests
import json
import mysql.connector
import pandas as pd

# getting user name with exchanged token during authorization 
def get_user_name(access_token):
    
    url = 'https://app.asana.com/api/1.0/users/me'
    headers = {'Authorization': f'Bearer {access_token}'}
    
    payload = {
        'opt_fields': 'name',
        'opt_pretty': True
    }
    
    response = requests.get(url, headers=headers, params=payload)
    status = response.status_code

    if status == 200:
        response_json = response.json()
        user_name = response_json['data']['name'] 
    else:
        print(f'error: {status}')
    
    return user_name


# extracting asana data 
