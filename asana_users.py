import requests
import json
import pandas as pd
import os
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

#asana creds
asana_token = os.getenv('ASANA_TOKEN')
workspace_gid = os.getenv('WORKSPACE_GID')
team_gid = os.getenv('TEAM_GID')

#google sheets creds
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

#skip list
with open('users_to_skip.json', 'r', encoding='utf-8') as config_file:
    config = json.load(config_file)
users_to_skip = config.get('users_to_skip', '')


#asana_users_df
def get_users(asana_token, team_gid):
    
    url = f"https://app.asana.com/api/1.0/teams/{team_gid}/users"
    headers = {'Authorization': f'Bearer {asana_token}'}
    
    payload = {
    'opt_fields': 'name',
     'opt_pretty': True
     }
    
    response = requests.get(url, headers=headers, params=payload)
    users_json = response.json()
    users_df = pd.json_normalize(users_json['data'], max_level=1)
    
    rows_to_keep = []
    for _, row in users_df.iterrows():
        if row['name'] not in users_to_skip:
            rows_to_keep.append(row)
            
    users_df = pd.DataFrame(rows_to_keep).reset_index(drop=True)
    users_df = users_df.rename(columns={'gid':'user_gid'})                      
    users_df['idx'] = (users_df.index + 1).tolist()
    
    col_order = ['idx', 'user_gid', 'name']
    users_df = users_df[col_order]
    
    return users_df

users_df = get_users(asana_token, team_gid)
print(users_df)

#connect to asana_users GS table and update data

#get current gs instance 

table = client.open('asana_users')
sheet = table.worksheet('users')
gs_data = sheet.get('A:D')
gs_users = pd.DataFrame(gs_data[1:], columns=gs_data[0])

print(gs_users)

#compare gs_users and users_df

# update users_df with tokens from user_token

def upd_users_df(users_df, gs_users):
    
    users_df_upd = users_df.copy()
    users_df_upd['user_token'] = None
    
    for _, row in users_df_upd.iterrows():
        
        user = row['name']
        
        if user in gs_users['name']:
            row['user_token'] = gs_users[gs_users['name'] == user]['user_token']
        
    return users_df_upd

users_df_upd = upd_users_df(users_df, gs_users)
users_df_upd

