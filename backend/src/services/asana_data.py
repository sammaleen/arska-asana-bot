import requests
import mysql.connector
import pandas as pd
from datetime import date

from config.load_env import db_user, db_host, db_pass, database, token_ttl

import redis
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

import logging
logger = logging.getLogger(__name__)


# GET USER NAME getting user name with exchanged token during authorization 
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


# GET TASKS / extracting today tasks for a user
def get_tasks(access_token, user_gid, workspace_gid):
    
    # get user's mytasks list gid
    url = f"https://app.asana.com/api/1.0/users/{user_gid}/user_task_list"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    payload = {
        'workspace': workspace_gid,
        'opt_fields': '',
        'opt_pretty': True  
        }
    
    response = requests.get(url, headers=headers, params=payload)
    status = response.status_code

    if status == 200:
        response_json = response.json()
    else:
        print(f'error: {status}')
    
    list_gid = response_json['data']['gid']
    
    # get my tasks for today
    my_tasks = []
    
    url = f"https://app.asana.com/api/1.0/user_task_lists/{list_gid}/tasks"
    
    payload = {
        'completed_since': 'now',
        'opt_fields': 'name, created_at, due_on, start_on, projects, projects.name, section.name, notes, assignee_section.name, created_by.name, created_by.gid, permalink_url',
        'limit': 100,
        'opt_pretty': True  
        }
    
     # pagination
    while True:
        response = requests.get(url, headers=headers, params=payload)
        
        if response.status_code == 200:
            json_data = response.json()
            
            if json_data.get('data'): 
                my_tasks.extend(json_data['data'])
            
            # check for more pages presence
            if json_data.get('next_page'): 
                payload['offset'] = json_data['next_page']['offset']  # update for next page
            else:
                break 
        else:
            print(f"error: {response.status_code}")
            break
        
    if my_tasks:
        my_tasks_df = pd.json_normalize(my_tasks, max_level=3) 
        my_tasks_df.rename(columns={'gid':'task_gid',
                                    'name':'task_name',
                                    'permalink_url':'url',
                                    'projects':'project_name'}, inplace=True)
    
        # SECTION NAME = TODAY и СЕГОДНЯ
        my_tasks_df = my_tasks_df[(my_tasks_df['assignee_section.name'] == 'Today') 
                                  | (my_tasks_df['assignee_section.name'] == 'Сегодня')]
        
        # extracting project names from nested list []
        if 'project_name' in my_tasks_df.columns:
            my_tasks_df['project_name'] = my_tasks_df['project_name'].apply(
                lambda x: x[0]['name'] if isinstance(x, list) and x else '')
        
        #re-order columns
        order = ['task_gid','project_name','task_name',
                    'start_on','due_on','notes',
                    'created_at','url','created_by.gid',
                    'created_by.name','assignee_section.gid','assignee_section.name']
        
        my_tasks_df = my_tasks_df[order]
        
    else:
        my_tasks_df = pd.DataFrame() 
        
    return my_tasks_df

        
# GET PERSONAL TOKEN from db - users
def get_user_token(user_name):
    
    conn = mysql.connector.connect(user = db_user,
                               password = db_pass,
                               #host = db_host,
                               host='127.0.0.1',
                               database = database)
    
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT user_token FROM users WHERE name = %s", 
                   (user_name,))
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if result:
        return result['user_token']
    else:
        return None
    
    
# SAVE EXTRACTED ASANA data to table 'bot'
def save_asana_data(user_name, user_token, user_id):
    
    try:
        redis_key = f"user_token:{user_id}"
        redis_client.set(redis_key, user_token, ex=token_ttl)
        
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            #host = db_host,
            host='127.0.0.1',
            database=database
        )
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO bot (user_id, user_name, user_token, date_added)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            user_token = VALUES(user_token), date_added = VALUES(date_added)
            """,
            (user_id, user_name, user_token, date.today())
        )
        conn.commit()
        
        logger.info(f"asana data saved for {user_name}/{user_id}")
        return True
    
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return False
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    
# GET TOKEN FROM CACHE
def get_cached_token(user_id):
    
    try:
        redis_key = f"user_token:{user_id}"
        
        # check redis cache for token
        user_token = redis_client.get(redis_key)
        
        if user_token:
            logger.info(f"token retrieved from redis cache for user: {user_id}")
            return user_token
        
        # fallback to db 'bot' table
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            #host = db_host,
            host='127.0.0.1',
            database=database
        )
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT user_token FROM bot WHERE user_id = %s",
                       (user_id,))
        result = cursor.fetchone()
        
        if result:
            user_token = result['user_token']
            
            # update redis cache
            redis_client.set(redis_key, user_token, ex=token_ttl)
            logger.info(f"token retrieved from db and cached in redis for user: {user_id}")
            return user_token
        
        return None 
        
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return None
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()