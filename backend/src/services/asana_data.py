import requests
import json
import mysql.connector
import pandas as pd
from datetime import date
from datetime import datetime

from config.load_env import db_user, db_host, db_pass, database, token_ttl

import logging
logger = logging.getLogger(__name__)

from services.redis_client import get_redis_client
redis_client = get_redis_client()


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

        
# GET PERSONAL TOKEN AND GID from db 'users'
def get_user_data(user_name):
    
    conn = mysql.connector.connect(user = db_user,
                               password = db_pass,
                               #host = db_host,
                               host='127.0.0.1',
                               database = database)
    
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT user_gid, user_token FROM users WHERE name = %s", 
                   (user_name,))
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if result:
        return result.get('user_gid'), result.get('user_token')
    else:
        return None, None
    
    
# SAVE EXTRACTED TG and ASANA data to redis cache and table 'bot'
def save_asana_data(user_name, user_gid, user_token, user_id, tg_user):
    
    try:
        redis_key = f"user_data:{user_id}"
        redis_data = {"user_gid": user_gid, "tg_user": tg_user, "user_name": user_name, "user_token": user_token}
        redis_client.set(redis_key, json.dumps(redis_data), ex=token_ttl)
        
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
            INSERT INTO bot (user_id, tg_user, user_name, user_token, user_gid, date_added)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            tg_user = VALUES(tg_user), user_name = VALUES(user_name), user_token = VALUES(user_token), date_added = VALUES(date_added)
            """,
            (user_id, tg_user, user_name, user_token, user_gid, date.today())
        )
        conn.commit()
        
        logger.info(f"asana data saved for {user_name}/{user_id}")
        return True
    
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return False
    
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
    
    
# GET REDIS DATA FROM CACHE
def get_redis_data(user_id):
    
    cursor = None
    conn = None
    
    try:
        redis_key = f"user_data:{user_id}"
        cached_data = redis_client.get(redis_key)  # check redis cache for token
        
        if cached_data:
            user_data = json.loads(cached_data)
            
            user_gid = user_data.get('user_gid')
            user_name = user_data.get('user_name')
            user_token = user_data.get('user_token')
            tg_user = user_data.get('tg_user')
            
            logger.info(f"user data retrieved from Redis cache for user: {user_id}")
            return user_gid, user_name, user_token, tg_user
        
        # fallback to db 'bot' table
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            #host = db_host,
            host='127.0.0.1',
            database=database
        )
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT tg_user, user_name, user_token, user_gid FROM bot WHERE user_id = %s",
                       (user_id,))
        result = cursor.fetchone()
        
        if result:
            user_gid = result['user_gid']
            user_name = result['user_name']
            user_token = result['user_token']
            tg_user = result['tg_user']
            
            # update redis cache
            redis_data = {"user_gid": user_gid, "tg_user": tg_user, "user_name": user_name, "user_token": user_token}
            redis_client.set(redis_key, json.dumps(redis_data), ex=token_ttl)
            logger.info(f"user data retrieved from DB and cached in Redis for user: {user_id}")
            return user_gid, user_name, user_token, tg_user
        
        return None, None, None, None
        
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return None, None, None, None
    
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
              
              
# GET TASKS FROM ASANA + CHECK NOTES FROM DB / extracting tasks for a user from today/сегодня section of mytask list
def get_tasks(user_id, workspace_gid):
    
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    
    #get list gid for my tasks board
    url = f"https://app.asana.com/api/1.0/users/{user_gid}/user_task_list"
    headers = {'Authorization': f'Bearer {user_token}'}
    
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
        'opt_fields': 'name, due_on, projects, projects.name, section.name, notes, assignee_section.name, permalink_url',
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
        my_tasks_df.drop('task_gid', axis=1, inplace=True)
        my_tasks_df['idx'] = (my_tasks_df.index + 1).tolist()  
        order = ['idx','project_name','task_name','due_on','notes','url']
        
        my_tasks_df = my_tasks_df[order]
        
    else:
        my_tasks_df = pd.DataFrame() 
        
    return my_tasks_df


# FORMAT mytasks message

def format_df(df, extra_note, max_len=None, max_note_len=None):
    
    current_date = datetime.now().strftime("%d %b %Y - %a")
    message = f"*{current_date}*\n\n"
    
    grouped_tasks = df.groupby('project_name') # group tasks by project
    
    for project, group in grouped_tasks:
        message += f"*{project if project else 'No project'}*\n"
        
        # reset idx, enumerate from 1
        for idx, row in enumerate(group.itertuples(), start=1):
            task = row.task_name
            url = row.url
            notes = row.notes if row.notes else '-'
            due = row.due_on if row.due_on else 'No DL'

            # crop notes if exceed max_note_len
            if len(notes) > max_note_len:
                notes = notes[:max_note_len - 3].rstrip() + " (...)"

            task_entry = f"{idx}. [{task}]({url}) · `{due}`\n{notes}\n"
            message += task_entry

        message += "\n" 

    if max_len and len(message) > max_len:
        message = message[:max_len].rstrip() + " (...)"

    if extra_note:
        if len(extra_note) > max_note_len:
            extra_note = extra_note[:max_note_len - 3].rstrip() + " (...)"
        #message += "\n" 
        message += f"*✲Note:*\n{extra_note}\n\n"
        
    return message


# CHECK NOTES from 'notes' bd
def get_note(user_id):
    
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    
    try:
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            #host = db_host,
            host='127.0.0.1',
            database=database
        )
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(
            """
            SELECT note
            FROM notes
            WHERE user_name = %s AND date_added = %s
            """,
            (user_name, date.today())
        )
        
        result = cursor.fetchone()
        if result:
            extra_note = result['note']
            return extra_note
        return None
        
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return None
    
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
    

# ADD NOTE to DB 'notes'
def store_note(note, user_id):
    
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    
    try:
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            #host = db_host,
            host='127.0.0.1',
            database=database
        )
        cursor = conn.cursor()
        
        cursor.execute(
            """INSERT INTO notes (user_name, note, date_added)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            note = VALUES(note), date_added = VALUES(date_added)
            """,
            (user_name, note, date.today())
        )
        conn.commit()
        
        logger.info(f"note saved for: {user_id}/{user_name}")
        return True
        
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return False
        
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        
         
# GET TASKS FROM DB + CHECK NOTES for additions
def get_tasks_dict():
    
    try:
        conn = mysql.connector.connect(
        user=db_user,
        password=db_pass,
        #host = db_host,
        host='127.0.0.1',
        database=database
        )
        
        # tasks data
        tasks_query = """
        SELECT project_name, user_name, task_name, due_on, notes, url
        FROM tasks 
        WHERE date_extracted = %s
        """
        params = (date.today(),)
        tasks_df = pd.read_sql(tasks_query, conn, params=params)
        print(tasks_df.columns)
        
        # notes data
        notes_query = """
        SELECT user_name, note
        FROM notes
        WHERE date_added = %s
        """
        params = (date.today(),)
        notes_df = pd.read_sql(notes_query, conn, params=params)
        
        logger.info(f"fetched tasks, notes data from DB")
        
        # form tasks_dict
        tasks_dict = {}
        
        if tasks_df is not None and not tasks_df.empty and notes_df is not None:
            users = tasks_df['user_name'].unique().tolist()
            
            for user in users:
                user_tasks = tasks_df[tasks_df['user_name'] == user]
                user_notes = notes_df[notes_df['user_name'] == user] if not notes_df.empty else None
                
                if user_notes is not None:
                    user_tasks['extra_note'] = user_notes['note'].values
                else:
                    #user_tasks['extra_note'] = ''
                    user_tasks['extra_note'] = [None] * len(user_tasks)
                    
                tasks_dict[user] = user_tasks.reset_index(drop=True)      
                
        return tasks_dict
            
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return None
    
    finally:
        if conn is not None:
            conn.close()


# FORMAT report messages 
def format_report(user_df, user, tg_user_name, max_len=None, max_note_len=None):
    
    current_date = datetime.now().strftime("%d %b %Y · %a")
    if tg_user_name:
        message = f"*{user}* @{tg_user_name}\n{current_date}\n\n"
    else:
        message = f"*{user}*\n{current_date}\n\n"
    
    grouped_tasks = user_df.groupby('project_name') # group tasks by project
    
    for project, group in grouped_tasks:
        message += f"*{project if project else 'No project'}*\n"
        
        # reset idx, enumerate from 1
        for idx, row in enumerate(group.itertuples(), start=1):
            task = row.task_name
            url = row.url
            notes = row.notes if row.notes else '-'
            due = row.due_on if row.due_on else 'No DL'

            # crop notes if exceed max_note_len
            if len(notes) > max_note_len:
                notes = notes[:max_note_len - 3].rstrip() + " (...)"

            task_entry = f"{idx}. [{task}]({url}) · `{due}`\n{notes}\n"
            message += task_entry

        message += "\n" 

    if max_len and len(message) > max_len:
        message = message[:max_len].rstrip() + " (...)"

    if 'extra_note' in user_df.columns and not user_df['extra_note'].isnull().iloc[0]: 
        extra_note = user_df['extra_note'].iloc[0]  
        
        if len(extra_note) > max_note_len:
            extra_note = extra_note[:max_note_len - 3].rstrip() + " (...)"
        message += f"*✲Note:*\n{extra_note}\n\n"
        
    return message


#GET TG USER
def get_tg_user(user_name):
    
    try:
        conn = mysql.connector.connect(
        user=db_user,
        password=db_pass,
        #host = db_host,
        host='127.0.0.1',
        database=database
        )
        
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(
            """
            SELECT tg_user
            FROM bot
            WHERE user_name = %s 
            LIMIT 1
            """,
            (user_name,)
        )
        
        result = cursor.fetchone()
        if result:
            tg_user_name = result['tg_user']
            return tg_user_name
        return None
        
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return None
    
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()