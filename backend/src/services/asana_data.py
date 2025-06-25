import requests
import json
import html
import mysql.connector
import redis
import redis.exceptions
import pandas as pd
from datetime import datetime, date

from config.load_env import team_gid, asana_token, db_user, db_host, db_pass, database, token_ttl, pm_users, ba_users, av_users

import logging
logger = logging.getLogger(__name__)

from services.redis_client import get_redis_client
redis_client = get_redis_client()

# GET all users and store in redis
def get_asana_users(asana_token, team_gid):
    
    asana_users = []
    
    url = f"https://app.asana.com/api/1.0/teams/{team_gid}/users"
    headers = {'Authorization': f'Bearer {asana_token}'}
    
    payload = {
        'opt_fields': 'name'
        }
    
    try:
        response = requests.get(url, headers=headers, params=payload)
        response.raise_for_status()
        
        users_json = response.json()
        users_df = pd.json_normalize(users_json['data'], max_level=1)
        asana_users = users_df['gid'].tolist()
        
    except requests.exceptions.RequestException as err:
        logging.error(f"network errror: {err} trying fetching Asana username")
    
    return asana_users


# GET USER NAME with exchanged access token during auth
def get_user_name(access_token):
    
    url = 'https://app.asana.com/api/1.0/users/me'
    headers = {'Authorization': f'Bearer {access_token}'}
    
    payload = {
        'opt_fields': 'name'
    }
    
    try:
        response = requests.get(url, headers=headers, params=payload)
        response.raise_for_status()
        
        response_json = response.json()
        user_name = response_json.get('data',{}).get('name')
        return user_name
    
    except requests.exceptions.RequestException as err:
        logging.error(f"network errror: {err} trying fetching Asana username")
        return None


#GET USER GID
def get_user_gid(access_token):
    
    url = 'https://app.asana.com/api/1.0/users/me'
    headers = {'Authorization': f'Bearer {access_token}'}
    
    payload = {
        'opt_fields': 'name'
    }
    
    try:
        response = requests.get(url, headers=headers, params=payload)
        response.raise_for_status()
        
        response_json = response.json()
        user_name = response_json.get('data',{}).get('name')
        user_gid = response_json.get('data',{}).get('gid')
        logging.info(f"get user_gid for user: {user_name}")
        return user_gid
    
    except requests.exceptions.RequestException as err:
        logging.error(f"network errror: {err} trying fetching user_gid")
        return None
    
    
# GET PERSONAL TOKEN AND NAME from db 'users'
def get_user_data(user_gid):
    
    conn = None
    cursor = None
    
    try:
        conn = mysql.connector.connect(
            user = db_user,
            password = db_pass,
            host = db_host,
            database = database,
            charset='utf8mb4'
            )
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(
            "SELECT name, user_token FROM users WHERE user_gid = %s", 
            (user_gid,)
            )
        result = cursor.fetchone()
        
        if result:
            return result.get('name'), result.get('user_token')
        else:
            return None, None
        
    except mysql.connector.Error as err:
        logging.error(f"DB error: {err} for user: {user_gid}")
        return None, None
        
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        
    
# SAVE EXTRACTED TG and ASANA data to redis cache and table 'bot'
def save_asana_data(user_name, user_gid, user_token, user_id, tg_user):
    
    conn = None
    cursor = None
    
    try:
        redis_key = f"user_data:{user_id}"
        redis_data = {"user_gid": user_gid, 
                      "tg_user": tg_user, 
                      "user_name": user_name,
                      "user_token": user_token}
        
        redis_client.set(redis_key, json.dumps(redis_data, ensure_ascii=False), ex=token_ttl)
        
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            host = db_host,
            database=database,
            charset='utf8mb4'
        )
        cursor = conn.cursor()
    
        cursor.execute(
            """
            INSERT INTO bot (user_id, tg_user, user_name, user_token, user_gid, date_added)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            tg_user = %s,
            user_name = %s,
            user_token = %s,
            date_added = %s
            """,
            (user_id, tg_user, user_name, user_token, user_gid, date.today(),
            tg_user, user_name, user_token, date.today())
        )
        conn.commit()
        
        logger.info(f"Asana data saved for: {user_name}/{user_id}")
        return True
    
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err} for user: {user_name}/{user_id}")
        return False
    
    except redis.exceptions.RedisError as err:
        logger.error(f"Redis error: {err} for user: {user_name}/{user_id}")
        return False
    
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
    
    
# GET REDIS DATA FROM CACHE
def get_redis_data(user_id):
    
    conn = None
    cursor = None
    
    try:
        # checking redis cache
        redis_key = f"user_data:{user_id}"
        cached_data = redis_client.get(redis_key) 
        
        if cached_data:
            user_data = json.loads(cached_data)
            
            user_gid = user_data.get('user_gid')
            user_name = user_data.get('user_name')
            user_token = user_data.get('user_token')
            tg_user = user_data.get('tg_user')
            
            logger.info(f"user data retrieved from Redis cache for user: {user_name}/{user_id}")
            return user_gid, user_name, user_token, tg_user
        
        # fallback to db 'bot' table
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            host = db_host,
            database=database
        )
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
                       SELECT tg_user, user_name, user_token, user_gid 
                       FROM bot 
                       WHERE user_id = %s
                       """,
                       (user_id,))
        result = cursor.fetchone()
        
        if result:
            user_gid = result['user_gid']
            user_name = result['user_name']
            user_token = result['user_token']
            tg_user = result['tg_user']
            
            # update redis cache
            redis_data = {"user_gid": user_gid, "tg_user": tg_user, "user_name": user_name, "user_token": user_token}
            redis_client.set(redis_key, json.dumps(redis_data, ensure_ascii=False))
            
            logger.info(f"user data retrieved from DB and cached in Redis for user: {user_id}/{user_name}")
            return user_gid, user_name, user_token, tg_user
        
        logger.info(f"no data is found for user: {user_id}")
        return None, None, None, None
        
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return None, None, None, None
    
    except redis.exceptions.RedisError as err:
        logger.error(f"Redis error: {err} for user: {user_id}")
        return None, None, None, None
    
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
            
              
# EXTRACTING PROJECT NAMES
def extract_projects(task_gid, user_token):

    while task_gid:
        
        # fetch data on a single task
        url = f'https://app.asana.com/api/1.0/tasks/{task_gid}'
        headers = {'Authorization': f'Bearer {user_token}'}
        
        payload = {
            'opt_fields': 'projects, projects.name, parent, parent.name'
        }
        
        try:
            response = requests.get(url, headers=headers, params=payload)
            response.raise_for_status()
            
            response_json = response.json()
            data = response_json.get('data', {})
            
            # project names
            projects = data.get('projects', [])
            project_names = [p['name'] for p in projects] if projects else []
            
            # parent gids
            parent = data.get('parent')
            parent_gid = parent.get('gid') if parent else None
            
            if project_names:
                return project_names
            if parent_gid:
                task_gid = parent_gid
            else:
                return []
            
        except requests.exceptions.RequestException as err:
            logging.error(f"network error {err} while extracting project names for task: {task_gid}")
            return None
    return []
    
    
# GET TASKS FROM ASANA + CHECK NOTES FROM DB / extracting tasks for a user from today/сегодня section of mytask list
def get_tasks(user_id, workspace_gid):
    
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    
    if user_gid is None or user_token is None:
        logging.error(f"unable to retrieve Asana creds for user: {user_id}")
        return pd.DataFrame()
    
    # get list from asana
    url = f"https://app.asana.com/api/1.0/users/{user_gid}/user_task_list"
    headers = {'Authorization': f'Bearer {user_token}'}
    
    payload = {
        'workspace': workspace_gid,
        'opt_fields': '',
        'opt_pretty': True  
        }
    
    try: 
        response = requests.get(url, headers=headers, params=payload)
        response.raise_for_status()
        
        response_json = response.json()
        list_gid = response_json.get('data',{}).get('gid')
    
    except requests.exceptions.RequestException as err:
        logging.error(f"network errror: {err} trying fetching Asana list_gid for user: {user_name}/{user_id}")
        return pd.DataFrame()
    
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
            logging.error(f"error: {response.status_code} while fetching Asana tasks for user: {user_name}/{user_id}")
            break
        
    if my_tasks:
        my_tasks_df = pd.json_normalize(my_tasks, max_level=3) 
        
        my_tasks_df.rename(columns={'gid':'task_gid',
                                    'name':'task_name',
                                    'permalink_url':'url',
                                    'projects':'project_name'}, inplace=True)
    
        # filter tasks from today sections
        if 'assignee_section.name' in my_tasks_df.columns:
            my_tasks_df = my_tasks_df[
                my_tasks_df['assignee_section.name'].str.lower().isin(['today', 'сегодня', 'фокус'])
                ]
          
        # extracting project names from nested list - if tasks belong to multiple projects
        def dicts_to_names(x):
            if isinstance(x, list):
                return [p.get('name', '') for p in x if 'name' in p]
            return []
        
        if 'project_name' in my_tasks_df.columns:
            my_tasks_df['project_name'] = my_tasks_df['project_name'].apply(dicts_to_names)
        
        # extracting project names from parent tasks
        for idx, task_row in my_tasks_df.iterrows():
            proj = task_row['project_name']
            task_gid = task_row['task_gid']
            
            if not proj:
                project_names = extract_projects(task_gid, user_token)
                if project_names:
                    my_tasks_df.at[idx, 'project_name'] = project_names

        logging.info(f"mytasks data retrieved for user: {user_name}")
    else:
        my_tasks_df = pd.DataFrame() 
     
    return my_tasks_df


# FORMAT mytasks message
def format_df(df, extra_note, max_len=None, max_note_len=None):
    
    current_date = datetime.now().strftime("%d %b %Y · %a")
    message = f"<b>{current_date}</b>\n\n"
    
    # convert project list to str
    df['project_name'] = df['project_name'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                                                  
    # group tasks by project_name 
    grouped_tasks = df.groupby('project_name')
    
    for project, group in grouped_tasks:
        if isinstance(project, list):
            project_name = ', '.join(project)  
        else:
            project_name = project if project else 'No project'
 
        project_escaped = html.escape(project_name)
        message += f"━\n<b>{project_escaped}</b>\n"

        # sort tasks by due date
        sorted_group = sorted(
            group.itertuples(),
            key=lambda row: datetime.strptime(row.due_on, '%Y-%m-%d') if row.due_on else datetime.max
        )
        
        for idx, row in enumerate(sorted_group, start=1):
            
            # escape all fields to avoid HTML issues
            task_escaped = html.escape(row.task_name)
            url_escaped = html.escape(row.url)
            notes = html.escape(row.notes) if row.notes else '-' 
            #notes = html.escape(getattr(row, 'notes', '-')) if hasattr(row, 'notes') else '-'
            due = html.escape(row.due_on) if row.due_on else 'No DL'

            # format due date if available
            if due != 'No DL':
                due_date = datetime.strptime(due, '%Y-%m-%d')
                due = due_date.strftime("%d-%m-%Y")

            # truncate notes if necessary
            if max_note_len and len(notes) > max_note_len:
                notes = notes[:max_note_len - 3].rstrip() + " (...)"

            task_entry = f'{idx}. <a href="{url_escaped}">{task_escaped}</a> · <code>{due}</code>\n{notes}\n\n'
            message += task_entry

        message += "\n"

    # truncate final message carefully if needed
    if max_len and len(message) > max_len:
        safe_cut = message.rfind('</a>', 0, max_len)
        
        if safe_cut != -1:
            message = message[:safe_cut+4] + " (...)"
        else:
            message = message[:max_len].rstrip() + " (...)"

    # handle extra note if provided
    if extra_note:
        extra_escaped = html.escape(extra_note)
        if max_note_len and len(extra_escaped) > max_note_len:
            extra_escaped = extra_escaped[:max_note_len - 3].rstrip() + " (...)"
        message += f"<b>✲ Note:</b>\n{extra_escaped}\n\n"
    
    return message


# CHECK NOTES from 'notes' bd
def get_note(user_id):
    
    conn = None
    cursor = None
    
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    
    if user_name is None:
        logging.error(f"unable to retrieve Asana creds for user: {user_id}")
        return None
    
    try:
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            host = db_host,
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
    
    conn = None
    cursor = None
    
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    
    if user_name is None:
        logging.error(f"unable to retrieve Asana creds for user: {user_id}")
        return False
    
    try:
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            host = db_host,
            database=database
        )
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO notes (user_name, note, date_added)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            note = VALUES(note), date_added = VALUES(date_added)
            """,
            (user_name, note, date.today())
        )
        conn.commit()
        
        logger.info(f"note {note} saved for: {user_name}/{user_id}")
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
def get_report(user_name, pm_users, ba_users, av_users):
    
    skip_users = pm_users + ba_users + av_users
    skip_users = [user.lower() for user in skip_users]
    
    logger.info(f"request for general report by {user_name}")
    
    conn = None
    
    try:
        conn = mysql.connector.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            database=database
        )
        
        # fetching tasks
        tasks_query = """
            SELECT project_name, user_name, task_name, due_on, notes, url
            FROM tasks
            WHERE date_extracted = %s
        """
        params = (date.today(),)
        tasks_df = pd.read_sql(tasks_query, conn, params=params)
        
        # convert due_on format to dd-mm-YYYY / No DL
        tasks_df['due_on'] = pd.to_datetime(tasks_df['due_on'], errors='coerce').dt.date
        tasks_df['due_on'] = tasks_df['due_on'].apply(
            lambda x: x.strftime("%d-%m-%Y") if pd.notnull(x) else "No DL"
        )

        # fetching notes
        notes_query = """
            SELECT user_name, note
            FROM notes
            WHERE date_added = %s
        """
        notes_df = pd.read_sql(notes_query, conn, params=params)
        
        # normalize project names
        if not tasks_df.empty and 'project_name' in tasks_df.columns:
            tasks_df['project_name'] = (
                tasks_df['project_name']
                .fillna('')  # replace NaN with empty str
                .astype(str) # ensure str
                .str.strip() # strip whitespace
            )
            tasks_df.loc[tasks_df['project_name'] == '', 'project_name'] = 'No project'
        
        # building tasks_dict with tasks_df of each user
        tasks_dict = {}
        
        if tasks_df is not None and not tasks_df.empty and notes_df is not None:
            users = tasks_df['user_name'].unique().tolist()
            
            for user in users:
                user_lower = user.lower()
                
                if user_lower in skip_users:
                    logger.info(f"general report, skipping user: {user}")
                    continue 
                
                # filter tasks and notes for the user
                user_tasks = tasks_df[tasks_df['user_name'] == user].copy()
                user_notes = notes_df[notes_df['user_name'] == user] if not notes_df.empty else None
                
                # merge notes into user_tasks as 'extra_note'
                if user_notes is not None and not user_notes.empty:
                    note_value = user_notes['note'].iloc[0]
                    user_tasks.loc[:, 'extra_note'] = note_value
                else:
                    user_tasks.loc[:, 'extra_note'] = None

                tasks_dict[user] = user_tasks.reset_index(drop=True)
        
        return tasks_dict

    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return None
    
    finally:
        if conn is not None:
            conn.close()



# REPORT FOR PM
def get_report_pm(user_name, pm_users):
    
    if not pm_users:
        logger.info("PM users list is empty")
        return None
    
    logger.info(f"request for PM report by {user_name}")    
        
    conn = None
        
    try:
        conn = mysql.connector.connect(
        user=db_user,
        password=db_pass,
        host = db_host,
        database=database
        )
        
        pm_user_names = ', '.join(['%s'] * len(pm_users))
        
        # tasks data
        tasks_query = (
            f"""
            SELECT project_name, user_name, task_name, due_on, notes, url
            FROM tasks 
            WHERE date_extracted = %s
            AND user_name IN ({pm_user_names})
            """
            )
        params = (date.today(),) + tuple(pm_users)
        tasks_df = pd.read_sql(tasks_query, conn, params=params)
        
        # convert due_on format to dd-mm-YYYY / No DL
        tasks_df['due_on'] = pd.to_datetime(tasks_df['due_on'], errors='coerce').dt.date
        tasks_df['due_on'] = tasks_df['due_on'].apply(
            lambda x: x.strftime("%d-%m-%Y") if pd.notnull(x) else "No DL"
        )
        
        # notes data
        notes_query = (
            f"""
            SELECT user_name, note
            FROM notes
            WHERE date_added = %s
            AND user_name IN ({pm_user_names})
            """
            )
        notes_df = pd.read_sql(notes_query, conn, params=params)
        
        # normalize project names
        if not tasks_df.empty and 'project_name' in tasks_df.columns:
            tasks_df['project_name'] = (
                tasks_df['project_name']
                .fillna('')  # replace NaN with empty str
                .astype(str) # ensure str
                .str.strip() # strip whitespace
            )
            tasks_df.loc[tasks_df['project_name'] == '', 'project_name'] = 'No project'
        
           
        # build tasks_dict
        tasks_dict = {}
        
        if tasks_df is not None and not tasks_df.empty and notes_df is not None:
            users = tasks_df['user_name'].unique().tolist()
            
            for user in users:
                user_tasks = tasks_df[tasks_df['user_name'] == user]
                user_notes = notes_df[notes_df['user_name'] == user] if not notes_df.empty else None
                
                # merge notes into user_tasks as 'extra_note'
                if user_notes is not None and not user_notes.empty:
                    note_value = user_notes['note'].iloc[0]
                    user_tasks.loc[:, 'extra_note'] = note_value
                else:
                    user_tasks = user_tasks.copy()
                    user_tasks.loc[:, 'extra_note'] = None
                    
                tasks_dict[user] = user_tasks.reset_index(drop=True)      
                
        return tasks_dict
            
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return None
    
    finally:
        if conn is not None:
            conn.close()


# REPORT FOR BA
def get_report_ba(user_name, ba_users):
    
    if not ba_users:
        logger.info("BA users list is empty")
        return None
    
    logger.info(f"request for BA report by {user_name}")    
        
    conn = None
        
    try:
        conn = mysql.connector.connect(
        user=db_user,
        password=db_pass,
        host = db_host,
        database=database
        )
        
        ba_user_names = ', '.join(['%s'] * len(ba_users))
        
        # tasks data
        tasks_query = (
            f"""
            SELECT project_name, user_name, task_name, due_on, notes, url
            FROM tasks 
            WHERE date_extracted = %s
            AND user_name IN ({ba_user_names})
            """
            )
        params = (date.today(),) + tuple(ba_users)
        tasks_df = pd.read_sql(tasks_query, conn, params=params)
        
        # convert due_on format to dd-mm-YYYY / No DL
        tasks_df['due_on'] = pd.to_datetime(tasks_df['due_on'], errors='coerce').dt.date
        tasks_df['due_on'] = tasks_df['due_on'].apply(
            lambda x: x.strftime("%d-%m-%Y") if pd.notnull(x) else "No DL"
        )
        
        # notes data
        notes_query = (
            f"""
            SELECT user_name, note
            FROM notes
            WHERE date_added = %s
            AND user_name IN ({ba_user_names})
            """
            )
        notes_df = pd.read_sql(notes_query, conn, params=params)
        
        # normalize project names
        if not tasks_df.empty and 'project_name' in tasks_df.columns:
            tasks_df['project_name'] = (
                tasks_df['project_name']
                .fillna('')  # replace NaN with empty str
                .astype(str) # ensure str
                .str.strip() # strip whitespace
            )
            tasks_df.loc[tasks_df['project_name'] == '', 'project_name'] = 'No project'
        
        # build tasks_dict
        tasks_dict = {}
        
        if tasks_df is not None and not tasks_df.empty and notes_df is not None:
            users = tasks_df['user_name'].unique().tolist()
            
            for user in users:
                user_tasks = tasks_df[tasks_df['user_name'] == user]
                user_notes = notes_df[notes_df['user_name'] == user] if not notes_df.empty else None
                
                # merge notes into user_tasks as 'extra_note'
                if user_notes is not None and not user_notes.empty:
                    note_value = user_notes['note'].iloc[0]
                    user_tasks.loc[:, 'extra_note'] = note_value
                else:
                    user_tasks = user_tasks.copy()
                    user_tasks.loc[:, 'extra_note'] = None
                    
                tasks_dict[user] = user_tasks.reset_index(drop=True)      
                
        return tasks_dict
            
    except mysql.connector.Error as err:
        logger.error(f"DB error: {err}")
        return None
    
    finally:
        if conn is not None:
            conn.close()

# REPORT for AV
def get_report_av(user_name, av_users):
    
    if not av_users:
        logger.info("AV users list is empty")
        return None
    
    logger.info(f"request for AV report by {user_name}")    
        
    conn = None
        
    try:
        conn = mysql.connector.connect(
        user=db_user,
        password=db_pass,
        host = db_host,
        database=database
        )
        
        av_user_names = ', '.join(['%s'] * len(av_users))
        
        # tasks data
        tasks_query = (
            f"""
            SELECT project_name, user_name, task_name, due_on, notes, url
            FROM tasks 
            WHERE date_extracted = %s
            AND user_name IN ({av_user_names})
            """
            )
        params = (date.today(),) + tuple(av_users)
        tasks_df = pd.read_sql(tasks_query, conn, params=params)
        
        # convert due_on format to dd-mm-YYYY / No DL
        tasks_df['due_on'] = pd.to_datetime(tasks_df['due_on'], errors='coerce').dt.date
        tasks_df['due_on'] = tasks_df['due_on'].apply(
            lambda x: x.strftime("%d-%m-%Y") if pd.notnull(x) else "No DL"
        )
        
        # notes data
        notes_query = (
            f"""
            SELECT user_name, note
            FROM notes
            WHERE date_added = %s
            AND user_name IN ({av_user_names})
            """
            )
        notes_df = pd.read_sql(notes_query, conn, params=params)
        
        # normalize project names
        if not tasks_df.empty and 'project_name' in tasks_df.columns:
            tasks_df['project_name'] = (
                tasks_df['project_name']
                .fillna('')  # replace NaN with empty str
                .astype(str) # ensure str
                .str.strip() # strip whitespace
            )
            tasks_df.loc[tasks_df['project_name'] == '', 'project_name'] = 'No project'
        
        # build tasks_dict
        tasks_dict = {}
        
        if tasks_df is not None and not tasks_df.empty and notes_df is not None:
            users = tasks_df['user_name'].unique().tolist()
            
            for user in users:
                user_tasks = tasks_df[tasks_df['user_name'] == user]
                user_notes = notes_df[notes_df['user_name'] == user] if not notes_df.empty else None
                
                # merge notes into user_tasks as 'extra_note'
                if user_notes is not None and not user_notes.empty:
                    note_value = user_notes['note'].iloc[0]
                    user_tasks.loc[:, 'extra_note'] = note_value
                else:
                    user_tasks = user_tasks.copy()
                    user_tasks.loc[:, 'extra_note'] = None
                    
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
        message = f"<b>{user}</b> @{tg_user_name}\n{current_date}\n\n"
    else:
        message = f"<b>{user}</b>\n{current_date}\n\n"

    grouped_tasks = user_df.groupby('project_name')  # group tasks by project

    for project, group in grouped_tasks:
        project_name = project  
        message += f"━\n<b>{project_name}</b>\n"
        
        # sort tasks on due date
        def parse_due(x):
            try:
                return datetime.strptime(x, '%d-%m-%Y')
            except ValueError:
                # 'No DL' or invalid => treat as something far in the future
                return datetime.max

        sorted_group = sorted(group.itertuples(), key=lambda row: parse_due(row.due_on))

        # reset idx, enumerate from 1
        for idx, row in enumerate(sorted_group, start=1):
            task = row.task_name
            url = row.url
            notes = row.notes if row.notes else '-'  
            due = row.due_on if row.due_on else 'No DL'

            # crop notes if they exceed length limit
            if max_note_len and len(notes) > max_note_len:
                notes = notes[:max_note_len - 3].rstrip() + " (...)"

            # escape characters for HTML formatting
            task_escaped = (task.replace("<", "&lt;")
                                 .replace(">", "&gt;")
                                 .replace("&", "&amp;"))
            url_escaped = (url.replace("<", "&lt;")
                               .replace(">", "&gt;")
                               .replace("&", "&amp;"))

            task_entry = f"{idx}. <a href='{url_escaped}'>{task_escaped}</a> · <code>{due}</code>\n{notes}\n\n"
            message += task_entry

        message += "\n"

    # crop the whole message if it exceeds max_len
    if max_len and len(message) > max_len:
        message = message[:max_len].rstrip() + " (...)"

    # handle extra notes
    if 'extra_note' in user_df.columns:
        first_note = user_df['extra_note'].dropna()
        
        if not first_note.empty:
            extra_note = first_note.iloc[0]
            
            # crop note if needed
            if max_note_len and len(extra_note) > max_note_len:
                extra_note = extra_note[:max_note_len - 3].rstrip() + " (...)"
            message += f"<b>✲ Note:</b>\n{extra_note}\n\n"

    return message

# FORMAT report for AV
def format_report_av(user_df, user, tg_user_name, max_len=None, max_note_len=None):
    def esc(text):
        return html.escape(text, quote=False)

    now = datetime.now().strftime("%d %b %Y · %a")
    if tg_user_name:
        header = f"<b>{esc(user)}</b> @{esc(tg_user_name)}\n{now}\n\n"
    else:
        header = f"<b>{esc(user)}</b>\n{now}\n\n"

    segments = [header]

    # sort tasks on due date
    def parse_due(x):
        try:
            return datetime.strptime(x, "%d-%m-%Y")
        except:
            return datetime.max

    for project, group in user_df.groupby('project_name'):  # group tasks by project
        seg = f"━\n<b>{esc(project)}</b>\n"
        tasks = sorted(group.itertuples(), key=lambda row: parse_due(row.due_on))
        for idx, row in enumerate(tasks, start=1):  # reset idx, enumerate from 1
            seg += (
                f"{idx}. <a href=\"{esc(row.url)}\">{esc(row.task_name)}</a>"
                f" · <code>{esc(row.due_on or 'No DL')}</code>\n\n"
            )
        segments.append(seg)

    # handle extra notes
    if 'extra_note' in user_df.columns:
        extras = user_df['extra_note'].dropna()
        if not extras.empty:
            note = extras.iloc[0]
            if max_note_len and len(note) > max_note_len:  # crop note if needed
                note = note[:max_note_len - 3].rstrip() + " (...)"
            segments.append(f"<b>✲ Note:</b>\n{esc(note)}\n\n")

    # stitch into one or more messages
    messages = []
    current = ""
    for seg in segments:
        # if adding this segment would bust the limit, start a new message
        if max_len and len(current) + len(seg) > max_len:
            messages.append(current.rstrip())
            current = seg
        else:
            current += seg

    if current:
        messages.append(current.rstrip())

    return messages


#GET TG USER
def get_tg_user(user_name):
    
    conn = None
    cursor = None
    
    try:
        conn = mysql.connector.connect(
        user=db_user,
        password=db_pass,
        host = db_host,
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