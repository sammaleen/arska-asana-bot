import os 
import json
import dotenv 

dotenv.load_dotenv()

# asana 
asana_token = os.getenv("ASANA_TOKEN")
workspace_gid = os.getenv("WORKSPACE_GID")  
team_gid = os.getenv('TEAM_GID')
portfolio_gid = os.getenv('PORTFOLIO_GID')  

# asana OAuth
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
redirect_uri = "https://arska-sammaleen.eu.pythonanywhere.com/callback"
auth_url = "https://app.asana.com/-/oauth_authorize"

# telegram
bot_token = os.getenv("BOT_TOKEN")
report_chat_id = os.getenv("REPORT_CHAT_ID") # main chat
report_chat_id_pm = os.getenv("REPORT_CHAT_ID_PM") # PM's chat
report_chat_id_ba = os.getenv("REPORT_CHAT_ID_BA") # BA's chat

# user lists from json
cwdir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(cwdir, '../../../'))
json_path = os.path.join(root_dir, 'users_to_skip.json')

with open('json_path', 'r', encoding='utf-8') as config_file:
    config = json.load(config_file)
    
pm_users = config.get('PM','')
ba_users = config.get('BA','')

print(pm_users, ba_users)

# database 
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
database = os.getenv("DATABASE")

# redis
rd_host = os.getenv("RD_HOST")
rd_port = int(os.getenv("RD_PORT"))
rd_pass = os.getenv("RD_PASS")
rd_user = os.getenv("RD_USER")
token_ttl = 518400 # store cache for 6 days

# misc
gs_url = os.getenv("GS_URL")

 


