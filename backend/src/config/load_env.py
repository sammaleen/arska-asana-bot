import os 
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
redirect_uri = "http://localhost:5000/callback"
auth_url = "https://app.asana.com/-/oauth_authorize"

#telegram
bot_token = os.getenv("BOT_TOKEN")

    

 




