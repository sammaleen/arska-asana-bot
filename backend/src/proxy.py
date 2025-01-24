from flask import Flask, request, jsonify
import requests

proxy_app = Flask(__name__)

# proxy the oauth callaback request to the main flask app in main.py
@proxy_app.route('/callback', methods=['GET'])
def callback():
    response = request.get("http://127.0.0.1:8000/callback", params=request.args)
    
    return (response.text, response.status_code, response.headers.items())
    