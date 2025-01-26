import requests
from flask import Flask, request, Response

proxy_app = Flask(__name__)

@proxy_app.route('/callback', methods=['GET'])
def callback():
    url = "https://arska-sammaleen.eu.pythonanywhere.com/callback"
    params = request.args  
    headers = dict(request.headers)  # convert headers to a mutable dict
    response = requests.get(url, params=params, headers=headers) 
    content_type = response.headers.get('Content-Type', 'text/plain')
    return Response(response.content, status=response.status_code, content_type=content_type)
