import requests
from flask import Flask, request, Response

proxy_app = Flask(__name__)

@proxy_app.route('/callback', methods=['GET'])
def callback():

    #url = "http://127.0.0.1:5000/callback"
    url = "https://arska-sammaleen.eu.pythonanywhere.com/callback"

    params = request.args
    headers = dict(request.headers)  # convert headers to a mutable dict
    
    try:
        # send GET request to the flask app
        response = requests.get(url, params=params, headers=headers, timeout=20)
        content_type = response.headers.get('Content-Type', 'text/plain')
        
        # return the response from the flask app
        return Response(response.content, status=response.status_code, content_type=content_type)
    
    except requests.exceptions.RequestException as err:
        return f"Error: {str(err)}", 500

