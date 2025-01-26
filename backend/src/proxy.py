import requests
from flask import Flask, request, Response

proxy_app = Flask(__name__)

@proxy_app.route('/callback', methods=['GET'])
def callback():
    url = "https://arska-sammaleen.eu.pythonanywhere.com/callback"
    params = request.args  # Forward query parameters
    headers = dict(request.headers)  # Convert headers to a mutable dictionary
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        content_type = response.headers.get('Content-Type', 'text/plain')
        return Response(response.content, status=response.status_code, content_type=content_type)
    except requests.RequestException as e:
        return Response(f"Error: {str(e)}", status=500, content_type='text/plain')
