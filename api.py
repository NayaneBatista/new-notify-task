from flask import Flask, request, jsonify, redirect, url_for
import requests

app = Flask(__name__)

# As  URLs devem apontar para onde seus microsserviços estão rodando


USUARIOS_SERVICE_URL = 'http://localhost:5000'
DADOS_SERVICE_URL = 'http://localhost:5001'

# Conexão com o user.py


@app.route('/usuarios', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/usuarios/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def usuarios_proxy(path):
    url = f"{USUARIOS_SERVICE_URL}/usuarios/{path}".rstrip('/')
    response = requests.request(
        method=request.method,
        url=url,
        headers={key: value for (key, value)
                 in request.headers if key != 'Host'},
        data=request.get_data(),
        allow_redirects=False)
    return (response.content, response.status_code, response.headers.items())

# Conexão com o data.py


@app.route('/dados', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/dados/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def dados_proxy(path):
    url = f"{DADOS_SERVICE_URL}/dados/{path}".rstrip('/')
    response = requests.request(
        method=request.method,
        url=url,
        headers={key: value for (key, value)
                 in request.headers if key != 'Host'},
        data=request.get_data(),
        allow_redirects=False)
    return (response.content, response.status_code, response.headers.items())


if __name__ == '__main__':
    app.run(port=8080)
