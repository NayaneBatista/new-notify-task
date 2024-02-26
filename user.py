from flask import Flask, jsonify, request
import mysql.connector
from mysql.connector import Error
import pika
import threading

app = Flask(__name__)

# Configurações do banco de dados
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'dbenayan'
}

# Função para conectar ao banco de dados


def conectar_bd():
    try:
        conexao = mysql.connector.connect(**db_config)
        if conexao.is_connected():
            return conexao
    except Error as e:
        print("Erro ao conectar ao MySQL", e)

# Função para desconectar do banco de dados


def desconectar_bd(conexao):
    if conexao and conexao.is_connected():
        conexao.close()

# Rota para criar um usuário


@app.route('/usuarios', methods=['POST'])
def criar_usuario():
    dados = request.json
    conexao = conectar_bd()
    cursor = conexao.cursor()
    query = "INSERT INTO usuarios (nome, email) VALUES (%s, %s)"
    valores = (dados['nome'], dados['email'])

    try:
        cursor.execute(query, valores)
        conexao.commit()
        return jsonify({"mensagem": "Usuário criado com sucesso!"}), 201
    except Error as e:
        print(e)
        return jsonify({"mensagem": "Erro ao criar usuário"}), 500
    finally:
        cursor.close()
        desconectar_bd(conexao)

# Rota para listar todos os usuários


@app.route('/usuarios', methods=['GET'])
def listar_usuarios():
    conexao = conectar_bd()
    cursor = conexao.cursor(dictionary=True)
    query = "SELECT * FROM usuarios"

    try:
        cursor.execute(query)
        usuarios = cursor.fetchall()
        return jsonify(usuarios), 200
    except Error as e:
        print(e)
        return jsonify({"mensagem": "Erro ao listar usuários"}), 500
    finally:
        cursor.close()
        desconectar_bd(conexao)

# Rota para obter um usuário específico


@app.route('/usuarios/<int:usuario_id>', methods=['GET'])
def obter_usuario(usuario_id):
    conexao = conectar_bd()
    cursor = conexao.cursor(dictionary=True)
    query = "SELECT * FROM usuarios WHERE id = %s"
    valores = (usuario_id,)

    try:
        cursor.execute(query, valores)
        usuario = cursor.fetchone()
        return jsonify(usuario) if usuario else jsonify({"mensagem": "Usuário não encontrado"}), 404
    except Error as e:
        print(e)
        return jsonify({"mensagem": "Erro ao obter usuário"}), 500
    finally:
        cursor.close()
        desconectar_bd(conexao)

# Rota para atualizar um usuário


@app.route('/usuarios/<int:usuario_id>', methods=['PUT'])
def atualizar_usuario(usuario_id):
    dados = request.json
    conexao = conectar_bd()
    cursor = conexao.cursor()
    query = "UPDATE usuarios SET nome = %s, email = %s WHERE id = %s"
    valores = (dados['nome'], dados['email'], usuario_id)

    try:
        cursor.execute(query, valores)
        conexao.commit()
        return jsonify({"mensagem": "Usuário atualizado com sucesso!"}), 200
    except Error as e:
        print(e)
        return jsonify({"mensagem": "Erro ao atualizar usuário"}), 500
    finally:
        cursor.close()
        desconectar_bd(conexao)

# Rota para deletar um usuário


@app.route('/usuarios/<int:usuario_id>', methods=['DELETE'])
def deletar_usuario(usuario_id):
    conexao = conectar_bd()
    cursor = conexao.cursor()
    query = "DELETE FROM usuarios WHERE id = %s"
    valores = (usuario_id,)

    try:
        cursor.execute(query, valores)
        conexao.commit()
        return jsonify({"mensagem": "Usuário deletado com sucesso!"}), 200
    except Error as e:
        print(e)
        return jsonify({"mensagem": "Erro ao deletar usuário"}), 500
    finally:
        cursor.close()
        desconectar_bd(conexao)


# Funções do Flask e rotas

def callback(ch, method, properties, body):
    print(f" [x] Recebido {body}")


def iniciar_consumidor_rabbitmq():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='fila_dados')
    channel.basic_consume(queue='fila_dados',
                          on_message_callback=callback, auto_ack=True)

    print(' [*] Aguardando mensagens. Para sair pressione CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    # Iniciar thread para o RabbitMQ
    thread = threading.Thread(target=iniciar_consumidor_rabbitmq)
    thread.start()

    # Iniciar o servidor Flask
    app.run(debug=True, port=5000, use_reloader=False)
