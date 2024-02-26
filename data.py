import pika
from flask import Flask, jsonify, request
from bson import json_util
from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)

# Conexão com o MongoDB


client = MongoClient('mongodb://localhost:27017/')
db = client.nome_do_banco_mongodb
colecao = db.nome_da_colecao

# Integração Rabbit


def enviar_mensagem_rabbitmq(mensagem):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='fila_dados')

    channel.basic_publish(exchange='', routing_key='fila_dados', body=mensagem)
    print(" [x] Mensagem enviada para a fila 'fila_dados'")
    connection.close()

# Rota para criar um novo item (POST)


@app.route('/dados', methods=['POST'])
def criar_dado():
    dado = request.json
    colecao.insert_one(dado)
    # Enviar uma mensagem após a criação do dado
    enviar_mensagem_rabbitmq("Novo dado criado")
    return jsonify({"mensagem": "Dado criado com sucesso!"}), 201


# Função auxiliar para converter ObjectId para string
def converter_objectid(dado):
    if isinstance(dado, list):
        return [converter_objectid(item) for item in dado]
    if isinstance(dado, dict):
        for key, value in dado.items():
            if isinstance(value, ObjectId):
                dado[key] = str(value)
        return dado
    return dado

# Rota para listar todos os itens (GET)


@app.route('/dados', methods=['GET'])
def listar_dados():
    dados = list(colecao.find())
    dados_convertidos = converter_objectid(dados)
    return jsonify(dados_convertidos), 200

# Rota para obter um item específico (GET)


@app.route('/dados/<string:dado_id>', methods=['GET'])
def obter_dado(dado_id):
    dado = colecao.find_one({"_id": dado_id})
    return jsonify(dado) if dado else jsonify({"mensagem": "Dado não encontrado"}), 404

# Rota para atualizar um item (PUT)


@app.route('/dados/<string:dado_id>', methods=['PUT'])
def atualizar_dado(dado_id):
    atualizacao = request.json
    resultado = colecao.update_one({"_id": dado_id}, {"$set": atualizacao})
    return jsonify({"mensagem": "Dado atualizado com sucesso!"}) if resultado.modified_count > 0 else jsonify({"mensagem": "Nenhuma alteração realizada"}), 200

# Rota para deletar um item (DELETE)


@app.route('/dados/<string:dado_id>', methods=['DELETE'])
def deletar_dado(dado_id):
    resultado = colecao.delete_one({"_id": dado_id})
    return jsonify({"mensagem": "Dado deletado com sucesso!"}) if resultado.deleted_count > 0 else jsonify({"mensagem": "Dado não encontrado"}), 404


if __name__ == '__main__':
    app.run(debug=True, port=5001)
