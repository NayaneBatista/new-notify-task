import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='fila_dados')

channel.basic_publish(exchange='', routing_key='fila_dados', body='Mensagem')
print(" [x] Mensagem enviada para a fila 'fila_dados'")

connection.close()
