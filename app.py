import os
import threading
import time
from flask import Flask, render_template_string
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)
logs = []

AMQP_CONFIG = {
    "hostname": "horse.lmq.cloudamqp.com",
    "username": "jrynbgfw",
    "password": "fW12mOLo5JzmtTd_gJx83y04HCCxl3hS",
    "virtual_host": "jrynbgfw",
    "port": 5671,
    "ssl": True,
    "ssl_options": {"server_hostname": "horse.lmq.cloudamqp.com"}
}

@app.route('/')
def home():
    return "Worker RPC funcionando"

@app.route('/monitor')
def monitor():
    html = """
    <html><head><title>Monitor RPC</title><meta http-equiv="refresh" content="2">
    <style>body{background:#111;color:#0f0;font-family:Arial;padding:20px}
    .log{background:#222;padding:10px;margin:5px;border-radius:5px}</style></head>
    <body><h1>Monitor Worker RPC</h1><h3>Total mensajes: {{ total }}</h3>
    {% for log in logs %}<div class="log">{{ log }}</div>{% endfor %}
    </body></html>
    """
    return render_template_string(html, logs=logs, total=len(logs))

def on_request(message):
    """Callback ejecutado al recibir un mensaje."""
    body = message.body
    log = f"[x] Recibido: {body}"
    print(log)
    logs.append(log)

    response = f"Procesado: {body}"
    response_message = Message.create(message.channel, response)
    response_message.correlation_id = message.correlation_id
    response_message.publish(routing_key=message.reply_to)
    message.ack()

def worker_loop():
    """Worker con reconexión automática ante caídas."""
    while True:
        try:
            print("[*] Conectando al broker...")
            connection = amqpstorm.Connection(**AMQP_CONFIG)
            channel = connection.channel()
            channel.basic.qos(prefetch_count=1)

            channel.queue.declare(
                queue='new_rpc_queue_cloud',
                durable=True,
                auto_delete=False
            )

            channel.basic.consume(on_request, queue='new_rpc_queue_cloud')
            print("[x] Worker listo. Esperando mensajes...")
            channel.start_consuming(to_tuple=False)

        except amqpstorm.AMQPConnectionError as e:
            print(f"[!] Conexión perdida: {e}. Reintentando en 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"[!] Error inesperado: {e}. Reintentando en 5s...")
            time.sleep(5)

worker_thread = threading.Thread(target=worker_loop, daemon=True)
worker_thread.start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)