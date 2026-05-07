import os
import threading
from flask import Flask, render_template_string
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)
logs = []

@app.route('/')
def home():
    """Ruta base para verificar el estado del servicio worker."""
    return "Worker RPC funcionando"

@app.route('/monitor')
def monitor():
    """Renderiza la interfaz web de monitoreo para los mensajes RPC entrantes."""
    html = """
    <html>
    <head>
        <title>Monitor RPC</title>
        <meta http-equiv="refresh" content="2">
        <style>
            body { background: #111; color: #0f0; font-family: Arial; padding: 20px; }
            .log { background: #222; padding: 10px; margin: 5px; border-radius: 5px; }
        </style>
    </head>
    <body>
        <h1>Monitor Worker RPC</h1>
        <h3>Total mensajes: {{ total }}</h3>
        {% for log in logs %}
            <div class="log">{{ log }}</div>
        {% endfor %}
    </body>
    </html>
    """
    return render_template_string(html, logs=logs, total=len(logs))

def worker():
    """Inicializa la conexión con el broker AMQP y procesa las solicitudes de la cola."""
    try:
        connection = amqpstorm.Connection(
            hostname='horse.lmq.cloudamqp.com',
            username='jrynbgfw',
            password='fW12mOLo5JzmtTd_gJx83y04HCCxl3hS',
            virtual_host='jrynbgfw',
            port=5672
        )
        channel = connection.channel()

        channel.queue.declare(
            queue='vilef_rpc_queue',
            durable=False,
            auto_delete=False
        )

        def on_request(message):
            """Callback ejecutado al recibir un mensaje; emite la respuesta al cliente."""
            body = message.body
            log = f"[x] Recibido: {body}"
            print(log)
            logs.append(log)

            response = f"Procesado: {body}"
            response_message = Message.create(channel, response)
            response_message.correlation_id = message.correlation_id
            
            response_message.publish(routing_key=message.reply_to)
            message.ack()

        channel.basic.consume(on_request, queue='vilef_rpc_queue')
        
        print("[x] Worker conectado exitosamente. Esperando mensajes en 'vilef_rpc_queue'...")
        channel.start_consuming(to_tuple=False)
        
    except Exception as e:
        print(f"\n[!] Error en la ejecución del worker: {e}\n")

worker_thread = threading.Thread(target=worker)
worker_thread.daemon = True
worker_thread.start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)