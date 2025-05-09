import threading
from time import sleep
from flask import Flask, request, jsonify
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)


class RpcClient(object):
    """Asynchronous Rpc client."""

    def __init__(self, host, username, password, rpc_queue):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue
        self.open()

    def open(self):
        """Open Connection."""
        self.connection = amqpstorm.Connection(self.host, self.username, self.password)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(self._on_response, no_ack=True, queue=self.callback_queue)
        self._create_process_thread()

    def _create_process_thread(self):
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        return message.correlation_id


@app.route('/rpc_call', methods=['POST'])
def rpc_call():
    """Handle file uploads and send them as RPC payloads."""

    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    file_content = file.read().decode('utf-8')  # Or .decode('latin1') depending on encoding

    corr_id = RPC_CLIENT.send_request(file_content)

    while RPC_CLIENT.queue[corr_id] is None:
        sleep(0.1)

    return jsonify({'response': RPC_CLIENT.queue[corr_id]})


if __name__ == '__main__':
    RPC_CLIENT = RpcClient('127.0.0.1', 'guest', 'guest', 'rpc_queue')
    app.run(debug=True)
