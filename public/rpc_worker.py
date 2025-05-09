import amqpstorm
import time

def on_request(message):
    """
    Callback function to process incoming requests.
    """
    print(f"Worker received request: {message.body}")
    
    # Simulate work (e.g., processing the request)
    time.sleep(1)

    # Convert message to uppercase as a simulated response
    response_message = amqpstorm.Message.create(
        channel, 
        message.body.upper(), 
        properties={
            'correlation_id': message.correlation_id
        }
    )

    response_message.publish(routing_key=message.reply_to)
    print(f"Worker sent response: {response_message.body}")


if __name__ == "__main__":
    connection = amqpstorm.Connection('127.0.0.1', 'guest', 'guest')
    channel = connection.channel()
    channel.queue.declare('rpc_queue')
    channel.basic.consume(on_request, queue='rpc_queue')
    
    print("Worker started. Waiting for requests...")
    channel.start_consuming(to_tuple=False)
