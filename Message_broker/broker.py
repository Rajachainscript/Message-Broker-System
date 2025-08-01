from flask import Flask, request, jsonify
from collections import defaultdict, deque
from datetime import datetime, timedelta
import threading
import fnmatch

app = Flask(__name__)

queues = defaultdict(deque)
queue_consumers = defaultdict(list)
consumer_index = defaultdict(int)
topic_subscribers = defaultdict(list)

lock = threading.Lock()

@app.route ('/')
def health_check():
    return "Message Broker is Running"

# Queue: Send message 
@app.route('/send_queue', methods=['POST'])
def send_queue():
    data = request.get_json()
    queue = data.get("queue")
    message = data.get("message")
    ttl_seconds = data.get("ttl_seconds", 60)

    if not queue or not message:
        return jsonify({"error": "Missing queue or message"}), 400

    expiry = datetime.utcnow() + timedelta(seconds=ttl_seconds)

    with lock:
        queues[queue].append({
            "message": message,
            "expiry": expiry
        })

    return jsonify({"status": f"Message added to queue '{queue}'"})

# Queue: Register Consumer
@app.route('/register_queue_consumer', methods=['POST'])
def register_queue_consumer():
    data = request.get_json()
    queue = data.get("queue")
    consumer_id = data.get("consumer_id")

    if not queue or not consumer_id:
        return jsonify({"error": "Missing queue or consumer_id"}), 400

    with lock:
        if consumer_id not in queue_consumers[queue]:
            queue_consumers[queue].append(consumer_id)

    return jsonify({"status": f"{consumer_id} registered to queue '{queue}'"})

# Queue: Receive message 
@app.route('/receive_queue', methods=['GET'])
def receive_queue():
    queue = request.args.get("queue")

    with lock:
        while queues[queue]:
            msg = queues[queue][0]
            if datetime.utcnow() > msg["expiry"]:
                queues[queue].popleft()
                continue

            if not queue_consumers[queue]:
                return jsonify({"message": None})

            index = consumer_index[queue] % len(queue_consumers[queue])
            selected_consumer = queue_consumers[queue][index]
            consumer_index[queue] += 1

            message = queues[queue].popleft()
            return jsonify({
                "consumer": selected_consumer,
                "message": message["message"]
            })

    return jsonify({"message": None})

# Topic: Subscribe consumer to topic
@app.route('/subscribe', methods=['POST'])
def subscribe():
    data = request.get_json()
    topic = data.get("topic")
    consumer_id = data.get("consumer_id")

    if not topic or not consumer_id:
        return jsonify({"error": "Missing topic or consumer_id"}), 400

    with lock:
        if consumer_id not in topic_subscribers[topic]:
            topic_subscribers[topic].append(consumer_id)

    return jsonify({"status": f"{consumer_id} subscribed to topic '{topic}'"})

# Topic: Publish 
@app.route('/publish', methods=['POST'])
def publish():
    data = request.get_json()
    topic = data.get("topic")
    message = data.get("message")
    ttl_seconds = data.get("ttl_seconds", 60)

    if not topic or not message:
        return jsonify({"error": "Missing topic or message"}), 400

    expiry = datetime.utcnow() + timedelta(seconds=ttl_seconds)

    with lock:
        for pattern, consumers in topic_subscribers.items():
            if fnmatch.fnmatch(topic, pattern):
                for consumer in consumers:
                    key = f"{pattern}:{consumer}"
                    queues[key].append({
                        "message": message,
                        "expiry": expiry
                    })

    return jsonify({"status": f"Message published to topic '{topic}'"})

# Topic: Receive message 
@app.route('/receive_topic', methods=['GET'])
def receive_topic():
    topic = request.args.get("topic")
    consumer_id = request.args.get("consumer_id")
    key = f"{topic}:{consumer_id}"

    with lock:
        while queues[key]:
            msg = queues[key][0]
            if datetime.utcnow() > msg["expiry"]:
                queues[key].popleft()  
                continue
            message = queues[key].popleft()
            return jsonify({"message": message["message"]})

        return jsonify({"message": None})

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=5000, debug=True)
