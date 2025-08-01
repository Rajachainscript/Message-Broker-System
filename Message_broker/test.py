from flask import Flask, request, jsonify
from collections import defaultdict, deque
from datetime import datetime, timedelta
import threading
import fnmatch
import uuid

app = Flask(__name__)

queues = defaultdict(deque)
in_flight = defaultdict(dict)  # queue_name -> {msg_id: msg}
queue_consumers = defaultdict(list)
consumer_index = defaultdict(int)
topic_subscribers = defaultdict(list)

lock = threading.Lock()

@app.route('/')
def health_check():
    return "Message Broker is Running"

# Queue: Send message with TTL
@app.route('/send_queue', methods=['POST'])
def send_queue():
    data = request.get_json()
    queue = data.get("queue")
    message = data.get("message")
    ttl_seconds = data.get("ttl_seconds", 60)

    if not queue or not message:
        return jsonify({"error": "Missing queue or message"}), 400

    msg_id = str(uuid.uuid4())
    expiry = datetime.utcnow() + timedelta(seconds=ttl_seconds)

    with lock:
        queues[queue].append({
            "id": msg_id,
            "message": message,
            "expiry": expiry
        })

    return jsonify({"status": f"Message added to queue '{queue}'", "id": msg_id})

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

# Queue: Receive message (check TTL) – don't delete until ACK
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
            in_flight[queue][message["id"]] = message

            return jsonify({
                "consumer": selected_consumer,
                "message_id": message["id"],
                "message": message["message"]
            })

    return jsonify({"message": None})

# ACK: Confirm message processed
@app.route('/ack', methods=['POST'])
def ack():
    data = request.get_json()
    queue = data.get("queue")
    msg_id = data.get("message_id")

    with lock:
        if msg_id in in_flight[queue]:
            del in_flight[queue][msg_id]
            return jsonify({"status": "ACK received, message removed"})
        else:
            return jsonify({"error": "Message not found"}), 404

# NACK: Requeue the message
@app.route('/nack', methods=['POST'])
def nack():
    data = request.get_json()
    queue = data.get("queue")
    msg_id = data.get("message_id")

    with lock:
        if msg_id in in_flight[queue]:
            msg = in_flight[queue].pop(msg_id)
            # if not expired, requeue it
            if datetime.utcnow() < msg["expiry"]:
                queues[queue].append(msg)
            return jsonify({"status": "NACK received, message requeued"})
        else:
            return jsonify({"error": "Message not found"}), 404

# Topic: Subscribe
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

# Topic: Publish message with TTL
@app.route('/publish', methods=['POST'])
def publish():
    data = request.get_json()
    topic = data.get("topic")
    message = data.get("message")
    ttl_seconds = data.get("ttl_seconds", 60)

    if not topic or not message:
        return jsonify({"error": "Missing topic or message"}), 400

    expiry = datetime.utcnow() + timedelta(seconds=ttl_seconds)
    msg_id = str(uuid.uuid4())

    with lock:
        for pattern, consumers in topic_subscribers.items():
            if fnmatch.fnmatch(topic, pattern):
                for consumer in consumers:
                    key = f"{pattern}:{consumer}"
                    queues[key].append({
                        "id": msg_id,
                        "message": message,
                        "expiry": expiry
                    })

    return jsonify({"status": f"Message published to topic '{topic}'", "id": msg_id})

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
            return jsonify({"message_id": message["id"], "message": message["message"]})

        return jsonify({"message": None})

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=5000, debug=True)
