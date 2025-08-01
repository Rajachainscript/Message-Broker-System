import requests

# Register consumer
requests.post("http://127.0.0.1:5000/register_queue_consumer", json={
    "queue": "my_queue",
    "consumer_id": "consumer1"
})

# Receive
res = requests.get("http://127.0.0.1:5000/receive_queue", params={
    "queue": "my_queue"
})
print(res.json())
