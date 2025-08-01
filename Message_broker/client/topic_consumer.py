import requests

# Subscribe
requests.post("http://127.0.0.1:5000/subscribe", json={
    "topic": "sports.football",
    "consumer_id": "consumer2"
})

# Receive
res = requests.get("http://127.0.0.1:5000/receive_topic", params={
    "topic": "sports.football",
    "consumer_id": "consumer2"
})
print(res.json())
