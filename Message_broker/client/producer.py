import requests

# Send message
res = requests.post("http://127.0.0.1:5000/send_queue", json={
    "queue": "my_queue",
    "message": "Hello Queue",
    "ttl_seconds": 10
})
print(res.json())

# Publish 
res = requests.post("http://127.0.0.1:5000/publish", json={
    "topic": "sports.football",
    "message": "Goal Scored!",
    "ttl_seconds": 10
})
print(res.json())
