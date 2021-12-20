##
from flask import Flask, request, Response, jsonify
import platform
import io, os, sys
import pika, redis
import hashlib, requests
import json

##
## Configure test vs. production
##
redisHost = os.getenv("REDIS_HOST") or "localhost"
rabbitMQHost = os.getenv("RABBITMQ_HOST") or "localhost"

print("Connecting to rabbitmq({}) and redis({})".format(rabbitMQHost,redisHost))

##
## Set up redis connections
##
db = redis.Redis(host=redisHost, db=1, decode_responses=True)                                                                           

##
## Set up rabbitmq connection
##
rabbitMQ = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitMQHost))
rabbitMQChannel = rabbitMQ.channel()

rabbitMQChannel.queue_declare(queue='toWorker')
rabbitMQChannel.exchange_declare(exchange='logs', exchange_type='topic')
infoKey = f"{platform.node()}.worker.info"
debugKey = f"{platform.node()}.worker.debug"

def log_debug(message, key=debugKey):
    print("DEBUG:", message, file=sys.stdout)
    rabbitMQChannel.basic_publish(
        exchange='logs', routing_key=key, body=message)
def log_info(message, key=infoKey):
    print("INFO:", message, file=sys.stdout)
    rabbitMQChannel.basic_publish(
        exchange='logs', routing_key=key, body=message)

app = Flask("Server")

@app.route("/apiv1/analyze", methods=['POST'])
def analyze():
    data = request.json
    message = json.dumps(data)
    log_debug(f"Sending request {message}")
    rabbitMQChannel.basic_publish(exchange='', routing_key='toWorker', body=message.encode())

    return json.dumps({
        "action": "queued"
    })


@app.route("/apiv1/cache", methods=['GET'])
def cache():
    model = request.args.get('model')
    sentences = db.smembers(model)
    keys = []
    for sentence in sentences:
        keys.append(model + ":" + sentence)
    values = db.mget(keys)
    response = {
        'model': model,
        'sentences': []
    }
    for val in values:
        if val:
            analysis = {
                'model': model,
                'result': json.loads(val)
            }
            response['sentences'].append(analysis)
    return json.dumps(response)


@app.route("/apiv1/sentence", methods=["GET"])
def sentence():
    data = request.json
    model = data['model']
    keys = []
    for sentence in data["sentences"]:
        sha1 = hashlib.sha1()
        sha1.update(sentence.encode('utf-8'))
        hashedSentence = sha1.hexdigest()
        keys.append(model + ":" + hashedSentence)
    values = db.mget(keys)
    res = {
        "model": "",
        "sentences": []
    }
    analysisList = []
    for i, val in enumerate(values):
        analysis = None
        if val:
            analysis = json.loads(val)
        sentenceResponse = {
            "analysis": analysis,
            "text": data['sentences'][i]
        }
        analysisList.append(sentenceResponse)
    res['sentences'] = analysisList
    res['model'] = model
    return json.dumps(res)

@app.route('/', methods=['GET'])
def hello():
    return '<h1> Sentiment Server</h1><p> Use a valid endpoint </p>'

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000)