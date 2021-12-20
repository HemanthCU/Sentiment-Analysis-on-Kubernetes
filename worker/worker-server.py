#
# Worker server
#
import pickle
import platform
import io
import os
import sys
import pika
import redis
import hashlib
import json
import requests

from flair.models import TextClassifier
from flair.data import Sentence


hostname = platform.node()

##
## Configure test vs. production
##
redisHost = os.getenv("REDIS_HOST") or "localhost"
rabbitMQHost = os.getenv("RABBITMQ_HOST") or "localhost"

print(f"Connecting to rabbitmq({rabbitMQHost}) and redis({redisHost})")

##
## Set up redis connections
##
db = redis.Redis(host=redisHost, db=1)                                                                           

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

def performAnalysis(classifier, sentence):
    print("performAnalysis")
    classifier = TextClassifier.load(classifier)
    finalSentence = Sentence(sentence)
    classifier.predict(finalSentence)
    return finalSentence.to_dict()

def onReceived(channel, methodFrame, headerFrame, body):
    print(methodFrame.delivery_tag)
    request = json.loads(body)
    classifier = request['model']
    if classifier not in ["sentiment", "sentiment-fast", "communicative-functions", "de-offensive-language"]:
        raise Exception("Classfier not in list of classifiers")
    for sentence in request['sentences']:
        sha1 = hashlib.sha1()
        sha1.update(sentence.encode('utf-8'))
        hashedSentence = sha1.hexdigest()

        key = classifier + ":" + hashedSentence
        cachedValue = db.get(key)
        if cachedValue is None:
            print("Recomputing...")
            res = performAnalysis(classifier, sentence)
            val = json.dumps(res)
            db.set(key, val)
            db.sadd(classifier, hashedSentence)
    channel.basic_ack(delivery_tag=methodFrame.delivery_tag)

rabbitMQChannel.basic_consume('toWorker', onReceived)
try:
    rabbitMQChannel.start_consuming()
except KeyboardInterrupt:
    rabbitMQChannel.stop_consuming()
rabbitMQChannel.close()