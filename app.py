from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
from kafka import KafkaConsumer, RoundRobinPartitionAssignor
import joblib

app = Flask(__name__)

# charge le modèle
model = joblib.load('random_forest_model.joblib')

# créer kafka pred topic
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# envoi prédicitons au topic kafka
def send_prediction(prediction):
    producer.send('prediction_charlemagne', value=prediction)

# écoute partition 
consumer = KafkaConsumer(
    'carolus_topic',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    group_id='carolus_group',
    partition_assignment_strategy=[RoundRobinPartitionAssignor]
)


def consume_messages():
    for message in consumer:
        data = message.value
        prediction = predict_with_model(data)
        send_prediction(prediction)

# faire des prédictions
def predict_with_model(data):
    prediction = model.predict(data)
    return prediction.tolist()

# route flask
def predict():
    data = request.get_json()
    prediction = predict_with_model(data)
    send_prediction(prediction)
    return jsonify({'prediction': prediction})

# run consumer kafka
consume_messages()

if __name__ == '__main__':
    app.run(debug=True)

