from sklearn.ensemble import RandomForestRegressor
from sklearn.datasets import fetch_california_housing
import joblib
from kafka import KafkaConsumer
from json import loads


housing = fetch_california_housing()
X, y = housing.data, housing.target

model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X, y)
joblib.dump(model, "random_forest_model.joblib")


# sauvegarde du modèle
joblib.dump(model, 'random_forest_model.joblib')

# charge le modèle et le retourne
def load_model():
    model = joblib.load('random_forest_model.joblib')
    return model

# test le modèle 
model = load_model()
prediction = model.predict(X)
print(prediction)

# création consumer
def predict_with_model(data):
    model = load_model()
    prediction = model.predict(data)
    print(prediction)

# création kafka consumer
consumer = KafkaConsumer(
    'carolus_topic',
    'carolus_group',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# définit fonction message consume depuis carolus_topic
def consume_messages():
    for message in consumer:
        data = message.value
        predict_with_model(data)

# lance consumer
consume_messages()




