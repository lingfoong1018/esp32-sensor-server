import os

# MQTT_BROKER   = os.getenv("MQTT_BROKER", "877b8720033f480392fe9b5705140916.s1.eu.hivemq.cloud")  # fallback to local for dev
# MQTT_PORT     = int(os.getenv("MQTT_PORT", 8883))

# MQTT_USER     = os.getenv("MQTT_USER", "esp32user")
# MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Test1234")

MQTT_BROKER   = os.getenv("MQTT_BROKER", "broker.hivemq.com")
MQTT_PORT     = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER     = os.getenv("MQTT_USER", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")


MQTT_TOPICS = [
    "esp32/sensor/temp",
    "esp32/sensor/humid",
    "esp32/sensor/pressure",
    "esp32/command/#",
    "esp32/ack/#",
]