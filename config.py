import os

MQTT_BROKER   = os.getenv("MQTT_BROKER", "192.168.0.77")  # fallback to local for dev
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