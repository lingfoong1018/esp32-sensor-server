import asyncio
import csv
import json
import os
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
 
import paho.mqtt.client as mqtt
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
 
from config import MQTT_BROKER, MQTT_PORT, MQTT_USER, MQTT_PASSWORD, MQTT_TOPICS
 
# ---------------------------------------------------------------------------
# CSV (kept for backwards compatibility with your existing data)
# ---------------------------------------------------------------------------
 
DATA_FILE = "data.csv"
 
def init_csv():
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "temperature", "humidity", "led", "status"])
 
def append_csv(temperature: float, humidity: float, led: str = "", status: str = "online"):
    with open(DATA_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            temperature, humidity, led, status,
        ])
 
# ---------------------------------------------------------------------------
# SQLite (new — stores individual topic readings with timestamps)
# ---------------------------------------------------------------------------
 
DB_PATH = "sensor_data.db"
 
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
 
def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS readings (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT    NOT NULL,
                topic     TEXT    NOT NULL,
                value     REAL    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS commands (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT    NOT NULL,
                target    TEXT    NOT NULL,
                payload   TEXT    NOT NULL
            )
        """)
        conn.commit()
 
def log_reading(topic: str, value: float):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO readings (timestamp, topic, value) VALUES (?, ?, ?)",
            (datetime.utcnow().isoformat(), topic, value),
        )
        conn.commit()
 
def log_command(target: str, payload: str):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO commands (timestamp, target, payload) VALUES (?, ?, ?)",
            (datetime.utcnow().isoformat(), target, payload),
        )
        conn.commit()
 
# ---------------------------------------------------------------------------
# In-memory cache of latest sensor values (for CSV append + /data compat)
# ---------------------------------------------------------------------------
 
_latest: dict = {"temp": None, "humid": None, "led": "off"}
 
# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------
 
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []
 
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
 
    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)
 
    async def broadcast(self, data: dict):
        message = json.dumps(data)
        for ws in list(self.active):
            try:
                await ws.send_text(message)
            except Exception:
                self.active.remove(ws)
 
manager = ConnectionManager()
_loop: asyncio.AbstractEventLoop | None = None
 
def broadcast_from_thread(data: dict):
    """Thread-safe bridge from paho callback thread to asyncio event loop."""
    if _loop:
        asyncio.run_coroutine_threadsafe(manager.broadcast(data), _loop)
 
# ---------------------------------------------------------------------------
# MQTT client
# ---------------------------------------------------------------------------
 
mqtt_client = mqtt.Client()
 
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        for topic in MQTT_TOPICS:
            client.subscribe(topic)
            print(f"   Subscribed to {topic}")
    else:
        print(f"MQTT connection failed (rc={rc})")
 
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    topic = msg.topic
    print(f"MQTT {topic}: {payload}")
 
    if topic == "esp32/sensor/temp":
        try:
            value = float(payload)
            _latest["temp"] = value
            log_reading(topic, value)
            # Keep CSV in sync whenever we have both values
            if _latest["humid"] is not None:
                append_csv(_latest["temp"], _latest["humid"], _latest["led"])
            broadcast_from_thread({"type": "sensor", "topic": topic, "value": value})
        except ValueError:
            print(f"  Bad payload on {topic}: {payload}")
 
    elif topic == "esp32/sensor/humid":
        try:
            value = float(payload)
            _latest["humid"] = value
            log_reading(topic, value)
            broadcast_from_thread({"type": "sensor", "topic": topic, "value": value})
        except ValueError:
            print(f"  Bad payload on {topic}: {payload}")
 
    elif topic == "esp32/sensor/pressure":
        try:
            value = float(payload)
            log_reading(topic, value)
            broadcast_from_thread({"type": "sensor", "topic": topic, "value": value})
        except ValueError:
            print(f"  Bad payload on {topic}: {payload}")
 
    elif topic.startswith("esp32/ack"):
        broadcast_from_thread({"type": "ack", "topic": topic, "payload": payload})
 
    else:
        print(f"  Unhandled topic: {topic}")
 
def start_mqtt():
    if MQTT_USER and MQTT_PASSWORD:
        mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()
 
# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------
 
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _loop
    _loop = asyncio.get_running_loop()
    init_csv()
    init_db()
    t = threading.Thread(target=start_mqtt, daemon=True)
    t.start()
    yield
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
 
# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
 
app = FastAPI(title="ESP32 Cloud Server", lifespan=lifespan)
 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
 
# Your existing index.html goes in a /templates folder (same as Flask)
templates = Jinja2Templates(directory="templates")
 
# Static assets (CSS, JS) — only mount if the folder exists
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
 
# ---------------------------------------------------------------------------
# "/" — serves your existing index.html (same as Flask render_template)
# ---------------------------------------------------------------------------
 
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
 
# ---------------------------------------------------------------------------
# "/log" — identical to your Flask route so old firmware still works
# ---------------------------------------------------------------------------
 
class LogPayload(BaseModel):
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    led: Optional[str] = ""
    status: Optional[str] = "online"
 
@app.post("/log")
def log_data(payload: LogPayload):
    """Legacy route — compatible with old app_main.c HTTP POST."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(DATA_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([ts, payload.temperature, payload.humidity,
                         payload.led, payload.status])
    # Also push to SQLite and live WebSocket clients
    if payload.temperature is not None:
        log_reading("esp32/sensor/temp", payload.temperature)
        broadcast_from_thread({"type": "sensor", "topic": "esp32/sensor/temp",
                               "value": payload.temperature})
    if payload.humidity is not None:
        log_reading("esp32/sensor/humid", payload.humidity)
        broadcast_from_thread({"type": "sensor", "topic": "esp32/sensor/humid",
                               "value": payload.humidity})
    return {"status": "success"}
 
# ---------------------------------------------------------------------------
# "/data" — identical to your Flask route (returns last 50 CSV rows)
# ---------------------------------------------------------------------------
 
@app.get("/data")
def get_data():
    """Legacy route — same response shape as Flask /data."""
    rows = []
    with open(DATA_FILE, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)[-50:]
    return rows
 
# ---------------------------------------------------------------------------
# WebSocket — live push to dashboard
# ---------------------------------------------------------------------------
 
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    print(f"Dashboard connected (total={len(manager.active)})")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
        print(f"Dashboard disconnected (total={len(manager.active)})")
 
# ---------------------------------------------------------------------------
# REST — sensor history (SQLite)
# ---------------------------------------------------------------------------
 
@app.get("/readings")
def get_readings(topic: Optional[str] = None, limit: int = 100):
    with get_db() as conn:
        if topic:
            rows = conn.execute(
                "SELECT * FROM readings WHERE topic = ? ORDER BY id DESC LIMIT ?",
                (topic, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM readings ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
    return [dict(r) for r in rows]
 
@app.get("/readings/latest")
def get_latest():
    topics = ["esp32/sensor/temp", "esp32/sensor/humid", "esp32/sensor/pressure"]
    result = {}
    with get_db() as conn:
        for t in topics:
            row = conn.execute(
                "SELECT value, timestamp FROM readings WHERE topic = ? ORDER BY id DESC LIMIT 1",
                (t,),
            ).fetchone()
            result[t] = dict(row) if row else None
    return result
 
# ---------------------------------------------------------------------------
# REST — LED commands
# ---------------------------------------------------------------------------
 
class LEDCommand(BaseModel):
    action: str                         # "on" | "off" | "color" | "brightness"
    color: Optional[str] = None         # hex e.g. "#FF8800"
    brightness: Optional[int] = None    # 0-255
 
@app.post("/command/led")
def send_led_command(cmd: LEDCommand):
    payload = json.dumps(cmd.model_dump(exclude_none=True))
    result = mqtt_client.publish("esp32/command/led", payload)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=502, detail="Failed to publish MQTT message")
    _latest["led"] = cmd.color or cmd.action
    log_command("led", payload)
    return {"status": "sent", "payload": payload}
 
# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
 
@app.get("/health")
def health():
    return {
        "status": "ok",
        "mqtt_connected": mqtt_client.is_connected(),
        "ws_clients": len(manager.active),
    }