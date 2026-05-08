import { useState, useEffect, useRef, useCallback } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine
} from "recharts";
import "./App.css";
 
const API = "";
const WS_URL = `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.host}/ws`;
 
const LED_COLORS = [
  { hex: "#ff0000", label: "RED" },
  { hex: "#ff6600", label: "ORG" },
  { hex: "#ffff00", label: "YLW" },
  { hex: "#00ff00", label: "GRN" },
  { hex: "#00ffff", label: "CYN" },
  { hex: "#0088ff", label: "BLU" },
  { hex: "#aa00ff", label: "PRP" },
  { hex: "#ffffff", label: "WHT" },
];

const sensorInfo = {
  temp: [
    ["BOARD", "ESP32-S3"],
    ["SENSOR", "TK12 NTC"],
    ["PIN", "GPIO 6"],
    ["INTERVAL", "5s"],
    ["BROKER", "HiveMQ"],
    ["LED", "GPIO 48"],
  ],
  humid: [
    ["BOARD", "ESP32-S3"],
    ["SENSOR", "NOT CONNECTED"],
    ["PIN", "—"],
    ["INTERVAL", "—"],
    ["BROKER", "HiveMQ"],
    ["LED", "GPIO 48"],
  ],
  pressure: [
    ["BOARD", "ESP32-S3"],
    ["SENSOR", "NOT CONNECTED"],
    ["PIN", "—"],
    ["INTERVAL", "—"],
    ["BROKER", "HiveMQ"],
    ["LED", "GPIO 48"],
  ],
};
 
function useWebSocket(url) {
  const [status, setStatus] = useState("connecting");
  const [lastMessage, setLastMessage] = useState(null);
  const wsRef = useRef(null);
  const retryRef = useRef(null);
  const retryDelay = useRef(2000);
 
  const connect = useCallback(() => {
    const ws = new WebSocket(url);
    wsRef.current = ws;
 
    ws.onopen = () => {
      setStatus("connected");
      retryDelay.current = 2000;
    };
    ws.onmessage = (e) => setLastMessage(JSON.parse(e.data));
    ws.onclose = () => {
      setStatus("reconnecting");
      retryRef.current = setTimeout(() => {
        retryDelay.current = Math.min(retryDelay.current * 1.5, 15000);
        connect();
      }, retryDelay.current);
    };
    ws.onerror = () => setStatus("error");
  }, [url]);
 
  useEffect(() => {
    connect();
    return () => {
      clearTimeout(retryRef.current);
      wsRef.current?.close();
    };
  }, [connect]);
 
  return { status, lastMessage };
}
 
function StatusDot({ status }) {
  const map = {
    connected: { cls: "dot-live", label: "LIVE" },
    connecting: { cls: "dot-connecting", label: "CONNECTING" },
    reconnecting: { cls: "dot-connecting", label: "RECONNECTING" },
    error: { cls: "dot-error", label: "ERROR" },
  };
  const { cls, label } = map[status] || map.connecting;
  return (
    <div className="status-indicator">
      <span className={`dot ${cls}`} />
      <span className="status-label">{label}</span>
    </div>
  );
}
 
function MetricCard({ label, value, unit, accent, sub, active, onClick }) {
  return (
    <div
      className={`metric-card ${active ? "active" : ""}`}
      onClick={onClick}
      style={{
        cursor: onClick ? "pointer" : "default",
        borderColor: active ? accent : undefined,
        boxShadow: active ? `0 0 12px ${accent}22` : undefined,
      }}
    >
      <div className="metric-label">{label}</div>
      <div className="metric-value" style={{ color: accent }}>
        {value ?? "—"}
        {value != null && <span className="metric-unit">{unit}</span>}
      </div>
      {sub && <div className="metric-sub">{sub}</div>}
    </div>
  );
}
 
function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <div className="tooltip-time">{label}</div>
      {payload.map((p) => (
        <div key={p.name} className="tooltip-row" style={{ color: p.color }}>
          {p.name}: {p.value?.toFixed(1)}°C
        </div>
      ))}
    </div>
  );
}
 
export default function App() {
  const [chartData, setChartData] = useState([]);
  const [latestTemp, setLatestTemp] = useState(null);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [selectedSensor, setSelectedSensor] = useState("temp");
  const [ledState, setLedState] = useState({ on: false, color: null });
  const [activeColor, setActiveColor] = useState(null);
  const [brightness, setBrightness] = useState(255);
  const [toast, setToast] = useState(null);
  const toastTimer = useRef(null);
  const { status, lastMessage } = useWebSocket(WS_URL);
 
  // Load history on mount
  useEffect(() => {
    fetch(`${API}/data`)
      .then((r) => r.json())
      .then((rows) => {
        const points = rows.map((row) => ({
          time: row.timestamp?.slice(11, 19) ?? "",
          temp: parseFloat(row.temperature) || null,
        })).filter((p) => p.temp !== null);
        setChartData(points.slice(-50));
        if (points.length > 0) {
          const last = points[points.length - 1];
          setLatestTemp(last.temp);
          setLastUpdate(rows[rows.length - 1]?.timestamp?.slice(11, 19));
        }
      })
      .catch(() => {});
  }, []);
 
  // Handle WebSocket messages
  useEffect(() => {
    if (!lastMessage) return;
    if (lastMessage.type === "sensor" && lastMessage.topic === "esp32/sensor/temp") {
      const value = lastMessage.value;
      const now = new Date().toTimeString().slice(0, 8);
      setLatestTemp(value);
      setLastUpdate(now);
      setChartData((prev) => {
        const next = [...prev, { time: now, temp: value }];
        return next.slice(-50);
      });
    }
  }, [lastMessage]);
 
  const showToast = (msg) => {
    setToast(msg);
    clearTimeout(toastTimer.current);
    toastTimer.current = setTimeout(() => setToast(null), 2000);
  };
 
  const sendLED = async (action, color, bri) => {
    const body = { action };
    if (color) body.color = color;
    if (bri !== undefined) body.brightness = bri;
    try {
      await fetch(`${API}/command/led`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      setLedState({ on: action !== "off", color: color || ledState.color });
      showToast(action === "off" ? "LED → OFF" : `LED → ${color || action}`);
    } catch {
      showToast("COMMAND FAILED");
    }
  };
 
  const pickColor = (hex) => {
    setActiveColor(hex);
    sendLED("color", hex);
  };
 
  return (
    <div className="app">
 
      {/* header */}
      <header className="header">
        <div className="header-left">
            <span className="header-bracket">[</span>
            ESP32·MONITOR
            <span className="header-bracket">]</span>
        </div>
        <div className="header-right">
          <StatusDot status={status} />
          <div className="header-time">
            {lastUpdate ? `LAST: ${lastUpdate}` : "AWAITING DATA"}
          </div>
        </div>
      </header>
 
      {/* metrics row */}
      <div className="metrics-row">
        <MetricCard
          label="TEMPERATURE"
          value={latestTemp?.toFixed(1)}
          unit="°C"
          accent="#f97316"
          sub="TK12 NTC THERMISTOR"
          onClick={() => setSelectedSensor("temp")}
        />
        <MetricCard
          label="HUMIDITY"
          value={null}
          unit="%"
          accent="#38bdf8"
          sub="NO SENSOR"
          onClick={() => setSelectedSensor("humid")}
        />
        <MetricCard
          label="PRESSURE"
          value={null}
          unit="hPa"
          accent="#a78bfa"
          sub="NO SENSOR"
          onClick={() => setSelectedSensor("pressure")}
        />
        <MetricCard
          label="LED STATE"
          value={ledState.on ? "ON" : "OFF"}
          unit=""
          accent={ledState.on ? (ledState.color || "#ffffff") : "#444"}
          sub={ledState.color ?? "—"}
          style={{ cursor: "default" }}
        />
      </div>
 
      {/* main grid */}
      <div className="main-grid">
 
        {/* chart */}
        <div className="panel chart-panel">
          <div className="panel-header">
            <span className="panel-title">
              {selectedSensor == "temp" ? "TEMPERATURE LOG"
                : selectedSensor == "humid" ? "HUMIDITY LOG" 
                : "PRESSURE LOG"}
            </span>
            <span className="panel-badge">{chartData.length} PTS</span>
          </div>
          <div className="chart-wrap">
            {chartData.length === 0 ? (
              <div className="chart-empty">AWAITING SENSOR DATA...</div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="2 6" stroke="#1e2028" vertical={false} />
                  <XAxis
                    dataKey="time"
                    tick={{ fill: "#444", fontSize: 12, fontFamily: "IBM Plex Mono" }}
                    tickLine={false}
                    axisLine={{ stroke: "#1e2028" }}
                    interval={Math.floor(chartData.length / 6)}
                    tickCount={6}
                  />
                  <YAxis
                    tick={{ fill: "#444", fontSize: 12, fontFamily: "IBM Plex Mono" }}
                    tickLine={false}
                    axisLine={false}
                    domain={[0, 40]}
                  />
                  <Tooltip content={<CustomTooltip />} />
                  <ReferenceLine y={25} stroke="#f97316" strokeDasharray="4 4" strokeOpacity={0.3} />
                  <Line
                    dataKey={selectedSensor}
                    name={selectedSensor}
                    stroke={selectedSensor === "temp" ? "#f97316" : selectedSensor === "humid" ? "#38bdf8" : "#a78bfa"}
                    strokeWidth={2}
                    dot={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
 
        {/* right column */}
        <div className="right-col">
 
          {/* LED control */}
          <div className="panel led-panel">
            <div className="panel-header">
              <span className="panel-title">LED CONTROL</span>
              <span className="panel-badge" style={{
                color: ledState.on ? (ledState.color || "#fff") : "#444",
                borderColor: ledState.on ? (ledState.color || "#fff") : "#444",
              }}>
                {ledState.on ? "● ON" : "○ OFF"}
              </span>
            </div>
 
            {/* LED preview */}
            <div
              className="led-preview"
              style={ledState.on ? {
                background: ledState.color || "#ffffff",
                boxShadow: `0 0 30px ${ledState.color || "#ffffff"}66`,
              } : {}}
            >
              {!ledState.on && <span className="led-preview-label">OFF</span>}
            </div>
 
            {/* color grid */}
            <div className="color-grid">
              {LED_COLORS.map((c) => (
                <button
                  key={c.hex}
                  className={`color-btn ${activeColor === c.hex ? "active" : ""}`}
                  style={{ "--c": c.hex }}
                  onClick={() => pickColor(c.hex)}
                  title={c.label}
                >
                </button>
              ))}
            </div>
 
            {/* brightness */}
            <div className="brightness-row">
              <span className="bri-label">☀</span>
              <input
                type="range"
                min={10}
                max={255}
                value={brightness}
                onChange={(e) => {
                  setBrightness(+e.target.value);
                  sendLED("brightness", activeColor, +e.target.value);
                }}
                className="bri-slider"
              />
              <span className="bri-val">{brightness}</span>
            </div>
 
            {/* on/off */}
            <div className="led-btns">
              <button className="led-btn btn-on" onClick={() => sendLED("on", activeColor)}>
                PWR ON
              </button>
              <button className="led-btn btn-off" onClick={() => {
                sendLED("off");
                setActiveColor(null);
              }}>
                PWR OFF
              </button>
            </div>
          </div>
 
          {/* device info */}
          <div className="panel info-panel">
            <div className="panel-header">
              <span className="panel-title">DEVICE INFO</span>
            </div>
            <div className="info-table">
              {sensorInfo[selectedSensor].map(([k, v]) => (
                <div key={k} className="info-row">
                  <span className="info-key">{k}</span>
                  <span className="info-val">{v}</span>
                </div>
              ))}
            </div>
          </div>
 
        </div>
      </div>
 
      {/* toast */}
      {toast && <div className="toast">{toast}</div>}
    </div>
  );
}