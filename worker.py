# ------------------ Health server p/ Render (porta obrigatória) ------------------
import os, threading
from http.server import BaseHTTPRequestHandler, HTTPServer

def start_health_server():
    port = int(os.environ.get("PORT", "10000"))  # Render injeta PORT
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/health"):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"ok")
            else:
                self.send_response(404)
                self.end_headers()

        # silencia logs do HTTP
        def log_message(self, *args, **kwargs):
            return

    httpd = HTTPServer(("", port), Handler)
    httpd.serve_forever()

# ------------------ Dependências principais ------------------
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from supabase import create_client, Client

# ------------------ Variáveis de ambiente ------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

BROKER   = os.environ.get("MQTT_BROKER")
PORT     = int(os.environ.get("MQTT_PORT", "8883"))
USER     = os.environ.get("MQTT_USER") or ""
PASSWORD = os.environ.get("MQTT_PASS") or ""

TOPIC_TEMP = "placa1/temperatura"
TOPIC_UMID = "placa1/umidade"
DEVICE_ID  = "placa1"

# ------------------ Inicializa Supabase ------------------
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL e/ou SUPABASE_SERVICE_KEY não definidos nas env vars.")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def salvar_leitura(device_id: str, temperatura: float, umidade: float):
    data = {
        "device_id": device_id,
        "ts": datetime.now(timezone.utc).isoformat(),  # ISO8601 em UTC
        "temperature_c": float(temperatura),
        "humidity_pct": float(umidade),
    }
    supabase.table("readings").insert(data).execute()
    print(f"✔️ Dados salvos: {data}")

# ------------------ MQTT callbacks ------------------
leituras = {"temperatura": None, "umidade": None}

def on_connect(client, userdata, flags, rc, properties=None):
    print("Conectado ao broker MQTT com código:", rc)
    # (re)assina os tópicos sempre que reconectar
    client.subscribe(TOPIC_TEMP, qos=0)
    client.subscribe(TOPIC_UMID, qos=0)

def on_message(client, userdata, msg):
    global leituras
    payload = msg.payload.decode().strip()
    try:
        val = float(payload)
    except ValueError:
        print(f"⚠️ Payload inválido em {msg.topic}: {payload!r}")
        return

    if msg.topic.endswith("temperatura"):
        leituras["temperatura"] = val
    elif msg.topic.endswith("umidade"):
        leituras["umidade"] = val

    if (leituras["temperatura"] is not None) and (leituras["umidade"] is not None):
        try:
            salvar_leitura(DEVICE_ID, leituras["temperatura"], leituras["umidade"])
        finally:
            # limpa para esperar o próximo par temp/umid
            leituras["temperatura"] = None
            leituras["umidade"] = None

# ------------------ Configura e inicia MQTT ------------------
client = mqtt.Client()  # vai funcionar com paho 1.x e 2.x (apenas um aviso de depreciação pode aparecer)

# autenticação (se houver)
if USER or PASSWORD:
    client.username_pw_set(USER, PASSWORD)

# TLS só se estiver usando 8883
if PORT == 8883:
    # usa CAs do sistema; se precisar, pode carregar um CA específico com client.tls_set(ca_certs="...")
    client.tls_set()

client.on_connect = on_connect
client.on_message = on_message

# reconexão automática (exponencial) e primeira conexão com retry
client.reconnect_delay_set(min_delay=2, max_delay=30)
client.connect(BROKER, PORT, keepalive=60)

# ------------------ Sobe o health server e entra no loop MQTT ------------------
threading.Thread(target=start_health_server, daemon=True).start()

# loop bloqueante que tenta reconectar se cair
client.loop_forever(retry_first_connection=True)
