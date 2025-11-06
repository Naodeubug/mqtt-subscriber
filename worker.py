# worker.py
import os
import ssl
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from supabase import create_client, Client

# ---------------- Health server (porta para o Render) ----------------
def start_health_server():
    port = int(os.environ.get("PORT", "10000"))  # Render injeta PORT
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/health"):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"ok")
            else:
                self.send_response(404); self.end_headers()
        def log_message(self, *args):  # silencia log HTTP
            return
    httpd = HTTPServer(("", port), Handler)
    httpd.serve_forever()

# Inicia o /health em background
threading.Thread(target=start_health_server, daemon=True).start()

# ---------------- Ambiente ----------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

MQTT_HOST = os.environ.get("MQTT_BROKER")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")

TOPIC_TEMP = "placa1/temperatura"
TOPIC_UMID = "placa1/umidade"

# ---------------- Supabase ----------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def salvar_leitura(device_id, temperatura, umidade):
    payload = {
        "device_id": device_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "temperature_c": float(temperatura),
        "humidity_pct": float(umidade),
    }
    try:
        supabase.table("readings").insert(payload).execute()
        print(f"[OK] salvo -> {payload}")
    except Exception as e:
        # Não deixa o processo morrer por erro transitório de rede/DB
        print(f"[ERRO] insert supabase: {e}")

# ---------------- MQTT ----------------
leituras = {"temperatura": None, "umidade": None}

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[MQTT] conectado rc={rc}")
    client.subscribe(TOPIC_TEMP)
    client.subscribe(TOPIC_UMID)

def on_disconnect(client, userdata, rc, properties=None):
    print(f"[MQTT] desconectado rc={rc} (Render dormiu? rede caiu?)")

def on_message(client, userdata, msg):
    global leituras
    try:
        v = msg.payload.decode().strip()
        if msg.topic.endswith("temperatura"):
            leituras["temperatura"] = v
        elif msg.topic.endswith("umidade"):
            leituras["umidade"] = v
        # quando tiver os dois, grava e zera
        if leituras["temperatura"] is not None and leituras["umidade"] is not None:
            salvar_leitura("placa1", leituras["temperatura"], leituras["umidade"])
            leituras = {"temperatura": None, "umidade": None}
    except Exception as e:
        print(f"[ERRO] on_message: {e}")

client = mqtt.Client(protocol=mqtt.MQTTv311)
client.username_pw_set(MQTT_USER, MQTT_PASS)

# TLS se porta 8883
if MQTT_PORT == 8883:
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)  # para testes; valide cert em produção

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# Reconexão exponencial
client.reconnect_delay_set(min_delay=5, max_delay=60)

# keepalive ajuda o broker a detectar sessão quebrada
print("[MQTT] conectando...")
client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

# Loop que tenta reconectar sozinho se a primeira falhar
client.loop_forever(retry_first_connection=True)
