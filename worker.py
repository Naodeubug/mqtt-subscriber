import paho.mqtt.client as mqtt
import os
from datetime import datetime, timezone
from supabase import create_client, Client

# ==== VARIÁVEIS DE AMBIENTE ====
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
BROKER = os.environ.get("MQTT_BROKER")
PORT = int(os.environ.get("MQTT_PORT", 8883))
USER = os.environ.get("MQTT_USER")
PASSWORD = os.environ.get("MQTT_PASS")

# ==== INICIALIZA SUPABASE ====
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def salvar_leitura(device_id, temperatura, umidade):
    data = {
        "device_id": device_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "temperature_c": float(temperatura),
        "humidity_pct": float(umidade),
    }
    supabase.table("readings").insert(data).execute()
    print(f"✔️ Dados salvos: {data}")

# ==== CALLBACKS MQTT ====
def on_connect(client, userdata, flags, rc):
    print("Conectado ao broker MQTT com código:", rc)
    client.subscribe("placa1/temperatura")
    client.subscribe("placa1/umidade")

leituras = {"temperatura": None, "umidade": None}

def on_message(client, userdata, msg):
    global leituras
    if msg.topic.endswith("temperatura"):
        leituras["temperatura"] = msg.payload.decode()
    elif msg.topic.endswith("umidade"):
        leituras["umidade"] = msg.payload.decode()

    if leituras["temperatura"] and leituras["umidade"]:
        salvar_leitura("placa1", leituras["temperatura"], leituras["umidade"])
        leituras = {"temperatura": None, "umidade": None}

# ==== CONEXÃO MQTT ====
client = mqtt.Client()
client.username_pw_set(USER, PASSWORD)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_forever()
