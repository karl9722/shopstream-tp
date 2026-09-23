"""
ShopStream - producteur Kafka qui simule un flux d'avis clients en temps réel.

Variables d'environnement :
  KAFKA_BOOTSTRAP  (défaut kafka:9092)
  TOPIC            (défaut reviews_stream)
  RATE             avis par seconde en moyenne (défaut 5)
  LATE_RATIO       part d'avis envoyés "en retard" (event_time dans le passé) (défaut 0.05)

Chaque message :
  key   = product_id (garantit l'ordre des avis d'un même produit dans une partition)
  value = JSON de l'avis (voir reviews.py)
"""
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

from reviews import ReviewFactory

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "reviews_stream")
RATE = float(os.environ.get("RATE", "5"))
LATE_RATIO = float(os.environ.get("LATE_RATIO", "0.05"))


def log(msg):
    print(f"[producer] {msg}", flush=True)


# 1) Attendre Kafka
while True:
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="shopstream-admin")
        break
    except NoBrokersAvailable:
        log(f"Kafka indisponible sur {BOOTSTRAP}, nouvel essai dans 3 s...")
        time.sleep(3)

# 2) Attendre que le topic soit créé par les étudiants (l'auto-création est désactivée)
while TOPIC not in admin.list_topics():
    log(f"Le topic '{TOPIC}' n'existe pas encore. Créez-le (cf. sujet, Partie 1). Nouvel essai dans 5 s...")
    time.sleep(5)
admin.close()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    linger_ms=50,
)
factory = ReviewFactory(seed=int(time.time()))
rng = random.Random()
sent = 0
log(f"Envoi vers '{TOPIC}' à ~{RATE} avis/s (Ctrl+C / docker compose stop producer pour arrêter)")
try:
    while True:
        now = datetime.now(timezone.utc)
        event_time = now
        if rng.random() < LATE_RATIO:  # événement en retard : utile pour comprendre le watermark
            event_time = now - timedelta(seconds=rng.randint(60, 600))
        review = factory.make(event_time)
        producer.send(TOPIC, key=review["product_id"], value=review)
        sent += 1
        if sent % 100 == 0:
            log(f"{sent} avis envoyés (dernier : {review['category']} / {review['rating']}★ / {review['lang']})")
        # rafales aléatoires pour rendre les fenêtres temporelles intéressantes
        burst = 3.0 if (int(time.time()) // 60) % 5 == 0 else 1.0
        time.sleep(rng.expovariate(RATE * burst))
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    log(f"Arrêt. {sent} avis envoyés.")
    sys.exit(0)
