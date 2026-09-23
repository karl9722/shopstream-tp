"""
Génère l'historique ShopStream dans /data (monté sur ./data) :
  - products.csv         : catalogue produits (~200 lignes)  -> table de dimension
  - reviews_history.csv  : ~200 000 avis sur 90 jours        -> historique pour le batch / le ML

Idempotent : ne régénère pas si les fichiers existent déjà (FORCE=1 pour forcer).
"""
import csv
import os
import random
from datetime import datetime, timedelta, timezone

from reviews import ReviewFactory, random_history_time

OUT = os.environ.get("DATA_DIR", "/data")
N = int(os.environ.get("N_HISTORY", "200000"))
DAYS = 90

os.makedirs(OUT, exist_ok=True)
for d in (OUT, os.path.join(OUT, "models"), os.path.join(OUT, "checkpoints")):
    os.makedirs(d, exist_ok=True)
    os.chmod(d, 0o777)  # le conteneur Jupyter (utilisateur jovyan) doit pouvoir écrire

hist_path = os.path.join(OUT, "reviews_history.csv")
prod_path = os.path.join(OUT, "products.csv")
if os.path.exists(hist_path) and os.environ.get("FORCE") != "1":
    print(f"[datagen] {hist_path} existe déjà, rien à faire.")
    raise SystemExit(0)

factory = ReviewFactory(seed=2026)
rng = random.Random(7)
start = (datetime.now(timezone.utc) - timedelta(days=DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
promo_day = start + timedelta(days=DAYS - 60)  # journée "soldes" avec un pic d'activité

with open(prod_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["product_id", "product_name", "category", "brand", "price"])
    w.writeheader()
    for p in factory.products:
        w.writerow({k: p[k] for k in w.fieldnames})

fields = ["review_id", "event_time", "user_id", "username", "product_id", "category", "rating",
          "lang", "text", "hashtags", "helpful_votes", "verified_purchase", "country"]
with open(hist_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    for i in range(N):
        if rng.random() < 0.06:
            t = promo_day + timedelta(seconds=rng.randrange(86400))
        else:
            t = random_history_time(rng, start, DAYS)
        r = factory.make(t)
        r["hashtags"] = "|".join(r["hashtags"])  # CSV : hashtags séparés par "|"
        w.writerow(r)
        if (i + 1) % 50000 == 0:
            print(f"[datagen] {i + 1}/{N} avis générés")

for p in (prod_path, hist_path):
    os.chmod(p, 0o666)
print(f"[datagen] OK -> {prod_path}, {hist_path}")
