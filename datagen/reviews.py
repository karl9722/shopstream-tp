"""
ShopStream - générateur d'avis clients synthétiques (bibliothèque standard uniquement).

Utilisé par :
  - generate_history.py : produit l'historique (CSV) chargé ensuite dans PostgreSQL
  - producer.py         : envoie des avis "en temps réel" dans Kafka

Chaque avis ressemble à un "toot" Mastodon : un auteur, un horodatage, un texte,
des hashtags et des métriques d'engagement (helpful_votes ~ favourites/reblogs).
"""
import random
import uuid
from datetime import datetime, timedelta, timezone

CATEGORIES = {
    "electronique": ["casque", "chargeur", "smartphone", "clavier", "enceinte", "souris"],
    "maison": ["aspirateur", "lampe", "cafetiere", "poele", "couette", "bouilloire"],
    "mode": ["jean", "baskets", "veste", "sac", "montre", "t-shirt"],
    "beaute": ["creme", "parfum", "shampoing", "mascara", "serum", "brosse"],
    "sport": ["tapis", "halteres", "velo", "raquette", "gourde", "chaussures"],
    "livres": ["roman", "manga", "bd", "guide", "livre", "dictionnaire"],
    "jouets": ["puzzle", "lego", "peluche", "jeu", "poupee", "voiture"],
    "alimentation": ["cafe", "the", "chocolat", "miel", "epices", "biscuits"],
}
CATEGORIES_EN = {
    "casque": "headphones", "chargeur": "charger", "smartphone": "smartphone", "clavier": "keyboard",
    "enceinte": "speaker", "souris": "mouse", "aspirateur": "vacuum", "lampe": "lamp",
    "cafetiere": "coffee maker", "poele": "pan", "couette": "duvet", "bouilloire": "kettle",
    "jean": "jeans", "baskets": "sneakers", "veste": "jacket", "sac": "bag", "montre": "watch",
    "t-shirt": "t-shirt", "creme": "cream", "parfum": "perfume", "shampoing": "shampoo",
    "mascara": "mascara", "serum": "serum", "brosse": "brush", "tapis": "mat",
    "halteres": "dumbbells", "velo": "bike", "raquette": "racket", "gourde": "bottle",
    "chaussures": "shoes", "roman": "novel", "manga": "manga", "bd": "comic", "guide": "guide",
    "livre": "book", "dictionnaire": "dictionary", "puzzle": "puzzle", "lego": "lego set",
    "peluche": "plush", "jeu": "board game", "poupee": "doll", "voiture": "toy car",
    "cafe": "coffee", "the": "tea", "chocolat": "chocolate", "miel": "honey",
    "epices": "spices", "biscuits": "cookies",
}

PHRASES = {
    "fr": {
        "pos": ["excellent {p}, je recommande", "tres satisfait de ce {p}", "super qualite",
                "livraison rapide et produit conforme", "parfait, rien a redire", "rapport qualite prix top",
                "j'adore ce {p}", "au top, je rachete", "tres bon produit", "emballage soigne, merci",
                "fonctionne parfaitement", "magnifique, conforme aux photos", "service client reactif et sympa"],
        "neg": ["tres decu par ce {p}", "qualite mediocre", "produit arrive casse", "a eviter absolument",
                "ne fonctionne pas", "remboursement demande", "livraison en retard et colis abime",
                "arnaque, rien a voir avec la description", "nul, je regrette mon achat", "tombe en panne apres deux jours",
                "service client injoignable", "trop cher pour ce que c'est", "horrible, retour immediat"],
        "neu": ["{p} correct sans plus", "ca fait le job", "conforme mais livraison lente",
                "bon produit mais un peu cher", "moyen, ni bon ni mauvais", "pas mal mais la taille est petite",
                "a voir sur la duree", "{p} basique"],
        "fill": ["", "", "", " pour le prix", " honnetement", " vraiment", " pour un cadeau", " apres une semaine d'utilisation"],
    },
    "en": {
        "pos": ["great {p}, highly recommend", "very happy with this {p}", "excellent quality",
                "fast delivery and exactly as described", "perfect, love it", "amazing value for money",
                "love this {p}", "works perfectly", "best purchase this year", "well packaged, thanks",
                "awesome, will buy again", "customer service was helpful"],
        "neg": ["very disappointed with this {p}", "poor quality", "arrived broken", "avoid at all costs",
                "does not work", "asked for a refund", "late delivery and damaged box", "scam, nothing like the pictures",
                "terrible, waste of money", "stopped working after two days", "customer service never answered",
                "overpriced junk"],
        "neu": ["{p} is ok", "does the job", "fine but shipping was slow", "decent but a bit expensive",
                "average, nothing special", "not bad but runs small", "we will see how long it lasts", "basic {p}"],
        "fill": ["", "", "", " for the price", " honestly", " really", " as a gift", " after a week of use"],
    },
}

HASHTAGS = {
    "pos": ["#top", "#recommande", "#qualite", "#bonplan", "#happy"],
    "neg": ["#decu", "#arnaque", "#sav", "#retour", "#fail"],
    "neu": ["#bof", "#moyen", "#avis"],
    "any": ["#livraison", "#prix", "#blackfriday", "#cadeau", "#promo", "#nouveaute"],
}
COUNTRIES = ["FR", "FR", "FR", "FR", "BE", "CH", "CA", "MA", "SN", "US", "GB"]
RATING_WEIGHTS = {5: 40, 4: 25, 3: 12, 2: 9, 1: 14}


class ReviewFactory:
    """Fabrique d'avis reproductible (seed) avec des utilisateurs plus ou moins actifs."""

    def __init__(self, seed=42, n_users=2000, n_products=200):
        self.rng = random.Random(seed)
        rng = self.rng
        self.users = [(f"u{i:05d}", f"{rng.choice(['alex','sam','lina','yanis','ines','noah','jade','adam','lea','omar','emma','hugo'])}_{i}")
                      for i in range(n_users)]
        # Loi de puissance : quelques "gros posteurs", beaucoup d'utilisateurs occasionnels
        self.user_weights = [1.0 / (i + 1) ** 0.9 for i in range(n_users)]
        rng.shuffle(self.user_weights)
        self.products = []
        for i in range(n_products):
            cat = rng.choice(list(CATEGORIES))
            item = rng.choice(CATEGORIES[cat])
            price = round(rng.uniform(3, 400 if cat == "electronique" else 150), 2)
            self.products.append({"product_id": f"p{i:04d}", "product_name": f"{item} {rng.choice(['classic','pro','max','eco','plus','mini'])}",
                                  "item": item, "category": cat, "price": price,
                                  "brand": rng.choice(["Nova", "Kairo", "Leto", "Brume", "Atlas", "Zenit", "Orbis"])})
        self.product_weights = [rng.paretovariate(1.2) for _ in range(n_products)]

    def _sentence(self, lang, polarity, item):
        rng = self.rng
        p = item if lang == "fr" else CATEGORIES_EN[item]
        ph = PHRASES[lang]
        parts = [rng.choice(ph[polarity]).format(p=p) + rng.choice(ph["fill"])]
        if rng.random() < 0.55:  # deuxième phrase
            second = polarity
            if rng.random() < 0.18:  # bruit : avis "mitigé" pour que le modèle ne soit pas parfait
                second = rng.choice(["pos", "neg", "neu"])
            parts.append(rng.choice(ph[second]).format(p=p))
        text = ". ".join(parts)
        if rng.random() < 0.3:
            text += rng.choice(["!", "!!", " :)", " :(", "...", " 👍", " 👎"])
        return text[0].upper() + text[1:]

    def make(self, event_time: datetime) -> dict:
        rng = self.rng
        user_id, username = rng.choices(self.users, weights=self.user_weights, k=1)[0]
        prod = rng.choices(self.products, weights=self.product_weights, k=1)[0]
        rating = rng.choices(list(RATING_WEIGHTS), weights=list(RATING_WEIGHTS.values()), k=1)[0]
        polarity = "pos" if rating >= 4 else "neg" if rating <= 2 else "neu"
        if rng.random() < 0.07:  # note incohérente avec le texte (ça arrive dans la vraie vie)
            polarity = rng.choice(["pos", "neg"])
        lang = "fr" if rng.random() < 0.72 else "en"
        tags = set()
        if rng.random() < 0.6:
            tags.add(rng.choice(HASHTAGS[polarity]))
        if rng.random() < 0.45:
            tags.add(rng.choice(HASHTAGS["any"]))
        tags.add(f"#{prod['category']}") if rng.random() < 0.3 else None
        text = self._sentence(lang, polarity, prod["item"])
        if tags:
            text += " " + " ".join(sorted(tags))
        return {
            "review_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "event_time": event_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "user_id": user_id,
            "username": username,
            "product_id": prod["product_id"],
            "category": prod["category"],
            "rating": rating,
            "lang": lang,
            "text": text,
            "hashtags": sorted(tags),
            "helpful_votes": int(rng.expovariate(1 / (6 if polarity == "neg" else 2))),
            "verified_purchase": rng.random() < 0.8,
            "country": rng.choice(COUNTRIES),
        }


# Profil horaire (activité plus forte le soir) utilisé pour l'historique
HOURLY_PROFILE = [0.3, 0.2, 0.15, 0.1, 0.1, 0.15, 0.3, 0.6, 0.8, 0.9, 1.0, 1.1,
                  1.3, 1.2, 1.0, 1.0, 1.1, 1.3, 1.6, 1.9, 2.1, 1.9, 1.3, 0.7]


def random_history_time(rng, start: datetime, days: int) -> datetime:
    day = rng.randrange(days)
    hour = rng.choices(range(24), weights=HOURLY_PROFILE, k=1)[0]
    base = start + timedelta(days=day, hours=hour, seconds=rng.randrange(3600))
    # Pic "Black Friday" simulé : on concentre des avis supplémentaires sur 2 jours
    return base
