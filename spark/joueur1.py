import os
import time
import json
import requests
import pandas as pd
from io import StringIO
from kafka import KafkaProducer

# 1. Configuration Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',              # Adresse du broker Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Paramètres des ligues et saisons
leagues = ['E0', 'SP1', 'D1', 'I1', 'F1']
# Génère ['0001','0102',...,'2324']
seasons = [f"{str(y)[-2:]}{str(y+1)[-2:]}" for y in range(2000, 2024)]

# 3. URL de base pour les CSV
base_url = "https://www.football-data.co.uk/mmz4281"

# 4. Création du dossier de cache (optionnel)
os.makedirs("data_cache", exist_ok=True)

# 5. Boucle de téléchargement et publication Kafka
for season in seasons:
    for league in leagues:
        url = f"{base_url}/{season}/{league}.csv"
        filename = f"data_cache/{league}_{season}.csv"
        try:
            resp = requests.get(url, timeout=10)
            # Vérifie que c'est bien un CSV de taille raisonnable
            if resp.status_code == 200 and 'text/csv' in resp.headers.get('Content-Type', '') and len(resp.content) > 500:
                # Lecture dans DataFrame pandas
                df = pd.read_csv(StringIO(resp.text))
                # Ajout de colonnes de contexte
                df['Season'] = season
                df['League'] = league

                # Optionnel : sauvegarde locale
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(resp.text)

                # Envoi message par message
                for _, row in df.iterrows():
                    # Drop des NaN pour alléger
                    record = row.dropna().to_dict()
                    producer.send('football11-matches', value=record)
                    time.sleep(0.005)  # throttle léger

                print(f"✅ Publié {league} {season} ({len(df)} lignes)")
            else:
                print(f"⚠️ CSV invalide ou manquant pour {league} {season}")
        except Exception as e:
            print(f"❌ Erreur {league} {season} : {e}")

# 6. Assure la livraison des derniers messages
producer.flush()
producer.close()
print("▶️ Fin du producer, tout est envoyé.")
