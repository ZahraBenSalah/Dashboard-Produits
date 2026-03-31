# ingestion/csv_ingestion.py
import pandas as pd
from datetime import datetime
import os
import time
from ingestion.kafka_producer import send_to_kafka  # ton module kafka

# -----------------------------
# Fonction de lecture CSV avec retry
# -----------------------------
def read_csv_with_retry(path, retries=3, delay=2):
    """
    Tente de lire un CSV plusieurs fois en cas d'erreur.
    path: chemin du fichier CSV
    retries: nombre de tentatives
    delay: délai en secondes entre chaque tentative
    """
    for i in range(retries):
        try:
            df = pd.read_csv(path)
            print(f"✅ Lecture réussie : {path}")
            return df
        except Exception as e:
            print(f"⚠ Tentative {i+1} échouée pour {path}: {e}")
            time.sleep(delay)
    raise Exception(f"❌ Impossible de lire {path} après {retries} essais")

# -----------------------------
# Fonction principale de chargement CSV
# -----------------------------
def load_csv():
    # Dossier raw
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(BASE_DIR, "data", "raw")

    # Lister tous les CSV
    csv_files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]

    if not csv_files:
        print("❌ Aucun fichier CSV trouvé dans le dossier raw")
        return

    all_dfs = []
    now = datetime.now()

    for file in csv_files:
        file_path = os.path.join(raw_dir, file)

        # Lecture CSV avec retry
        df = read_csv_with_retry(file_path)

        # Ajouter source et ingestion_date
        df["source"] = file.replace(".csv", "")
        df["ingestion_date"] = now

        # Sauvegarder CSV nettoyé
        clean_file = f"clean_{file.lower().replace(' ', '_')}"
        clean_path = os.path.join(raw_dir, clean_file)
        df.to_csv(clean_path, index=False)
        print(f"✅ CSV nettoyé sauvegardé : {clean_file}")

        all_dfs.append(df)

    # Concatène tous les CSV
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"✅ {len(all_dfs)} CSV chargés, total {len(combined_df)} lignes")

    # Envoyer à Kafka
    for _, row in combined_df.iterrows():
        record = row.to_dict()
        for k, v in record.items():
            if isinstance(v, (datetime, pd.Timestamp)):
                record[k] = str(v)
        send_to_kafka("csv-topic", record)

# -----------------------------
# Point d'entrée
# -----------------------------
def main():
    load_csv()

if __name__ == "__main__":
    main()