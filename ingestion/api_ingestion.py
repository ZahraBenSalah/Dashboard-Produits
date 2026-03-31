# ingestion/api_ingestion.py
import requests
import pandas as pd
from datetime import datetime
import os

def fetch_api_data():
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)

        df["source"] = "api"
        df["ingestion_date"] = datetime.now()

        # ✅ chemin ABSOLU
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        output_path = os.path.join(BASE_DIR, "data/raw/products_api.csv")

        os.makedirs(os.path.dirname(output_path), exist_ok=True)  # créer dossier si nécessaire
        df.to_csv(output_path, index=False)

        print("✅ API data saved at:", output_path)
    else:
        print("❌ API error:", response.status_code)

# ✅ Fonction main pour Prefect
def main():
    fetch_api_data()

# permet d'exécuter ce fichier directement
if __name__ == "__main__":
    main()