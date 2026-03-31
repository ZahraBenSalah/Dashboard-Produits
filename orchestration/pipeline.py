from prefect import task, flow
import os
import sys

# -----------------------
# 0️⃣ Ajouter le dossier racine au PYTHONPATH
# -----------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# -----------------------
# 1️⃣ Importer directement les fonctions des scripts
# -----------------------
from ingestion import api_ingestion, csv_ingestion
from storage import sqlite_storage
from transformation import batch_transformation

# -----------------------
# 2️⃣ Tâches Prefect
# -----------------------
@task
def run_api_ingestion():
    api_ingestion.main()  # Assurez-vous d'avoir une fonction main() dans api_ingestion.py

@task
def run_csv_ingestion():
    csv_ingestion.main()  # Assurez-vous d'avoir une fonction main() dans csv_ingestion.py

@task
def run_storage():
    sqlite_storage.main()  # Assurez-vous d'avoir une fonction main() dans sqlite_storage.py

@task
def run_transformation():
    batch_transformation.main()  # Assurez-vous d'avoir une fonction main() dans batch_transformation.py

# -----------------------
# 3️⃣ Pipeline / Flow Prefect
# -----------------------
@flow(name="Pipeline Data Warehouse")
def etl_pipeline():
    run_api_ingestion()
    run_csv_ingestion()
    run_storage()
    run_transformation()

# -----------------------
# 4️⃣ Exécution
# -----------------------
if __name__ == "__main__":
    etl_pipeline()