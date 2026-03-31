# transformation/batch_transformation.py
import sqlite3
import pandas as pd
from datetime import datetime
import os
import logging

# -----------------------
# Configuration logging
# -----------------------
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'pipeline.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    logging.info("📥 Début du pipeline de transformation")

    try:
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_path = os.path.join(BASE_DIR, "data", "data_warehouse.db")
        conn = sqlite3.connect(db_path)
        logging.info(f"✅ Connexion à SQLite réussie: {db_path}")
    except Exception as e:
        logging.error(f"❌ Erreur connexion SQLite: {e}")
        return

    try:
        products_df = pd.read_sql("SELECT * FROM products", conn)
        prices_df = pd.read_sql("SELECT * FROM prices", conn)
        logging.info(f"✅ Tables chargées: products ({len(products_df)} lignes), prices ({len(prices_df)} lignes)")
    except Exception as e:
        logging.error(f"❌ Erreur lecture tables SQLite: {e}")
        conn.close()
        return

    # -----------------------
    # Nettoyage produits
    # -----------------------
    try:
        products_df = products_df.dropna(subset=['title', 'category'])
        products_df = products_df.drop_duplicates(subset=['title', 'category', 'sub_category'])
        logging.info(f"✅ Produits nettoyés: {len(products_df)} lignes restantes après suppression NA/duplicates")
    except Exception as e:
        logging.error(f"❌ Erreur nettoyage produits: {e}")

    # -----------------------
    # Nettoyage prix
    # -----------------------
    try:
        prices_df['price'] = pd.to_numeric(prices_df['price'], errors='coerce')
        prices_df['actual_price'] = pd.to_numeric(prices_df['actual_price'], errors='coerce')
        prices_df = prices_df.dropna(subset=['price', 'actual_price'])
        logging.info(f"✅ Prix nettoyés: {len(prices_df)} lignes restantes après conversion et drop NA")
    except Exception as e:
        logging.error(f"❌ Erreur nettoyage prix: {e}")

    # -----------------------
    # Enrichissement
    # -----------------------
    try:
        prices_df['discount'] = prices_df['actual_price'] - prices_df['price']
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        products_df['transformation_date'] = now
        prices_df['transformation_date'] = now
        logging.info("✅ Enrichissement: discount calculé et dates ajoutées")
    except Exception as e:
        logging.error(f"❌ Erreur enrichissement: {e}")

    # -----------------------
    # Jointure produits + prix
    # -----------------------
    try:
        agg_df = products_df.merge(prices_df, on='product_id', how='inner')
        logging.info(f"✅ Jointure effectuée: {len(agg_df)} lignes après merge")
    except Exception as e:
        logging.error(f"❌ Erreur jointure: {e}")

    # -----------------------
    # Agrégation par catégorie et sous-catégorie
    # -----------------------
    try:
        category_summary = agg_df.groupby(['category', 'sub_category']).agg(
            avg_price=('price', 'mean'),
            min_price=('price', 'min'),
            max_price=('price', 'max'),
            avg_discount=('discount', 'mean'),
            num_products=('product_id', 'count')
        ).reset_index()
        logging.info(f"✅ Agrégation terminée: {len(category_summary)} lignes dans category_summary")
    except Exception as e:
        logging.error(f"❌ Erreur agrégation: {e}")

    # -----------------------
    # Sauvegarde dans SQLite
    # -----------------------
    try:
        products_df.to_sql('products_clean', conn, if_exists='replace', index=False)
        prices_df.to_sql('prices_enriched', conn, if_exists='replace', index=False)
        category_summary.to_sql('category_summary', conn, if_exists='replace', index=False)
        logging.info("✅ Sauvegarde terminée: tables products_clean, prices_enriched, category_summary")
    except Exception as e:
        logging.error(f"❌ Erreur sauvegarde tables SQLite: {e}")

    conn.close()
    logging.info("📤 Pipeline terminé et connexion SQLite fermée")


if __name__ == "__main__":
    main()