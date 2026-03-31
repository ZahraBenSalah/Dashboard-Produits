# storage/sqlite_storage.py
import sqlite3
import pandas as pd
from datetime import datetime
import os


def clean_price(col):
    """Nettoyer les colonnes prix (₹, â‚¹, virgules, espaces...)"""
    return (
        col.astype(str)
        .str.replace("â‚¹", "", regex=False)
        .str.replace("₹", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
    )


def main():
    # -----------------------------
    # Chemins
    # -----------------------------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(BASE_DIR, "data", "data_warehouse.db")
    raw_dir = os.path.join(BASE_DIR, "data", "raw")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # -----------------------------
    # Connexion SQLite
    # -----------------------------
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # -----------------------------
    # Création tables
    # -----------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        category TEXT,
        sub_category TEXT,
        image TEXT,
        source TEXT,
        ingestion_date TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        price_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        price REAL,
        actual_price REAL,
        date TEXT,
        source TEXT
    )
    """)

    conn.commit()

    # -----------------------------
    # Charger CSV
    # -----------------------------
    csv_files = [
        f for f in os.listdir(raw_dir)
        if f.endswith(".csv") and ("clean" in f or "amazon" in f)
    ]

    all_dfs = []

    for file in csv_files:
        path = os.path.join(raw_dir, file)
        try:
            df = pd.read_csv(path)
            all_dfs.append(df)
            print(f"✅ Chargé : {file}")
        except Exception as e:
            print(f"❌ Erreur lecture {file} : {e}")

    # API
    api_path = os.path.join(raw_dir, "products_api.csv")
    if os.path.exists(api_path):
        df_api = pd.read_csv(api_path)
        all_dfs.append(df_api)
        print("✅ API ajoutée")

    if not all_dfs:
        print("❌ Aucun fichier trouvé")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # -----------------------------
    # Nettoyage général
    # -----------------------------
    combined_df['ingestion_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if 'name' in combined_df.columns:
        combined_df = combined_df.dropna(subset=['name'])
        combined_df['name'] = combined_df['name'].astype(str).str.strip()

    # -----------------------------
    # 🔥 Nettoyage PRIX
    # -----------------------------
    if 'discount_price' in combined_df.columns:
        combined_df['discount_price'] = clean_price(combined_df['discount_price'])
        combined_df['discount_price'] = pd.to_numeric(combined_df['discount_price'], errors='coerce')

    if 'actual_price' in combined_df.columns:
        combined_df['actual_price'] = clean_price(combined_df['actual_price'])
        combined_df['actual_price'] = pd.to_numeric(combined_df['actual_price'], errors='coerce')

    # ❗ SUPPRIMER les lignes invalides (TRÈS IMPORTANT)
    combined_df = combined_df.dropna(subset=['discount_price', 'actual_price'])

    # -----------------------------
    # INSERT PRODUCTS
    # -----------------------------
    products_df = pd.DataFrame({
        'title': combined_df.get('name', combined_df.get('title', 'Unknown')),
        'category': combined_df.get('main_category', combined_df.get('category', 'Unknown')),
        'sub_category': combined_df.get('sub_category', None),
        'image': combined_df.get('image', None),
        'source': combined_df.get('source', 'Unknown'),
        'ingestion_date': combined_df['ingestion_date']
    })

    products_df.to_sql('products', conn, if_exists='append', index=False)

    # -----------------------------
    # INSERT PRICES
    # -----------------------------
    last_id = cursor.execute("SELECT MAX(product_id) FROM products").fetchone()[0]
    start_id = last_id - len(products_df) + 1

    prices_df = pd.DataFrame({
        'product_id': range(start_id, last_id + 1),
        'price': combined_df['discount_price'],
        'actual_price': combined_df['actual_price'],
        'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'source': combined_df.get('source', 'Unknown')
    })

    prices_df.to_sql('prices', conn, if_exists='append', index=False)

    print(f"✅ Données stockées ! Lignes valides: {len(prices_df)}")

    # Debug
    print("🔎 Exemple prix:")
    print(prices_df.head())

    conn.close()


if __name__ == "__main__":
    main()