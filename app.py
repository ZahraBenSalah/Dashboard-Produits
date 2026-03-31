# app.py
import streamlit as st
import pandas as pd
import sqlite3
import os
import plotly.express as px
from datetime import datetime

# ------------------------
# Chemin base de données
# ------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "data", "data_warehouse.db")

# ------------------------
# Charger les données
# ------------------------
conn = sqlite3.connect(db_path)
products_df = pd.read_sql("SELECT * FROM products_clean", conn)
prices_df = pd.read_sql("SELECT * FROM prices_enriched", conn)
category_summary = pd.read_sql("SELECT * FROM category_summary", conn)
conn.close()

# ------------------------
# Conversion des colonnes numériques
# ------------------------
prices_df['price'] = pd.to_numeric(prices_df['price'], errors='coerce').fillna(0)
prices_df['actual_price'] = pd.to_numeric(prices_df['actual_price'], errors='coerce').fillna(0)
prices_df['discount'] = pd.to_numeric(prices_df['discount'], errors='coerce').fillna(0)

# ------------------------
# Streamlit Dashboard
# ------------------------
st.set_page_config(page_title="Dashboard Produits", layout="wide")
st.title("📊 Dashboard Produits et Monitoring Avancé")

# ------------------------
# Sidebar navigation
# ------------------------
page = st.sidebar.radio("Navigation", ["Dashboard", "Monitoring"])

# ------------------------
# Filtrage commun
# ------------------------
category_options = products_df['category'].dropna().unique()
category_filter = st.sidebar.multiselect("Filtrer par catégorie", options=category_options, default=category_options)

filtered_products = products_df[products_df['category'].isin(category_filter)]

sub_category_options = filtered_products['sub_category'].dropna().unique()
sub_category_filter = st.sidebar.multiselect("Filtrer par sous-catégorie", options=sub_category_options, default=sub_category_options)

filtered_products = filtered_products[filtered_products['sub_category'].isin(sub_category_filter)]
filtered_prices = prices_df[prices_df['product_id'].isin(filtered_products['product_id'])]

# ------------------------
# Page Dashboard
# ------------------------
if page == "Dashboard":
    st.subheader("Liste des produits")
    st.dataframe(filtered_products)

    st.subheader("📈 Statistiques Globales")
    col1, col2, col3 = st.columns(3)
    col1.metric("Nombre de produits", len(filtered_products))
    col2.metric("Prix moyen", round(filtered_prices['price'].mean(), 2))
    col3.metric("Remise moyenne", round(filtered_prices['discount'].mean(), 2))

    # Graphique prix moyen par catégorie
    agg_prices = filtered_prices.merge(filtered_products, on='product_id')
    fig = px.bar(
        agg_prices.groupby('category')['price'].mean().reset_index(),
        x='category',
        y='price',
        title="Prix moyen par catégorie",
        labels={'price': 'Prix moyen', 'category': 'Catégorie'}
    )
    st.plotly_chart(fig, use_container_width=True)

    # Histogramme des remises
    fig2 = px.histogram(
        agg_prices,
        x='discount',
        nbins=30,
        title="Distribution des remises",
        labels={'discount': 'Remise'}
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Tableau résumé
    st.subheader("Résumé par catégorie et sous-catégorie")
    summary_filtered = category_summary[
        (category_summary['category'].isin(category_filter)) &
        (category_summary['sub_category'].isin(sub_category_filter))
    ]
    st.dataframe(summary_filtered)

# ------------------------
# Page Monitoring Avancé
# ------------------------
if page == "Monitoring":
    st.subheader("📡 Monitoring du pipeline et métriques en temps réel")

    # KPI principaux
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Nombre total produits", len(products_df))
    col2.metric("Nombre total prix", len(prices_df))
    col3.metric("Prix moyen global", round(prices_df['price'].mean(), 2))
    col4.metric("Remise moyenne globale", round(prices_df['discount'].mean(), 2))

    # Historique ingestion par date
    st.markdown("### 📊 Evolution nombre de produits ingérés par jour")
    products_df['ingestion_date'] = pd.to_datetime(products_df['transformation_date'])
    daily_counts = products_df.groupby(products_df['ingestion_date'].dt.date).size().reset_index(name='count')
    fig3 = px.line(daily_counts, x='ingestion_date', y='count', title="Produits ingérés par jour")
    st.plotly_chart(fig3, use_container_width=True)

    # Histogramme prix
    fig4 = px.histogram(prices_df, x="price", nbins=50, title="Distribution des prix")
    st.plotly_chart(fig4, use_container_width=True)

    # Histogramme remises
    fig5 = px.histogram(prices_df, x="discount", nbins=50, title="Distribution des remises")
    st.plotly_chart(fig5, use_container_width=True)

    # Top catégories
    st.markdown("### 🏷 Top catégories")
    top_cat = products_df['category'].value_counts().head(10)
    st.bar_chart(top_cat)

    # Temps exécution du pipeline (simulé)
    import time
    start_time = time.time()
    time.sleep(0.1)  # placeholder pour exécution réelle
    duration = round(time.time() - start_time, 2)
    st.info(f"⏱ Temps d'exécution du pipeline : {duration} sec")