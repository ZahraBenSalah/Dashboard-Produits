# 📊 Dashboard Produits et Prix

## 1. Description du projet
Ce projet permet de collecter, nettoyer, transformer et visualiser des données produits depuis des fichiers CSV et API.  
Il fournit un dashboard interactif avec **Streamlit** pour explorer les produits, leurs prix et remises, par catégorie et sous-catégorie.

Le pipeline complet couvre :
- Ingestion des données depuis CSV et API
- Nettoyage et standardisation des données
- Transformation et enrichissement (calcul des remises)
- Agrégation par catégorie et sous-catégorie
- Stockage dans SQLite
- Visualisation interactive via Streamlit + Plotly

---

## 2. Stack technique
- **Python 3.8+** – langage principal
- **Pandas** – manipulation et transformation des données
- **SQLite** – stockage local des données
- **Streamlit** – dashboard web interactif
- **Plotly** – graphiques dynamiques
- **Docker** – conteneurisation pour déploiement facile

---

## 3. Architecture du pipeline

```text
[CSV / API] 
      ↓
[SQLite Storage] 
      ↓
[Batch Transformation] → [Logs / Monitoring]
      ↓
[Products Clean / Prices Enriched / Category Summary]
      ↓
[Streamlit Dashboard (app.py)]