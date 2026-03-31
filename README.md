# Dashboard Produits et Prix

## 1-Description du projet

Ce projet consiste à construire un **pipeline complet de traitement de données** pour des produits e-commerce, incluant :  
- **Ingestion** de données depuis CSV et API.  
- **Nettoyage et transformation** des données (prix, catégories, sous-catégories).  
- **Stockage** dans une base de données SQLite.  
- **Dashboard interactif** avec Streamlit pour visualiser :  
  - Nombre de produits  
  - Prix moyen et remise moyenne  
  - Distribution des prix et remises  
  - Résumé par catégorie et sous-catégorie  

Le projet illustre la gestion d’un **pipeline ETL simple mais complet** et l’intégration d’un dashboard interactif.

---

## 2-Stack technique

- **Python 3.8+** – langage principal  
- **Pandas** – nettoyage et transformation de données  
- **SQLite** – stockage local des données  
- **Streamlit** – création du dashboard interactif  
- **Plotly** – graphiques interactifs dans Streamlit  
- **Docker** – déploiement et containerisation  
- **Prefect (optionnel)** – suivi des logs ETL / monitoring avancé
- **Apache Kafka** – ingestion streaming temps réel  

---

### 3️⃣ Architecture du pipeline

```text
           ┌───────────────┐
           │   Ingestion   │ (CSV / API / Kafka)
           └───────┬───────┘
                   │
           ┌───────▼───────┐
           │   Nettoyage   │ (supprimer NaN, convertir prix, supprimer doublons)
           └───────┬───────┘
                   │
           ┌───────▼─────────────┐
           │ Transformation       │
           │ (calcul discount,    │
           │ agrégation par cat.) │
           └───────┬─────────────┘
                   │
           ┌───────▼─────────┐
           │ Stockage SQLite │ (products_clean, prices_enriched, category_summary)
           └───────┬─────────┘
                   │
           ┌───────▼─────────┐
           │  Dashboard      │
           │ Streamlit + Plotly
           └─────────────────┘
```
---
## 4-Sources de données utilisées
- CSV produits nettoyés (`raw/clean_*.csv`)  
- Fichiers CSV provenant d’Amazon (`raw/amazon_*.csv`)  
- API produit (`raw/products_api.csv`)
- Kafka → simulation ingestion temps réel

---

## 5-Stack technologique avec justifications
| Technologie | Rôle / Justification |
|------------|---------------------|
| Python | Langage principal pour l’ETL et le dashboard |
| Pandas | Manipulation et transformation des données |
| SQLite | Base légère pour stocker produits, prix et agrégations |
| Streamlit | Dashboard interactif et visualisation facile |
| Plotly | Graphiques interactifs pour les prix et remises |
| Docker | Déploiement reproductible et isolé |
| Prefect / Logs | Suivi et monitoring des pipelines ETL |
|Kafka   |Streaming temps réel pour ingestion et simulation

---

## 6-Instructions d'installation

1. **Cloner le dépôt** :
```bash
git clone https://github.com/ZahraBenSalah/Dashboard-Produits.git
cd Dashboard Produits
Installer les dépendances :
pip install -r requirements.txt
Lancer le pipeline ETL :
python -m storage.sqlite_storage
python -m transformation.batch_transformation
python3 -m orchestration.pipeline 
Démarrer le dashboard Streamlit :
streamlit run app.py
Option Docker :
docker build -t dashboard-produits .
docker run -p 8501:8501 dashboard-produits

Accéder ensuite à : http://localhost:8501
```
## 7-Credentials / URL d'accès au déploiement
URL : http://localhost:8501 (local)
Aucun login requis pour cette version
