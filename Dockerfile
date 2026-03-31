# Dockerfile
FROM python:3.11-slim

# Variables d'environnement pour Streamlit
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_PORT=8501
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copier requirements et installer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code
COPY . .

# Lancer l'app
CMD ["streamlit", "run", "app.py"]