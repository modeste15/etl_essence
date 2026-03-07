from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile
import os
import psycopg2
import psycopg2.extras
import time
import xml.etree.ElementTree as ET
import json
import logging
from psycopg2.extras import Json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

AIRFLOW_DB_HOST = os.getenv("AIRFLOW_DB_HOST")
AIRFLOW_DB_NAME = os.getenv("AIRFLOW_DB_NAME")
AIRFLOW_DB_USER = os.getenv("AIRFLOW_DB_USER")
AIRFLOW_DB_PASSWORD = os.getenv("AIRFLOW_DB_PASSWORD")

APP_DB_HOST = os.getenv("APP_DB_HOST")
APP_DB_NAME = os.getenv("APP_DB_NAME")
APP_DB_USER = os.getenv("APP_DB_USER")
APP_DB_PASSWORD = os.getenv("APP_DB_PASSWORD")


DATA_DIR = "/data"
DEZIP_DIR = "/data/dezipper"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DEZIP_DIR, exist_ok=True)


def get_db_airflow_conn():

    conn = psycopg2.connect(
        host=AIRFLOW_DB_HOST,
        database=AIRFLOW_DB_NAME,
        user=AIRFLOW_DB_USER,
        password=AIRFLOW_DB_PASSWORD,
        port=5432
    )

    try:
        psycopg2.extras.register_default_jsonb(conn_or_curs=conn, loads=json.loads)
    except Exception:
        logger.debug("Impossible d'enregistrer le convertisseur JSONB, fallback activé.")
    return conn

def get_db_app_conn():

    conn = psycopg2.connect(
        host=APP_DB_HOST,
        database=APP_DB_NAME,
        user=APP_DB_USER,
        password=APP_DB_PASSWORD,
        port=5432
    )

    try:
        psycopg2.extras.register_default_jsonb(conn_or_curs=conn, loads=json.loads)
    except Exception:
        logger.debug("Impossible d'enregistrer le convertisseur JSONB, fallback activé.")
    return conn


def parse_json_field(value):

    if value is None:
        return []
    if isinstance(value, (list, dict)):
        return value
    try:
        text = str(value).strip()
        if text == "":
            return []
        return json.loads(text)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning("Impossible de parser le champ JSON: %s — valeur: %r", e, value)
        return []


def download_data():

    url = "https://donnees.roulez-eco.fr/opendata/instantane"
    logger.info("Téléchargement des données depuis %s", url)
    response = requests.get(url)
    response.raise_for_status()

    timestamp = int(time.time())
    zip_path = os.path.join(DATA_DIR, f"instantane_{timestamp}.zip")

    with open(zip_path, "wb") as f:
        f.write(response.content)
    logger.info("Archive téléchargée: %s", zip_path)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(DEZIP_DIR)
    logger.info("Archive extraite dans %s", DEZIP_DIR)


def read_data():

    logger.info("Lecture des fichiers XML dans %s", DEZIP_DIR)
    with get_db_airflow_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bronze_data (
                    id SERIAL PRIMARY KEY,
                    pdv_id TEXT,
                    latitude TEXT,
                    longitude TEXT,
                    cp TEXT,
                    pop TEXT,
                    adresse TEXT,
                    ville TEXT,
                    services JSONB,
                    prix JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

            for file_name in os.listdir(DEZIP_DIR):
                if not file_name.endswith(".xml"):
                    continue
                file_path = os.path.join(DEZIP_DIR, file_name)
                logger.info("Traitement du fichier %s", file_path)
                try:
                    tree = ET.parse(file_path)
                    root = tree.getroot()
                except Exception as e:
                    logger.warning("Impossible de parser %s: %s", file_path, e)
                    continue

                for pdv in root.findall("pdv"):
                    pdv_id = pdv.get("id")
                    latitude = pdv.get("latitude")
                    longitude = pdv.get("longitude")
                    cp = pdv.get("cp")
                    pop = pdv.get("pop")

                    adresse = pdv.findtext("adresse")
                    ville = pdv.findtext("ville")

                    services = pdv.find("services")
                    services_list = [s.text for s in services.findall("service")] if services is not None else []

                    prix_list = []
                    for prix in pdv.findall("prix"):
                        prix_list.append({
                            "nom": prix.get("nom"),
                            "valeur": prix.get("valeur"),
                            "maj": prix.get("maj")
                        })

                    try:
                        # Utiliser Json(...) pour insérer proprement en JSONB
                        cur.execute(
                            """
                            INSERT INTO bronze_data (
                                pdv_id, latitude, longitude, cp, pop, adresse, ville, services, prix
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                pdv_id, latitude, longitude, cp, pop, adresse, ville,
                                Json(services_list),
                                Json(prix_list)
                            )
                        )
                    except Exception as e:
                        logger.warning("Erreur insertion bronze_data pour pdv_id=%s : %s", pdv_id, e)
                conn.commit()
    logger.info("Lecture et insertion dans bronze_data terminées.")


def bronze_to_silver():

    logger.info("Début transformation bronze -> silver")
    with get_db_app_conn() as conn_app:
        with conn_app.cursor() as cur_app:
            # Création des tables si nécessaire
            cur_app.execute("""
            CREATE TABLE IF NOT EXISTS pdv (
                pdv_id TEXT PRIMARY KEY,
                latitude TEXT,
                longitude TEXT,
                cp TEXT,
                pop TEXT,
                adresse TEXT,
                ville TEXT
            );
            """)
            cur_app.execute("""
            CREATE TABLE IF NOT EXISTS services (
                id SERIAL PRIMARY KEY,
                nom TEXT UNIQUE
            );
            """)
            cur_app.execute("""
            CREATE TABLE IF NOT EXISTS pdv_service (
                pdv_id TEXT REFERENCES pdv(pdv_id),
                service_id INT REFERENCES services(id),
                PRIMARY KEY (pdv_id, service_id)
            );
            """)
            cur_app.execute("""
            CREATE TABLE IF NOT EXISTS produit (
                id SERIAL PRIMARY KEY,
                nom TEXT UNIQUE
            );
            """)
            cur_app.execute("""
            CREATE TABLE IF NOT EXISTS prix_pdv (
                pdv_id TEXT REFERENCES pdv(pdv_id),
                produit_id INT REFERENCES produit(id),
                valeur NUMERIC,
                maj TIMESTAMP,
                PRIMARY KEY (pdv_id, produit_id, maj)
            );
            """)
            conn_app.commit()

        with get_db_airflow_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT pdv_id, latitude, longitude, cp, pop, adresse, ville, services, prix FROM bronze_data")
            rows = cur.fetchall()
            logger.info("Nombre d'enregistrements bronze à traiter: %d", len(rows))

            for row in rows:
                pdv_id, latitude, longitude, cp, pop, adresse, ville, services_json, prix_json = row

                # Insert PDV si le pdv_id n'existe pas déjà
                try:
                    cur_app.execute("""
                        INSERT INTO pdv(pdv_id, latitude, longitude, cp, pop, adresse, ville)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (pdv_id) DO NOTHING
                    """, (pdv_id, latitude, longitude, cp, pop, adresse, ville))
                except Exception as e:
                    logger.warning("Erreur insertion pdv pour %s: %s", pdv_id, e)
                    continue

                # Normaliser services et insertion
                services_list = parse_json_field(services_json)
                for service in services_list:
                    if not service:
                        continue
                    try:
                        cur_app.execute("INSERT INTO services(nom) VALUES (%s) ON CONFLICT (nom) DO NOTHING", (service,))
                        cur_app.execute("SELECT id FROM services WHERE nom=%s", (service,))
                        res = cur_app.fetchone()
                        if res:
                            service_id = res[0]
                            cur_app.execute("INSERT INTO pdv_service(pdv_id, service_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", (pdv_id, service_id))
                    except Exception as e:
                        logger.warning("Erreur traitement service '%s' pour pdv %s: %s", service, pdv_id, e)

                # Normaliser prix et insertion
                prix_list = parse_json_field(prix_json)
                for prix in prix_list:
                    if not isinstance(prix, dict):
                        logger.warning("Format prix inattendu pour pdv %s: %r", pdv_id, prix)
                        continue
                    nom = prix.get('nom')
                    valeur_raw = prix.get('valeur')
                    maj = prix.get('maj')

                    if not nom:
                        logger.debug("Prix sans nom ignoré pour pdv %s: %r", pdv_id, prix)
                        continue

                    try:
                        valeur = float(valeur_raw) if valeur_raw not in (None, '') else None
                    except Exception:
                        logger.warning("Impossible de convertir valeur en float pour pdv %s, produit %s: %r", pdv_id, nom, valeur_raw)
                        valeur = None

                    try:
                        cur_app.execute("INSERT INTO produit(nom) VALUES (%s) ON CONFLICT (nom) DO NOTHING", (nom,))
                        cur_app.execute("SELECT id FROM produit WHERE nom=%s", (nom,))
                        res = cur_app.fetchone()
                        if not res:
                            logger.warning("Impossible de récupérer id produit pour %s", nom)
                            continue
                        produit_id = res[0]

                        cur_app.execute("""
                            INSERT INTO prix_pdv(pdv_id, produit_id, valeur, maj)
                            VALUES (%s,%s,%s,%s)
                            ON CONFLICT (pdv_id, produit_id, maj) DO NOTHING
                        """, (pdv_id, produit_id, valeur, maj))
                    except Exception as e:
                        logger.warning("Erreur insertion prix pour pdv %s produit %s: %s", pdv_id, nom, e)

            conn_app.commit()
    logger.info("Transformation bronze -> silver terminée.")


def clean_bronze():
    """
    Vide la table bronze_data après transformation.
    """
    logger.info("Nettoyage de bronze_data")
    with get_db_airflow_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bronze_data")
        conn.commit()
    logger.info("Table bronze_data vidée.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bronze_silver_etl',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'silver', 'etl']
) as dag:

    task_download = PythonOperator(
        task_id='download_data',
        python_callable=download_data
    )

    task_read = PythonOperator(
        task_id='read_data',
        python_callable=read_data
    )

    task_bronze_to_silver = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=bronze_to_silver
    )

    task_clean_bronze = PythonOperator(
        task_id='clean_bronze',
        python_callable=clean_bronze
    )

    task_download >> task_read >> task_bronze_to_silver >> task_clean_bronze
