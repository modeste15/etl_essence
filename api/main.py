from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv

from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator


load_dotenv()


app = FastAPI(
    title="API Stations Carburant",
    description="API pour exploiter les données du pipeline ETL carburant",
    version="1.0"
)

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", 5432))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


Instrumentator().instrument(app).expose(app)

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

class PDV(BaseModel):
    pdv_id: str
    latitude: str
    longitude: str
    cp: str
    pop: Optional[str]
    adresse: Optional[str]
    ville: Optional[str]


class Service(BaseModel):
    id: int
    nom: str


class Produit(BaseModel):
    id: int
    nom: str


class Prix(BaseModel):
    pdv_id: str
    produit: str
    valeur: float
    maj: datetime


class StationDetail(BaseModel):
    pdv_id: str
    adresse: Optional[str]
    ville: Optional[str]
    cp: str
    services: List[str]
    carburants: List[Prix]




@app.get("/")
def root():
    return {"message": "API Carburant active"}


# ==========================
# STATIONS
# ==========================

@app.get("/stations", response_model=List[PDV])
def get_stations(
        ville: Optional[str] = None,
        cp: Optional[str] = None,
):

    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    query = "SELECT * FROM pdv WHERE 1=1"
    params = []

    if ville:
        query += " AND LOWER(ville) LIKE LOWER(%s)"
        params.append(f"%{ville}%")

    if cp:
        query += " AND cp = %s"
        params.append(cp)


    cur.execute(query, params)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


@app.get("/stations/{pdv_id}", response_model=StationDetail)
def get_station(pdv_id: str):
    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT * FROM pdv WHERE pdv_id=%s", (pdv_id,))
    station = cur.fetchone()
    if not station:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Station non trouvée")

    # Récupération des services
    cur.execute("""
        SELECT s.nom
        FROM services s
        JOIN pdv_service ps ON ps.service_id = s.id
        WHERE ps.pdv_id=%s
    """, (pdv_id,))
    services = [s["nom"] for s in cur.fetchall()]

    # Récupération des prix
    cur.execute("""
        SELECT pr.nom as produit, p.valeur, p.maj, p.pdv_id
        FROM prix_pdv p
        JOIN produit pr ON pr.id = p.produit_id
        WHERE p.pdv_id=%s
        ORDER BY p.maj DESC
    """, (pdv_id,))
    
    prix_raw = cur.fetchall()
    # Transformation pour correspondre au modèle Pydantic
    prix = [
        {
            "produit": p["produit"],
            "valeur": float(p["valeur"]),  
            "maj": p["maj"].isoformat(),   
            "pdv_id": p["pdv_id"]          
        }
        for p in prix_raw
    ]

    cur.close()
    conn.close()

    return {
        "pdv_id": station["pdv_id"],
        "adresse": station["adresse"],
        "ville": station["ville"],
        "cp": station["cp"],
        "services": services,
        "carburants": prix
    }




@app.get("/services", response_model=List[Service])
def get_services():

    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT * FROM services ORDER BY nom")

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows



@app.get("/produits", response_model=List[Produit])
def get_produits():

    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT * FROM produit ORDER BY nom")

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


@app.get("/villes/{ville_cp}")
def get_villes(ville_cp: str):

    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
    SELECT 
        MIN(cp) AS cp,
        INITCAP(LOWER(ville)) || ' (' || MIN(cp) || ')' AS ville_cp
    FROM pdv
    WHERE LOWER(ville) LIKE LOWER(%s) OR cp = %s
    GROUP BY LOWER(ville), MIN(cp)
    ORDER BY ville_cp
""", (f"%{ville_cp}%", ville_cp))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows




@app.get("/prix/{carburant}")
def get_prix_carburant(carburant: str, limit: int = 50):

    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT p.pdv_id, pr.nom as produit, p.valeur, p.maj
        FROM prix_pdv p
        JOIN produit pr ON pr.id = p.produit_id
        WHERE LOWER(pr.nom) = LOWER(%s)
        ORDER BY p.valeur ASC
        LIMIT %s
    """, (carburant, limit))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows




@app.get("/stations-moins-cheres/{carburant}")
def stations_moins_cheres(carburant: str, limit: int = 10):

    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT pdv.ville, pdv.adresse, p.valeur, p.maj
        FROM prix_pdv p
        JOIN produit pr ON pr.id = p.produit_id
        JOIN pdv ON pdv.pdv_id = p.pdv_id
        WHERE LOWER(pr.nom) = LOWER(%s)
        ORDER BY p.valeur ASC
        LIMIT %s
    """, (carburant, limit))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


    @app.get("/stations-prix-range")
    def stations_prix_range(
        prix_min: float = Query(0),
        prix_max: float = Query(2.0),
        carburant: Optional[str] = None,
        limit: int = Query(50, le=200)
    ):
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT DISTINCT pdv.pdv_id, pdv.adresse, pdv.ville, pdv.cp, 
                            pr.nom as carburant, p.valeur, p.maj
            FROM prix_pdv p
            JOIN produit pr ON pr.id = p.produit_id
            JOIN pdv ON pdv.pdv_id = p.pdv_id
            WHERE p.valeur BETWEEN %s AND %s
        """
        params = [prix_min, prix_max]

        if carburant:
            query += " AND LOWER(pr.nom) = LOWER(%s)"
            params.append(carburant)

        query += " ORDER BY p.valeur ASC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows



"""

    requete pour power bi :

"""


@app.get("/pbi/stations")
    def stations_pbi(
        
    ):
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT *
            FROM pdv
        """


        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows


@app.get("/pbi/services")
    def services_pbi(
        
    ):
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT *
            FROM services
        """


        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows

@app.get("/pbi/pdv_services")
    def pdv_services_pbi(
        
    ):
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT *
            FROM pdv_services
        """


        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows


@app.get("/pbi/prix_pdv")
    def prix_pdv_pbi(
        
    ):
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT *
            FROM prix_pdv
        """


        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows


@app.get("/pbi/produits")
    def produits_pbi(
        
    ):
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT *
            FROM produits
        """


        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows