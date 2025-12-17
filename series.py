from pymongo import MongoClient
import os
from dotenv import load_dotenv
import random
import json

load_dotenv()

user = os.getenv("USUARIO")
password = os.getenv("PASSWORD")
name_cluster = os.getenv("NAME_CLUSTER")

cliente = MongoClient(f'mongodb+srv://{user}:{password}@{name_cluster}.b692yrn.mongodb.net/')

try:
    cliente.admin.command('ping')
    print("Conexión existosa...\n")
except Exception as e:
    print(e)

# Creación de la base de datos y colección
baseDatos = cliente["TV_StreamDB"]
coleccion = baseDatos["series"]

def insert_data():
    titulos = [
        "Stranger Things", "Breaking Bad", "The Office", "Game of Thrones",
        "The Mandalorian", "Dark", "Narcos", "The Witcher", "Lost",
        "Peaky Blinders", "Vikings", "Sherlock", "Friends", "House of Cards",
        "Black Mirror"
    ]

    plataformas = ["Netflix", "HBO", "Disney+", "Prime Video", "Apple TV+"]

    generos = [
        "Drama", "Comedia", "Sci-Fi", "Crimen",
        "Fantasia", "Aventura", "Thriller"
    ]

    # Insertar 50 series ficticias
    series = []

    for i in range(50):
        serie = {
            "titulo": random.choice(titulos) + f" {i+1}", # para evitar duplicados
            "plataforma": random.choice(plataformas),
            "temporadas": random.randint(1, 10),
            "genero": random.sample(generos, random.randint(1, 3)),
            "puntuacion": round(random.uniform(5.0, 9.9), 1),
            "finalizada": random.choice([True, False]),
            "año_estreno": random.randint(1995, 2023)
        }
        series.append(serie)

    resultado = coleccion.insert_many(series)
    print(f"Se han insertado {len(resultado.inserted_ids)} series correctamente")

    # Insertar 10 series sin puntuacion ni finalizada
    series_incompletas = []

    for i in range(10):
        serie = {
            "titulo": f"Serie Incompleta {i+1}",
            "plataforma": random.choice(plataformas),
            "temporadas": random.randint(1, 5),
            "genero": random.sample(generos, random.randint(1, 2)),
            "año_estreno": random.randint(2000, 2023)
            # sin puntuacion
            # sin finalizada
        }
        series_incompletas.append(serie)

    coleccion.insert_many(series_incompletas)

def consult_data():
    resultados = {}

    # Maratones largos
    resultados["maraton_largo"] = list(coleccion.find({
        "temporadas": { "$gte": 5 },
        "puntuacion": { "$gte": 8.0 }
    }))

    # Joyas recientes de la comedia
    resultados["joyas_comedia"] = list(coleccion.find({
        "genero": "Comedia",
        "año_estreno": { "$gte": 2020 }
    }))

    # Series finalizadas
    resultados["finalizadas"] = list(coleccion.find({
        "finalizada": True
    }))

    # Series sin puntuación con más de 3 temporadas o estrenadas antes de 2010
    resultados["sin_puntuacion"] = list(coleccion.find({
        "puntuacion": { "$exists": False },
        "$or": [
            { "temporadas": { "$gt": 3 } },
            { "año_estreno": { "$lt": 2010 } }
        ]
    }))

    # Series de plataformas específicas ordenadas por puntuación
    plataformas_especificas = ["Netflix", "HBO"]
    resultados["series_plataformas"] = list(coleccion.find({
        "plataforma": { "$in": plataformas_especificas }
    }).sort("puntuacion", -1))

    return resultados

def read_consult(resultados):
    for nombre, lista in resultados.items():
        print(f"\n--- {nombre.upper()} ---")
        for serie in lista:
            print(serie)
    

def transform_BSON_to_JSON(resultados):
    for nombre, lista in resultados.items():
        # Convertir ObjectId a string
        for serie in lista:
            serie["_id"] = str(serie["_id"])

        # Guardar en archivo JSON
        with open(f"{nombre}.json", "w", encoding="utf-8") as f:
            json.dump(lista, f, ensure_ascii=False, indent=2)
        print(f"Guardado: {nombre}.json")
        
def mean_puntuacion():
    total_puntuacion = 0
    count = 0

    for serie in coleccion.find({"puntuacion": {"$exists": True}}):
        total_puntuacion += serie["puntuacion"]
        count += 1

    if count > 0:
        promedio = total_puntuacion / count
        print(f"\nPromedio de puntuación de todas las series: {promedio:.2f}")
    else:
        print("\nNo hay series con puntuación para calcular el promedio.")

def create_new_collection():
    coleccion2 = baseDatos["detalles_produccion"]

    # Leear todos los títulos de la colección series
    titulos_series = [serie["titulo"] for serie in coleccion.find()]

    # Opciones aleatorias
    paises = ["EE.UU.", "Corea del Sur", "España", "Reino Unido", "Canadá", "Japón"]
    actores = [
        "Actor A", "Actor B", "Actor C", "Actor D", "Actor E",
        "Actor F", "Actor G", "Actor H", "Actor I", "Actor J"
    ]

    produccion = []

    for titulo in titulos_series:
        doc = {
            "titulo": titulo,
            "pais_origen": random.choice(paises),
            "reparto_principal": random.sample(actores, 3),
            "presupuesto_por_episodio": round(random.uniform(1.0, 10.0), 2)  # en millones
        }
        produccion.append(doc)

    coleccion2.insert_many(produccion)
    print(f"Se han insertado {len(produccion)} documentos en detalles_produccion")

    return coleccion2

# Consultar series que esten finalizadas, con más de 8 de puntacuíon y sean de EEUU.
def consult_union(coleccion2):
    # Hacemos un join entre series y detalles_produccion usando el campo titulo
    pipeline = [
        {
            "$lookup": {
                "from": "series",
                "localField": "titulo",
                "foreignField": "titulo",
                "as": "info_serie"
            }
        },
        { "$unwind": "$info_serie" },
        { "$match": {
            "info_serie.finalizada": True,
            "info_serie.puntuacion": { "$gt": 8 },
            "pais_origen": "EE.UU."
        }}
    ]

    resultados = list(coleccion2.aggregate(pipeline))

    print(f"Series finalizadas, puntuación >8 y país EE.UU.: {len(resultados)}")
    for serie in resultados:
        print({
            "titulo": serie["titulo"],
            "puntuacion": serie["info_serie"]["puntuacion"],
            "finalizada": serie["info_serie"]["finalizada"],
            "pais_origen": serie["pais_origen"],
            "reparto_principal": serie["reparto_principal"]
        })

    return resultados

#insert_data()
resultados = consult_data()
# transform_BSON_to_JSON(resultados)
# read_consult(resultados)
# mean_puntuacion()
coleccion2 = create_new_collection()
consult_union(coleccion2)