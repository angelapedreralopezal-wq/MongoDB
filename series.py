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

def transform_BSON_to_JSON(resultados):
    for nombre, lista in resultados.items():
        # Convertir ObjectId a string
        for serie in lista:
            serie["_id"] = str(serie["_id"])

        # Guardar en archivo JSON
        with open(f"{nombre}.json", "w", encoding="utf-8") as f:
            json.dump(lista, f, ensure_ascii=False, indent=2)
        print(f"Guardado: {nombre}.json")
        
#insert_data()
resultados = consult_data()
transform_BSON_to_JSON(resultados)