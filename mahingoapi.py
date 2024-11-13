from flask import Flask, jsonify, Response
import redis
import logging
import boto3
import json
from boto3.dynamodb.conditions import Key
import threading
import time
from botocore.exceptions import ClientError
from datetime import datetime

# Initialiser l'application Flask
app = Flask(__name__)

# Configurer la connexion à Redis
r = redis.Redis(host='iot.bloctechno.com', port=6379, db=0, decode_responses=True)

# Configuration de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')

# Configurer le logging
logging.basicConfig(level=logging.INFO)


# Fonction qui recupere les données depuis Redis
def get_data_from_redis():
    """ Récupère toutes les données dans Redis  """
    all_data = {}

    try:
        # Utilisation de scan_iter pour une meilleure performance
        for key in r.scan_iter('*'):
            value = r.get(key)
            if value is not None:
                all_data[key] = value
            else:
                all_data[key] = 'None'
        
        return all_data
    
    except Exception as e:
        # Gestion des erreurs
        print("Erreur lors de la récupération des données depuis Redis:", str(e))
        return jsonify({'error': 'Une erreur est survenue lors de la récupération des données'}), 500


# Transformer les chiffres en chaine de caractere
def all_data_in_str(d):
    """ Convertit tous les éléments dans un dictionnaire ou une liste en chaînes de caractères. """
    if isinstance(d, dict):
        # Si c'est un dictionnaire, on parcourt chaque élément et on applique récursivement la transformation
        return {k: all_data_in_str(v) for k, v in d.items()}
    elif isinstance(d, list):
        # Si c'est une liste, on applique récursivement la transformation à chaque élément
        return [all_data_in_str(i) for i in d]
    else:
        # Sinon, on transforme directement en chaîne de caractères
        return str(d)


# Ajouter une clé en haut du dictionnaire
def add_key_to_data(key, val, dct):
    new_dict = {key: val}
    new_dict.update(dct)
    return new_dict


# Recode le timestamp en TimesTemp
def change_key_timestamp(d):
    if "timestamp" in d:
        y = d.pop("timestamp")  
        d = add_key_to_data("TimesTemp", y, d)
    return d


# Endpoint pour l'application mobile pour récupérer les données nettoyées de Redis
@app.route('/get_clean_data', methods=['GET'])
def get_clean_data():
    try:
        # Récupérer les données brutes depuis Redis
        raw_data = get_data_from_redis()
        
        # Transformer les données en chaînes de caractères et ajouter la clé TimesTemp
        cleaned_data = {k: change_key_timestamp(all_data_in_str(json.loads(v))) for k, v in raw_data.items()}
        
        # Retourner les données nettoyées dans un format JSON
        return jsonify(cleaned_data), 200
    except Exception as e:
        logging.error(f"Erreur lors de la récupération ou du nettoyage des données : {str(e)}")
        return jsonify({'error': 'Une erreur est survenue lors du traitement des données'}), 500


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)

