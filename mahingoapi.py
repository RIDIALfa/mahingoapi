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
# configuration du dynamodb
dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
# Configurer le logging
logging.basicConfig(level=logging.INFO)


# Fonction qui recupere les données depuis redis
def get_data_from_redis():
    """ Récupère toutes les données dans Redis  """
    all_data = {}

    try:
        # Utilisation de scan_iter pour une meilleure performance
        for key in r.scan_iter('*'):
            value = r.get(key)
            if value is not None:
                all_data[key]=value
            else:
                all_data[key]='None'
        
        # Affichage des données dans la console pour vérification
        # print("Données récupérées depuis Redis:", all_data)
       
        return all_data
    
    except Exception as e:
        # Gestion des erreurs
        print("Erreur lors de la récupération des données depuis Redis:",  str(e))
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


# ajout de la clé sur au debut 
def add_key_to_data(key,val, dct):
    new_dict={ key: val}
    new_dict.update(dct)
    return new_dict


# recode timestamp in TimesTemp
def change_key_timestamp(d):
    if "timestamp" in d:
        y = d.pop("timestamp")  
        d=add_key_to_data("TimesTemp", y, d)
    return d

# Fonction pour envoyer les données du redis dans le cloud AWS sur bdd dunamodb
def add_data_to_dynamodb(data):
    # Connexion à DynamoDB
    
    table = dynamodb.Table('MahingoAnimalsData') 

    for key, val in data.items():
        val=json.loads(val)
        val=all_data_in_str(val)
        val=add_key_to_data("AnimalsId", key,val)
        table.put_item(Item=val) # Insertion des données dans la table DynamoDB
   
   
    return jsonify({"message": "Donnees inserees avec succes!"}), 200


# Fonction de synchronisation des données
def sync_data():
    data = get_data_from_redis()
    if data:
        add_data_to_dynamodb(data)
        logging.info("Synchronisation des données depuis Redis vers DynamoDB effectuée avec succès")
    else:
        logging.warning("Aucune donnée trouvée dans Redis")

# Fonction qui va tourner en boucle toutes les 3 minutes pour la synchronisation
"""def sync_periodically():
    while True:
        sync_data()  # Appel de la fonction de synchronisation directe, pas via la route
        time.sleep(180)  # 180 secondes = 3 minutes"""

@app.route('/sync_redis_to_dynamodb', methods=['POST'])

def sync_redis_to_dynamodb():
    sync_data()
    return jsonify({'message': f'Data synchronized from Redis to DynamoDB '}), 200


# Créer et démarrer le thread lors du lancement de l'application Flask
"""sync_thread = threading.Thread(target=sync_periodically)
sync_thread.daemon = True
sync_thread.start()"""

################################################################################
################################################################################
###########                                                              #######
########### Functions deleting historical data from temporal db          #######
###########        and insert new data inside                            #######
################################################################################
################################################################################

# Connexion à DynamoDB
temp_table = dynamodb.Table('MahingoAnimalsData_Temp')

# Fonction pour effacer toutes les données de la table temporaire
def clear_temp_table():
    
    unique_animal_id = 'M001'  

    # Suppression directe de la ligne unique
    try:
        # Suppression directe de la ligne unique
        response = temp_table.delete_item(Key={'AnimalsId': unique_animal_id})
        print("La base de données a été nettoyée avec succès.")
    except Exception as e:
        print(f"Erreur lors de la suppression : {str(e)}")

# Fonction pour insérer de nouvelles données dans la table temporaire
def insert_new_data_in_temp(new_data):
    item={
        'AnimalsId': 'M001',
        'timestamp': "2024",
        'datas':new_data
    }
    
    temp_table.put_item(Item=item)
    print('données inserées avec succés')

# Fonction de synchronisation
def sync_temp_data():
    new_data = get_data_from_redis()  # appel de la fonction qui récupère nos nouvelles données
    
    # 1. Nettoyer la table temporaire
    clear_temp_table()
    
    # 2. Insérer les nouvelles données dans la table temporaire
    insert_new_data_in_temp(new_data)
    
    # 3. (Facultatif) Ajouter les nouvelles données à la table historique
    #with historical_table.batch_writer() as batch:
     #   for item in new_data:
      #      batch.put_item(Item=item)


@app.route('/trigger_sync', methods=['GET'])
def trigger_sync():
    sync_temp_data()
    return "Synchronisation terminée avec succès.", 200



################################################################################
################################################################################
###########                                                              #######
###########              Endpoint for mobile app                         #######
################################################################################
################################################################################
key="M001 9/25/2024, 5:37:10 PM"
# recuperer les données de la bdd temporaire
def get_data_from_temp_database():
    item_id="M001"
    timestamp='2024'
    try:
        # Get the item from DynamoDB
        response = temp_table.get_item(Key={'AnimalsId': item_id, 'timestamp':timestamp})

        # Check if 'Item' is in the response (i.e., the item exists)
        if 'Item' in response and response['Item']:
            return jsonify(response['Item'])
        else:
            return jsonify({'error': 'Item not found'}), 404

    except ClientError as e:
        return jsonify({'an error': str(e)}), 500
        
#Traitement de datetime pour separer la date et l'heure
def datetime_processing(dateandtime):
    dateandtime= datetime.strptime(dateandtime, "%m/%d/%Y, %I:%M:%S %p")
    date=str(datetime.date(dateandtime))
    time=str(datetime.time(dateandtime))
    return list((date, time))

# Traitement et labelisation de la temperature corporelle
def temperature_processing(temp):
    #La température corporelle normale d'un mouton et  bovins est entre 38,5°C et 39,5°C; +40 indique un probleme
    _min=38.5
    _max=39.5
    if temp <_min:             # un resultat inferieur a 38.5 indique peut etre de l'hypothermie
        result='Hypothermie'
    elif temp > _max:          # un resultat superieur a 39.5 indique peut etre de la fievre
        result='Fiévreux'
    else:                       # un resultat comprie entre 38.5 et 39.5 indique une temperature normale
        result='Normale'
    final_result={
        'temperature':temp,
        'result':result
    }
    return final_result

# Traitement et labelisation de la temperature corporelle
def location_processing(locat):
    result=locat
    result['security']=True # a suivre
    return result
    
# apperçu pour gyroscope et accelerometer
def activity_level():
    activity={
        'level' : 6,
        'statut' : "good"
    }
    return activity
    
# transformer une ligne de données
def transforme_a_line_of_data(line):
    data={}
    dateandtime=line["timestamp"]
    temperature=line['temperature']
    location= line['location']
    
    data['date']=datetime_processing(dateandtime)[0]
    data['time']=datetime_processing(dateandtime)[1]
    data['temperature']=temperature_processing(temperature)
    data['location']=location_processing(location)
    data['activity']=activity_level()
   
    return  data

# recuperer le fichier des données sous formme de dictionnaire avec juste les données des capteurs

def dict_of_data(response_data):
    final_dict = {}
    
    # on s'assure que la réponse est un JSON 
    if isinstance(response_data, Response):
        response_data = response_data.get_json()
    
    for key, data in response_data["datas"].items():  # Parcourir les données
        if key!="x":
            data=json.loads(data)
            final_dict[key] = transforme_a_line_of_data(data)


    
    #final_dict = json.dumps(final_dict)  # Convertir en JSON string
    return final_dict

        
#def final_json_to_return():
        
@app.route('/getting_data_for_app', methods=['GET'])
def getting_data_for_app():
    response_data = get_data_from_temp_database()
    
    # Vérifie si la réponse est bien JSON et contient les données
    if response_data.status_code == 200:
        data = dict_of_data(response_data.get_json())
        return jsonify(data), 200
    else:
        return response_data


if __name__ == '__main__':
    #app.run(debug=False, use_reloader=False)  # Désactive le mode de redémarrage auto
    app.run(debug=True)