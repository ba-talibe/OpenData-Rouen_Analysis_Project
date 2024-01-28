"""
This file contain de set of functions that allow 
you to fetch data from api using asynchronous programming
for better perfomances
"""

datacount_base_url = f"https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records"
counter_location_base_url = f"https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-sites/records"

import os
import time
import json
import asyncio
import aiohttp
import requests
import numpy as np
import pandas as pd
from urllib.parse import urlencode, urlunparse, urlparse, parse_qs


from environs import Env




env = Env()
env.read_env()

def url_builder(base_url, **kwargs):
    # Parse the base URL
    scheme, netloc, path, params, query, fragment = urlparse(base_url)

    # Parse existing query parameters
    existing_params = parse_qs(query)

    # Update existing parameters with new ones
    existing_params.update(kwargs)

    # Encode the parameters and construct the new query string
    new_query = urlencode(existing_params, doseq=True)

    # Build the final URL
    final_url = urlunparse((scheme, netloc, path, params, new_query, fragment))

    return final_url

def get_data_as_dataframe(
        limit : int = 50, 
        offset : int = 0, 
        select : str = "", 
        where  : str  = "",
        group_by : str = "",
        order_by : str = "",
        refine : dict = {},
        exclude : dict = {}
        )  -> pd.DataFrame:
    

    # query = f"?limit={limit}" \
    #         f"&offset={offset}" \
    query = ""
    if select != "":
        query += f"{'?' if len(query) == 0 else '&'}select={select}" 
    
    if where != "":
        query += f"{'?' if len(query) == 0 else '&'}where={where}" 

    if group_by != "":
        query += f"{'?' if len(query) == 0 else '&'}group_by={group_by}" 

    if order_by != "":
        query += f"{'?' if len(query) == 0 else '&'}order_by={order_by}" 

    if refine != {}:
        for key in refine:
            query += f"{'?' if len(query) == 0 else '&'}refine={key}%3A{refine[key]}"

    if exclude != {}:
        for key in exclude:
            query += f"{'?' if len(query) == 0 else '&'}exclude={key}%3A{exclude[key]}"
        
    if 0 < limit <= 100:
        query += f"{'?' if len(query) == 0 else '&'}limit={limit}" \
            f"&offset={offset}" 
        response = requests.get(datacount_base_url + query).json()
        total_count = response["total_count"] 
        data = response["results"]

    else:
        if limit == 0:
            response = requests.get(datacount_base_url + query+ f"{'?' if len(query) == 0 else '&'}limit=1").json()
            limit = int(response["total_count"])
        # le nombre de requete de limité à 100 entrés possible
        nbre_request = limit // 100
        # le nombre d'entré de la derniere requete
        last_limit = limit  % 100
        
        data = []
        start = time.perf_counter()
        for idx_req in range(nbre_request):
            limit_query = f"{'?' if len(query) == 0 else '&'}limit={100}" \
                    f"&offset={offset + idx_req*100}"
            response = requests.get(datacount_base_url + query + limit_query).json() 
            if "message" in response.keys():
                print(response)
                break
            data += response["results"]
        
        #envoie de la dernier requete
        limit_query = f"{'?' if len(query) == 0 else '&'}limit={last_limit}" \
                f"&offset={offset + (nbre_request)*100}"
        response = requests.get(datacount_base_url + query + limit_query).json() 
        data += response["results"]
        print("time", time.perf_counter() - start)
        total_count = response["total_count"] 
    return total_count, pd.DataFrame(data )


def update_dataset(path):
    # L'URL de l'API pour donné de compteur 
    limit_datacount = 100 # télécharger 100 données mais ne pas saugarder localement 
    offset_datacount = 0
    dateEtHeure ='2023-12-24T23'
    #api avec un filtre "order by" dans l'ordre décroissant de dates dans datacount
    api_url_datacount = "https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records?order_by=date%20DESC&limit=" + str(limit_datacount) +"&offset=" + str(offset_datacount)

    #L'URL de l'API pour localisation des sites de comptage
    api_url_localisation = "https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-sites/records?limit=29"

    #L'URL de l'API pour télécharger toutes les données 
    api_url_export = "https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/exports/json?lang=fr&timezone=Europe%2FBerlin"

    json_file_path = os.path.join(path, 'local_data.json')
    csv_file_path = os.path.join(path, 'local_data.csv')

 

    # #local_data telecharge depuis serveur
    local_data = pd.read_json(json_file_path)

    update_data = [] # initialise update_data par une liste

    dateEtHeure = local_data.iloc[0]['date'][:-12] # pour avoir la forme de variable dateEtHeure identique que celle declarée dessus

    #L'URL de l'API pour la mise à jour de données (where date > dateEtHeure le plus récent indiqué dans local_data, order by date descendant, timezone Europe/Berlin)
    api_url_update_data ="https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records?where=date%3Edate'"+dateEtHeure+"%3A00%3A00%2B01%3A00'&order_by=date%20DESC&limit="+str(limit_datacount)+"&offset="+str(offset_datacount)+"&timezone=Europe%2FBerlin"


    # Effectuer une requête GET pour obtenir les données
    response_updateData = requests.get(api_url_update_data) # dictionnaire (indice total_count, indice results)
    response_localisation = requests.get(api_url_localisation)

    if (response_updateData.status_code != 200):
    
        print(f'Échec de la requête HTTP, code de statut {response_updateData.status_code}')

    if (response_localisation.status_code != 200):
    
        print(f'Échec de la requête HTTP, code de statut {response_localisation.status_code}')



    # Si la requête a réussi (code de statut HTTP 200), vous pouvez accéder aux données
    data_localisation = response_localisation.json()

    total_count = response_updateData.json()['total_count']

    print(total_count)

    if (total_count != 0) : # le nombre de données mises à jour 
        while (total_count >= 100) : # data limit is 100
            update_data.extend(requests.get(api_url_update_data).json()['results'])
            total_count -= 100
            offset_datacount +=  100
            api_url_update_data ="https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records?where=date%3Edate'"+dateEtHeure+"%3A00%3A00%2B01%3A00'&order_by=date%20DESC&limit="+str(limit_datacount)+"&offset="+str(offset_datacount)+"&timezone=Europe%2FBerlin"

        limit_datacount = total_count
        api_url_update_data ="https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records?where=date%3Edate'"+dateEtHeure+"%3A00%3A00%2B01%3A00'&order_by=date%20DESC&limit="+str(limit_datacount)+"&offset="+str(offset_datacount)+"&timezone=Europe%2FBerlin"
        update_data.extend(requests.get(api_url_update_data).json()['results'])
        list_local_data =local_data.to_dict(orient='records')
        update_data.extend(list_local_data)
        # Utilisez json.dump() pour sauvegarder le dictionnaire dans le fichier JSON
        df=pd.DataFrame(update_data)
        with open(json_file_path, 'w') as fichier_json:
            json.dump(update_data, fichier_json)

        df.to_csv(csv_file_path, index=False)

def update_csv_dataset(path):
    # L'URL de l'API pour donné de compteur 
    limit_datacount = 100 # télécharger 100 données mais ne pas saugarder localement 
    offset_datacount = 0
    dateEtHeure ='2023-12-24T23'
    #api avec un filtre "order by" dans l'ordre décroissant de dates dans datacount
    api_url_datacount = "https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records?order_by=date%20DESC&limit=" + str(limit_datacount) +"&offset=" + str(offset_datacount)

    #L'URL de l'API pour localisation des sites de comptage
    api_url_localisation = "https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-sites/records?limit=29"

    #L'URL de l'API pour télécharger toutes les données 
    api_url_export = "https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/exports/json?lang=fr&timezone=Europe%2FBerlin"

    json_file_path = os.path.join(path, 'local_data.json')
    csv_file_path = os.path.join(path, 'local_data.csv')

 

    # #local_data telecharge depuis serveur
    local_data = pd.read_csv(csv_file_path)

    update_data = [] # initialise update_data par une liste

    dateEtHeure = local_data.iloc[0]['date'][:-12] # pour avoir la forme de variable dateEtHeure identique que celle declarée dessus

    #L'URL de l'API pour la mise à jour de données (where date > dateEtHeure le plus récent indiqué dans local_data, order by date descendant, timezone Europe/Berlin)
    api_url_update_data ="https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records?where=date%3Edate'"+dateEtHeure+"%3A00%3A00%2B01%3A00'&order_by=date%20DESC&limit="+str(limit_datacount)+"&offset="+str(offset_datacount)+"&timezone=Europe%2FBerlin"


    # Effectuer une requête GET pour obtenir les données
    response_updateData = requests.get(api_url_update_data) # dictionnaire (indice total_count, indice results)
    response_localisation = requests.get(api_url_localisation)

    if (response_updateData.status_code != 200):
    
        print(f'Échec de la requête HTTP, code de statut {response_updateData.status_code}')

    if (response_localisation.status_code != 200):
    
        print(f'Échec de la requête HTTP, code de statut {response_localisation.status_code}')



    # Si la requête a réussi (code de statut HTTP 200), vous pouvez accéder aux données
    data_localisation = response_localisation.json()

    total_count = response_updateData.json()['total_count']

    print(total_count)

    if (total_count != 0) : # le nombre de données mises à jour 
        while (total_count >= 100) : # data limit is 100
            update_data.extend(requests.get(api_url_update_data).json()['results'])
            total_count -= 100
            offset_datacount +=  100
            api_url_update_data ="https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records?where=date%3Edate'"+dateEtHeure+"%3A00%3A00%2B01%3A00'&order_by=date%20DESC&limit="+str(limit_datacount)+"&offset="+str(offset_datacount)+"&timezone=Europe%2FBerlin"

        limit_datacount = total_count
        api_url_update_data ="https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records?where=date%3Edate'"+dateEtHeure+"%3A00%3A00%2B01%3A00'&order_by=date%20DESC&limit="+str(limit_datacount)+"&offset="+str(offset_datacount)+"&timezone=Europe%2FBerlin"
        update_data.extend(requests.get(api_url_update_data).json()['results'])
        list_local_data =local_data.to_dict(orient='records')
        update_data.extend(list_local_data)
        # Utilisez json.dump() pour sauvegarder le dictionnaire dans le fichier JSON
        df=pd.DataFrame(update_data)
        df.to_csv(csv_file_path, index=False)
        
        
cwd = os.getcwd()

def load_dataset(path, parse_date=False, index_col="id"):

    update_dataset(path)
    csv_file_path = os.path.join(path, "local_data.csv")

    return pd.read_csv(csv_file_path, parse_dates=True, index_col="date")



async def main():
    _, df =  get_data_as_dataframe(9000)

if __name__ == '__main__':
    #load_dataset(os.getcwd() + "/data_toolset")
    print("Done")