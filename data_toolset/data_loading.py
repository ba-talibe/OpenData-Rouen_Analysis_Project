"""
This file contain de set of functions that allow 
you to fetch data from api using asynchronous programming
for better perfomances
"""

datacount_base_url = f"https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-data/records"
counter_location_base_url = f"https://data.metropole-rouen-normandie.fr/api/explore/v2.1/catalog/datasets/eco-counter-sites/records"


import asyncio
import requests
import pandas as pd
import aiohttp
import time
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
async def main():
    _, df =  get_data_as_dataframe(9000)

if __name__ == '__main__':
    asyncio.run(main())