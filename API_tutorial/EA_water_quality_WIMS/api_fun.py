import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import pandas as pd
import numpy as np

def get_api_response(url, csv=False):
    """ Helper function to send request to API and get the response 
    
    :param str url: The URL of the API request
    :param bool csv: Whether this is a CSV request. Default False. 
    :return: API response
    """ 
    # Send request and read response
    print(url)
    response = requests.get(url)
    print('API status code: ', response.status_code)
    # print('Content type: ', response.headers.get("content-type", ""))

    if csv:
        return response
    elif response.headers.get("content-type", "") == 'text/html; charset=utf-8':
        return response
    else:
        # Decode from JSON to Python dictionary
        return json.loads(response.content)

def get_ea_api_allAreas(year, determinand = "", endpoint = '/data/measurement', limit_n = 500):
    """ Function to get data from all areas for a particular determinand and year
    
    :param str endpoint: The particular EA endpoint to quary, e.g. '/id/sampling-point' Default is '/data/measurement'.
    :param int limit_n: limit on the number of records to return initially. Default 500. 
    :param str determinand: the measured property. To be given in id form, 'determinand=0172'. List of determinands can be retrieved from EA API
    :param int year: Year measurements took place, 'year=2021'
    :return: API response
    """ 
    # initialise variables
    base = 'http://environment.data.gov.uk/water-quality'
    dataList = []
    retry = Retry(connect=5, backoff_factor=0.8)
    adapter = HTTPAdapter(max_retries=retry)
    limit = f'_limit={limit_n}'
        
    # Get list of sub areas 
    subAreas_url = f'{base}/id/ea-subarea'
    areas = get_api_response(subAreas_url)
    subAreas_list = []
    for a in areas['items']:
        queryString = 'subArea=' + a['notation']
        subAreas_list.append(queryString
        )
        
    # change base url depending on endpoint
    if endpoint == '/data/measurement':
        base_url = f'{base}{endpoint}?{determinand}&{year}&{limit}'
    elif endpoint == '/data/sample':
        base_url = f'{base}{endpoint}?{year}&{limit}'
           
    
    for sa in subAreas_list:
        total_length = 0
        has_more_data = True
        limit = f'_limit={limit_n}'
        
        session = requests.Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
                
        while has_more_data:
            #url = f'{base}{endpoint}?{determinand}&{year}&{sa}&{limit}'
            url = f'{base_url}&{sa}'
            r = session.get(url)
            print(url)

            if r.status_code == 200:
                data = r.json()

                # Check if there are more items available in the list
                has_more_data = len(data['items']) == limit_n
                print('Number of items retreived: ', len(data['items']))
                
                if has_more_data == True:
                    # Increase the limit for the next API call (e.g., double the limit)
                    limit_n *= 2
                    limit = f'_limit={limit_n}'
                    
            else:
                print("Error: Unable to fetch data. Status Code:", r.status_code)
                break
            
        for item in data['items']:
            dataList.append(item)

    try:
        print("Total length of the list:", len(dataList))
    except:
        print("Total length of the list: Unknown!")
        
    return dataList


def get_ea_api_allAreas_yearsRange(start_year, end_year, determinand = "", endpoint = '/data/measurement', limit_n = 500):
    """ Function to get data from all areas and a range of years for a particular determinand
    
    :param str endpoint: The particular EA endpoint to quary, e.g. '/id/sampling-point' Default '/data/measurement'.
    :param int limit_n: limit on the number of records to return initially. Default 500. 
    :param str determinand: the measured property. To be given in id form, 'determinand=0172'. List of determinands can be retrieved from EA API
    :param int start_year: First year measurements took place, '2021'
    :param int end_year: Last year measurements took place, '2023'
    :return: API response
    """ 
    # initialise variables
    base = 'http://environment.data.gov.uk/water-quality'
    data_list_year = []
    data_list = []
    retry = Retry(connect=5, backoff_factor=0.8)
    adapter = HTTPAdapter(max_retries=retry)
    limit = f'_limit={limit_n}'
    

        
    # Get list of sub areas 
    subAreas_url = f'{base}/id/ea-subarea'
    areas = get_api_response(subAreas_url)
    subAreas_list = []
    for a in areas['items']:
        queryString = 'subArea=' + a['notation']
        subAreas_list.append(queryString
        )
   
    # loop through range of years
    years_range = np.arange(start_year, end_year+1)
    for current_year in years_range:
        year = f'year={current_year}'
        
        # change base url depending on endpoint and year
        if endpoint == '/data/measurement':
            base_url = f'{base}{endpoint}?{determinand}&{year}&{limit}'
        elif endpoint == '/data/sample':
            base_url = f'{base}{endpoint}?{year}&{limit}'  

        for sa in subAreas_list:
            total_length = 0
            has_more_data = True
            limit = f'_limit={limit_n}'

            session = requests.Session()
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            while has_more_data:
                url = f'{base_url}&{sa}'
                r = session.get(url)
                print(url)

                if r.status_code == 200:
                    data = r.json()
                    # Check if there are more items available in the list
                    has_more_data = len(data['items']) == limit_n
                    print('Number of items retreived: ', len(data['items']))

                    if has_more_data == True:
                        # Increase the limit for the next API call (e.g., double the limit)
                        limit_n *= 2
                        limit = f'_limit={limit_n}'

                else:
                    print("Error: Unable to fetch data. Status Code:", r.status_code)
                    break

            for item in data['items']:
                data_list_year.append(item)
                
        # append the year of data to main list
        data_list.append(data_list_year)
        # clear data_list_year for next loop
        data_list_year = []
        
    data_list = [item for sublist in data_list for item in sublist]

    
    try:
        print("Total length of the list:", len(data_list))
    except:
        print("Total length of the list: Unknown!")
        
    return data_list


def convert_ea_dict_to_df(data, parent_key='', sep='_'):
    flattened_data = []
    
    def flatten_dict(d, parent_key='', sep='_'):
        items = {}
        for key, value in d.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, dict):
                items.update(flatten_dict(value, new_key, sep))
            else:
                items[new_key] = value
        return items
    
    for item in data:
        flattened_data.append(flatten_dict(item))
        
    df = pd.DataFrame(flattened_data)
    return df
