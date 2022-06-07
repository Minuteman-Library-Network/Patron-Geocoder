'''
Jeremy Goldstein
Minuteman Library Network
jgoldstein@minlib.net

Script will gather 10,000 patron records via a sql query
create a temporary csv of patron ids and addresses to pass through the census bureau's geocoding service
Then loop through the resulting file to update the census varfield in each patron record with the geoid returned from the geocoder
Field will be segmented into subfields for the state, country, tract, and block values subvalues of the geo id as well as today's date as a last updated marker

Data is then used by the patron map scripts to create choropleth maps of patron demographics and annonymized activity
'''

import requests
import json
import os
import configparser
import psycopg2
import pandas as pd
import csv
import censusgeocode
from datetime import datetime
from datetime import timedelta
from datetime import date
from base64 import b64encode

def get_token():
    # config api    
    config = configparser.ConfigParser()
    config.read('api_info.ini')
    base_url = config['api']['base_url']
    client_key = config['api']['client_key']
    client_secret = config['api']['client_secret']
    auth_string = b64encode((client_key + ':' + client_secret).encode('ascii')).decode('utf-8')
    header = {}
    header["authorization"] = 'Basic ' + auth_string
    header["Content-Type"] = 'application/x-www-form-urlencoded'
    body = {"grant_type": "client_credentials"}
    url = base_url + '/token'
    response = requests.post(url, data=json.dumps(body), headers=header)
    json_response = json.loads(response.text)
    token = json_response["access_token"]
    return token

def mod_patron(patronid,state,county,tract,block,token):
#function will use the patron api to update the k tagged varfield (census in Minuteman) of the given patron record
#with the current geoid of that patron's address that was returned by the census geocoder
	
    config = configparser.ConfigParser()
    config.read('api_info.ini')
    url = config['api']['base_url'] + "/patrons/" + patronid
    header = {"Authorization": "Bearer " + token, "Content-Type": "application/json;charset=UTF-8"}
    payload = {"varFields": [{"fieldTag": "k", "content": "|s" + state + "|c" + county + "|t" + tract + "|b" + block + "|d" + format(date.today())}]}
    request = requests.put(url, data=json.dumps(payload), headers = header)
   
def runquery():

    # import configuration file containing our connection string
    # app.ini looks like the following
    #[db]
    #connection_string = dbname='iii' user='PUT_USERNAME_HERE' host='sierra-db.library-name.org' password='PUT_PASSWORD_HERE' port=1032

    config = configparser.ConfigParser()
    config.read('api_info.ini')
    
    conn = psycopg2.connect("dbname='iii' user='" + config['api']['sql_user'] + "' host='" + config['api']['sql_host'] + "' port='1032' password='" + config['api']['sql_pass'] + "' sslmode='require'")

    #Opening a session and querying the database for weekly new items
    cursor = conn.cursor()
    
    cursor.execute(open("Geocode.sql","r").read())
    #For now, just storing the data in a variable. We'll use it later.
    rows = cursor.fetchall()
    conn.close()
    
    csvFile =  'patrons_to_geocode{}.csv'.format(date.today())
    
    with open(csvFile,'w', encoding='utf-8', newline='') as tempFile:
        myFile = csv.writer(tempFile, delimiter=',')
        myFile.writerows(rows)
    tempFile.close()
    
    return csvFile


def geocode(csvFile):
    start_time = datetime.now()
    #Be sure to adjust vintage as new data is made available to stay current
    cg = censusgeocode.CensusGeocode(benchmark='Public_AR_Current',vintage='Census2020_Current')
    
    print("sending batch file to process")
    result = cg.addressbatch(csvFile)
    
    print("---response from geocode %s seconds ---" % (datetime.now() - start_time))
	
    print("building data frame")
    df = pd.DataFrame(result, columns=result[0].keys())
    print("outputting to csv")
    df.to_csv('output.csv', mode='a', header=True)

    
def main():
    start_time = datetime.now()
    csvFile = runquery()
    print("---csv generated %s seconds ---" % (datetime.now() - start_time))
    print("calling geocode function")
    geocode(csvFile)
    print("writing to Sierra patrons")
    with open('output.csv', newline='') as csvFile2:
        reader = csv.DictReader(csvFile2)
        expiration_time = datetime.now() + timedelta(seconds=3600)
        token = get_token()
        for row in reader:
            patronid = row['id']
            state = row['statefp']
            county = row['countyfp']
            tract = row['tract']
            block = row['block']
            #print out current patronid to illustrate the current script progress...this will take a while so useful to show the script is still running
            print(patronid)
            #checks if API token has expired and gets a new one if necessary
            if datetime.now() < expiration_time:
                mod_patron(patronid,state,county,tract,block,token)
            else:
                print("refreshing token")
                expiration_time = datetime.now() + timedelta(seconds=3600)
                token = get_token()
                mod_patron(patronid,state,county,tract,block,token)

    print("---time to complete %s seconds ---" % (datetime.now() - start_time))    
    
    os.remove(csvFile)
    output_file = 'output.csv'
    os.remove(output_file)


main()
