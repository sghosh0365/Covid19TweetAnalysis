import requests
import json
import datetime
from TwitterLoadMongoDB import pushDatatoMongo

try:
    resp = requests.get('https://api.covid19api.com/summary')
    resp_json = resp.json()
    filename = 'C:\\Users\\TSG\\PycharmProjects\\TwitterCovid-19\\DataSets\\Covid-19CountrywiseCounts.json' # Change to your working directory
    with open(filename, 'w') as f:
        f.write(json.dumps(resp_json, indent=4))
        collection = "Covid19Figures"
        date_since = datetime.datetime.now().strftime("%Y-%m-%d")
        pushDatatoMongo(filename, collection, 2, date_since)
except Exception as ex:
    print(f'Error occurred while trying to fetch country wise corona patient counts {ex}')


