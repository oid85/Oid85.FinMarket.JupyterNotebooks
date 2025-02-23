import json
import os


host     = "26.147.25.39"
port     = 5432
database = "finmarket_prod"
user     = "postgres"
password = "postgres"

start_date = '2022-01-01'
end_date = '2025-12-31'


def get_watch_list_shares():
    file_name = 'c:\\Deploy\\Oid85.FinMarket.ResourceStore\\watchLists\\shares.json'

    if os.path.isfile(file_name):
        with open(file_name) as json_file:
            data = json.load(json_file)

    return data


def get_watch_list_indexes():
    file_name = 'c:\\Deploy\\Oid85.FinMarket.ResourceStore\\watchLists\\indexes.json'

    if os.path.isfile(file_name):
        with open(file_name) as json_file:
            data = json.load(json_file)

    return data