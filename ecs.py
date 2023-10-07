import os
import sys
import yaml
import json
import time
import hashlib
import requests

from dotenv import load_dotenv
from yaml.loader import SafeLoader

load_dotenv()

VERSION = "main"
INDEX_PREFIX = "ecs-"
ES_HOST = os.getenv("ES_HOST")
KIBANA_HOST = os.getenv("KIBANA_HOST")
API_KEY = os.getenv("API_KEY")


def sendToElastic(msg):
    print("Pushing the data to elasticsearch")
    res = requests.post(f"{ES_HOST}/_bulk",
                        headers={
                            "Authorization": "APIKey {APIKey}".format(APIKey=API_KEY),
                            "Content-Type": "application/x-ndjson"},
                        data=msg
                        )
    if res.status_code in [200, 201]:
        print("Data ingested successfully")
    else:
        print("Error while ingesting data: {err}".format(err=res.text))

    f = open(f"logs/response-{int(time.time())}.log", "w")
    f.write(res.text)
    f.close()

    res.close()


def loadECS(mode):
    yml = None
    print("Loading the yaml file")
    if mode.lower() == "url":
        print("Fetching the yaml from URL")
        res = requests.get(
            "https://raw.githubusercontent.com/elastic/ecs/{version}/generated/ecs/ecs_flat.yml".format(version=VERSION))
        if res.status_code != 200:
            exit(0)
        yml = res.text
        res.close()
    elif mode.lower() == "file":
        print("Fetching the yaml from file")
        f = open("./ecs_flat.yml", "r")
        yml = f.read()
        f.close()
    else:
        print("Invalid type")
        exit(0)

    print("Loaded the yaml file")
    return yaml.load(yml, Loader=SafeLoader)


def generateId(msg):
    id = hashlib.sha256(msg.encode())
    return id.hexdigest()


def processData(data):
    print("Started processing the mappings")
    i = 0
    msg = ""
    fields_array = []
    fields_with_allowed_values = []

    for field in data:
        field_split = field.split('.')
        if len(field_split) == 1:
            field_set = "base"
        else:
            field_set = field_split[0]

        del (field_split)

        fields_array.append(
            {"field_name": field, "field_set": field_set, "ecs_version": VERSION})

        j = False
        for props in data[field]:
            if props != "allowed_values":
                fields_array[i][props] = data[field][props]

        try:
            for allowed_value in data[field]["allowed_values"]:
                j = True
                parent = fields_array[i].copy()
                parent["allowed_values"] = allowed_value
                fields_with_allowed_values.append(parent)
        except KeyError:
            j = False
            pass

        if j:
            k = 0
            for entry in fields_with_allowed_values:
                id = f'{entry["field_set"]}-{entry["field_name"]}-{entry["allowed_values"]["name"]}'
                msg += f'{{"index": {{"_index": "{INDEX_PREFIX}{VERSION}",  "_id": "{generateId(id)}"}}}}\n{json.dumps(entry)}\n'
                k += 1

        msg += f'{{"index": {{"_index": "{INDEX_PREFIX}{VERSION}",  "_id": "{generateId(field)}"}}}}\n{json.dumps(fields_array[i])}\n'

        i += 1

    print("Done processing the mappings")
    return msg


def pushAssets():
    # ============== Push Index Template ====================
    print("Pushing assets to elasticsearch")
    template_available = True
    if requests.get(f"{ES_HOST}/_index_template/ecs-template", headers={
        "Authorization": "APIKey {APIKey}".format(APIKey=API_KEY)
    }).status_code != 200:
        template_available = False
        d = open("./assets/index_template.json", "r")
        res = requests.post(f"{ES_HOST}/_index_template/ecs-template",
                            headers={
                                "Authorization": "APIKey {APIKey}".format(APIKey=API_KEY),
                                "Content-Type": f"application/json"},
                            data=d.read())

        if res.status_code in [200, 201]:
            print("Assets pushed successfully")
        else:
            print("Error while ingesting data: {err}".format(err=res.text))

        res.close()
    if template_available:
        print("Template is already available, Skipping the push.")


def main():
    global VERSION
    if len(sys.argv) != 0:
        VERSION = sys.argv[1]

    if VERSION != "main":
        VERSION = "v"+VERSION

    if not os.path.exists("logs"):
        os.makedirs("logs")

    data = loadECS("url")
    msg = processData(data)
    sendToElastic(msg)
    pushAssets()


main()
