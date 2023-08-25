import json
import getpass
import argparse
import requests
import pystac_client
from pathlib import Path
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin

def change_to_https(request: requests.Request) -> requests.Request: 
    request.url = request.url.replace("http:", "https:")
    return request

def json_convert(jsonfile):

    """
        jsonfile: json file in dict format
        
        A function to map the Sentinel-2 STAC jsonfiles into the GeoServer database layout.
        There are different json layouts for Collections and Items. The function checks if the jsonfile is of type "Collection",
        or of type "Feature" (=Item). A number of properties are hardcoded into Sentinel-2 metadata as these are not collected in the STAC jsonfiles.
    """

    with open(jsonfile) as f:
        content = json.load(f)
    
    if content["type"] == "Collection":

        new_json = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][3]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][0],
                            content["extent"]["spatial"]["bbox"][0][3]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][0],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ]

                    ]
                ]
            },
            "properties": {
                "name": content["id"],
                "title": content["title"],
                "eo:identifier": content["id"],
                "description": content["description"],
                "timeStart": content["extent"]["temporal"]["interval"][0][0],
                "timeEnd": content["extent"]["temporal"]["interval"][0][1],
                "primary": True,
                "license": content["license"],
                "providers": content["providers"], # Providers added
                "derivedFrom": None,
                "licenseLink": None,
                "summaries": content["summaries"],
                "queryables": [
                    "eo:identifier"
                ]
            }
        }

        if "derived_from" in content:
            new_json["properties"]["derivedFrom"] = {
                    "href": content["derived_from"],
                    "rel": "derived_from",
                    "type": "application/json"
            }

        if "assets" in content:
            new_json["properties"]["assets"] = content["assets"]

        for link in content["links"]:
            if link["rel"] == "license":
                new_json["properties"]["licenseLink"] = {
                    "href": link["href"],
                    "rel": "license",
                    "type": "application/json"
                } # New License URL link

    if content["type"] == "Feature":

        new_json = {
            "type": "Feature",
            "geometry": content["geometry"],
            "properties": {
                "eop:identifier": content["id"],
                "eop:parentIdentifier": content["collection"],
                "timeStart": content["properties"]["start_datetime"],
                "timeEnd": content["properties"]["end_datetime"],
                "eop:resolution": content["gsd"],
                # "opt:cloudCover": int(content["properties"]["eo:cloud_cover"]),
                "crs": content["properties"]["proj:epsg"],
                "projTransform": content["properties"]["proj:transform"],
                # "thumbnailURL": content["links"]["thumbnail"]["href"],
                "assets": content["assets"]
            }
        }

    return json.loads(json.dumps(new_json))

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)

    args = parser.parse_args()

    pwd = getpass.getpass()

    # The uploaded collection is specific below, this could be done with an argument in the future
    collection_name = "wind_velocity_at_geocubes"

    workingdir = Path(__file__).parent
    sentinel = workingdir / "GeoCubes" / collection_name

    app_host = f"{args.host}/geoserver/rest/oseo/"
    catalog = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/")#, request_modifier=change_to_https)

    # Convert the STAC collection json into json that GeoServer can handle
    converted = json_convert(sentinel / "collection.json")

    #Additional code for changing collection data if the collection already exists
    collections = catalog.get_collections()
    col_ids = [col.id for col in collections]
    if collection_name in col_ids:
        r = requests.put(urljoin(app_host + "collections/", collection_name), json=converted, auth=HTTPBasicAuth("admin", pwd))
        r.raise_for_status()
        print(f"Updated {collection_name}")
    else:
        r = requests.post(urljoin(app_host, "collections/"), json=converted, auth=HTTPBasicAuth("admin", pwd))
        r.raise_for_status()
        print(f"Added new collection")

    # Get the posted items from the specific collection
    posted = catalog.search(collections=[collection_name]).item_collection()
    posted_ids = [x.id for x in posted]
    print(f"POSTed: {len(posted_ids)}")

    with open(sentinel / "collection.json") as f:
        rootcollection = json.load(f)

    items = [x['href'] for x in rootcollection["links"] if x["rel"] == "item"]

    print("POSTing items:")
    for i, item in enumerate(items):
        with open(sentinel / item) as f:
            payload = json.load(f)
        # Convert the STAC item json into json that GeoServer can handle
        converted = json_convert(sentinel / item)
        request_point = f"collections/{rootcollection['id']}/products"
        if payload["id"] in posted_ids:
            request_point = f"collections/{rootcollection['id']}/products/{payload['id']}"
            r = requests.put(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", pwd))
            r.raise_for_status()
        else:
            r = requests.post(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", pwd))
            r.raise_for_status()
        if len(items) >= 5: # Just to keep track that the script is still running
            if i == int(len(items) / 5):
                print("~20% of items added")
            elif i == int(len(items) / 5) * 2:
                print("~40% of items added")
            elif i == int(len(items) / 5) * 3:
                print("~60% of items added")
            elif i == int(len(items) / 5) * 4:
                print("~80% of items added")
    print("")