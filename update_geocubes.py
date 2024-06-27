import pystac
import rasterio
import requests
import datetime
import pandas as pd
import re
import time
import getpass
import argparse
import pystac_client
from bs4 import BeautifulSoup
from rio_stac.stac import create_stac_item
from urllib.parse import urljoin

def change_to_https(request: requests.Request) -> requests.Request: 
    request.url = request.url.replace("http:", "https:")
    # This is to help filtering logging, not needed otherwise
    request.headers["User-Agent"] = "update-script"
    return request

def get_datasets():
    """
        Datasets can be obtained from an API endpoint.
        Returns a dictionary containing the GeoCubes datasets and their relevant information.
    """

    data = requests.get("https://vm0160.kaj.pouta.csc.fi/geocubes/info/getDatasets")
    const_url = "https://vm0160.kaj.pouta.csc.fi"
    raw_datasets = data.text.split(";")
    split_datasets = [x.split(",") for x in raw_datasets]

    dataset_dict = {}
    for split in split_datasets:
        dataset_dict[split[0]] = dict(zip(["name", "layername", "years", "folder", "file_prefix", "max_resolution", "bit_depth", "producer", "metadata_URL"], split))

    for d in dataset_dict:
        year_split = dataset_dict[d]['years'].split(".")
        dataset_dict[d]['paths'] = []
        if len(year_split) == 1:
            dataset_dict[d]['paths'].append(f"{const_url}{dataset_dict[d]['folder']}{year_split[0]}/")
        else:
            for year in year_split:
                dataset_dict[d]['paths'].append(f"{const_url}{dataset_dict[d]['folder']}{year}/")
    
    return dataset_dict

def json_convert(content):

    """ 
    A function to map the STAC dictionaries into the GeoServer database layout.
    There are different json layouts for Collections and Items. The function checks if the dictionary is of type "Collection",
    or of type "Feature" (=Item).

    content - STAC dictionary from where the modified JSON will be made
    """
    
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
                "licenseLink": None,
                "summaries": content["summaries"],
                "queryables": [
                    "eo:identifier"
                ]
            }
        }

        if "assets" in content:
            new_json["properties"]["assets"] = content["assets"]

        for link in content["links"]:
            if link["rel"] == "license":
                new_json["properties"]["licenseLink"] = { #New License URL link
                    "href": link["href"],
                    "rel": "license",
                    "type": "application/json"
                }
            elif link["rel"] == "derived_from":
                derived_href = link["href"]
                new_json["properties"]["derivedFrom"] = {
                    "href": derived_href,
                    "rel": "derived_from",
                    "type": "application/json"
                }

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

        if content["properties"]["start_datetime"] is None and content["properties"]["end_datetime"] is None and content["properties"]["datetime"] is not None:
            new_json["properties"]["timeStart"] = content["properties"]["datetime"]
            new_json["properties"]["timeEnd"] = content["properties"]["datetime"]

    return new_json

def update_catalog(app_host, csc_catalog_client):

    """
    The main updating function of the script. Checks the collection items in the Geocubes and compares the to the ones in CSC catalog.

    app_host - The REST API path for updating the collections
    csc_catalog_client - The STAC API path for checking which items are already in the collections
    """
    title_regex_pattern = r" \(GeoCubes\)"
    session = requests.Session()
    session.auth = ("admin", pwd)
    log_headers = {"User-Agent": "update-script"} # Added for easy log-filtering

    # Get all Geocubes collections from the app_host
    csc_collections = [col for col in csc_catalog_client.get_collections() if col.id.endswith("at_geocubes")]

    csc_title_id_map = {c.title: c.id for c in csc_collections}
    collection_csv = pd.read_csv('karttatasot.csv', index_col='Nimi').to_dict('index')

    # Get the titles and IDs from CSC STAC and make the title correspond them to the ones in the CSV
    titles_and_ids = {}
    for title in csc_title_id_map:
        fixed_title = re.sub(title_regex_pattern, '', title)
        titles_and_ids[fixed_title] = csc_title_id_map[title]

    geocubes_datasets = get_datasets()
    for dataset in geocubes_datasets:
        try: # If there's more datasets in GeoCubes than in CSC STAC, skip them in this update script
            translated_name = collection_csv[dataset]["Name"]
        except KeyError:
            continue

        collection_id = titles_and_ids[translated_name]
        csc_collection = csc_catalog_client.get_child(collection_id)
        csc_collection_item_ids = [item.id for item in csc_collection.get_items()]

        paths = geocubes_datasets[dataset]['paths']
        print(f"Checking new items for {csc_collection.id}: ", end="")

        number_of_items_in_geocubes = 0
        number_of_items_added = 0
        for year_path in paths:

            #TIFs through BeautifulSoup
            page = requests.get(year_path)
            data = page.text
            soup = BeautifulSoup(data, features="html.parser")

            links = [link for link in soup.find_all("a")]

            item_links = [link.get("href") for link in links if link.get("href").endswith("tif")]
            item_sets = [item.split(".")[0] for item in item_links]
                
            grouped_dict = {}
            for item in item_sets:
                prefix = "_".join(item.split("_")[:4])
                if prefix not in grouped_dict:
                    grouped_dict[prefix] = []
                    grouped_dict[prefix].append(item)
            
            number_of_items_in_geocubes = number_of_items_in_geocubes + len(grouped_dict.keys())
            for key in grouped_dict.keys():
                
                # Takes the year from the path
                item_starttime = datetime.datetime.strptime(f"{year_path.split('/')[-2]}-01-01", "%Y-%m-%d")
                item_endtime = datetime.datetime.strptime(f"{year_path.split('/')[-2]}-12-31", "%Y-%m-%d")
                    
                # The sentinel and NDVI items are named a bit differently from the rest
                item_year = year_path.split("/")[-1]
                if "sentinel" in key:
                    name = key.split("_")[0].replace('-', '_')
                    item_info = "_".join(key.split(".")[0].split("_")[1:])
                    item_id = f"{name.lower().replace(' ', '_').replace(',', '')}_{item_info}"
                elif "ndvi" in key:
                    name = key.split("_")[0]
                    item_info = "_".join(key.split(".")[0].split("_")[1:])
                    item_id = f"{name.lower()}_{item_info}"
                else:
                    item_info = "_".join(key.split(".")[0].split("_")[1:])
                    item_id = f"{translated_name.lower().replace(' ', '_').replace(',', '')}_{item_info}"

                if item_id in csc_collection_item_ids:
                    continue
                else:
                    number_of_items_added = number_of_items_added + 1
                    with rasterio.open(year_path+grouped_dict[key][0]+".tif") as src:
                        assets = {
                            "COG": pystac.Asset(
                                href=year_path+grouped_dict[key][0]+".tif", 
                                media_type="image/tiff; application=geotiff; profile=cloud-optimized", 
                                title="COG",
                                roles=["data"],
                                extra_fields={
                                    "gsd": int(src.res[0]),
                                    "proj:shape": src.shape,
                                    "proj:transform": [
                                        src.transform.a,
                                        src.transform.b,
                                        src.transform.c,
                                        src.transform.d,
                                        src.transform.e,
                                        src.transform.f,
                                        src.transform.g,
                                        src.transform.h,
                                        src.transform.i
                                    ]
                                }
                            )
                        }
                    min_gsd = assets["COG"].extra_fields["gsd"]
                    for asset in grouped_dict[key][1:]:
                        with rasterio.open(year_path+asset+".tif") as src:
                            asset_id = asset.split("_")[-1]
                            assets[asset_id] = pystac.Asset(
                                href=year_path+asset+".tif",
                                media_type="image/tiff; application=geotiff", 
                                title=asset.split('_')[-1],
                                roles=["data"],
                                extra_fields={
                                    "gsd": int(src.res[0]),
                                    "proj:shape": src.shape,
                                    "proj:transform": [
                                        src.transform.a,
                                        src.transform.b,
                                        src.transform.c,
                                        src.transform.d,
                                        src.transform.e,
                                        src.transform.f,
                                        src.transform.g,
                                        src.transform.h,
                                        src.transform.i
                                    ]
                                }
                            )
                        
                        # Add the GSD into the Collection Summaries if not in it
                        if assets[asset_id].extra_fields["gsd"] not in csc_collection.summaries.lists["gsd"]:
                            csc_collection.summaries.lists["gsd"].append(assets[asset_id].extra_fields["gsd"])
                        min_gsd = min(min_gsd, assets[asset_id].extra_fields["gsd"])

                    item = create_stac_item(
                        source=year_path+key+".tif",
                        id=item_id,
                        assets=assets, 
                        asset_media_type=pystac.MediaType.TIFF, 
                        with_proj=True,
                    )
                    item.common_metadata.start_datetime = item_starttime
                    item.common_metadata.end_datetime = item_endtime
                    item.extra_fields["gsd"] = min_gsd
                    item.properties["proj:epsg"] = 3067
                    csc_collection.add_item(item)

                    item_dict = item.to_dict()
                    converted_item = json_convert(item_dict)
                    request_point = f"collections/{csc_collection.id}/products"
                    r = session.post(urljoin(app_host, request_point), headers=log_headers, json=converted_item)
                    r.raise_for_status()

        print(f"{len(csc_collection_item_ids)}/{number_of_items_in_geocubes}")
        if number_of_items_added:
            # Update the extents from the GeoCubes Items
            csc_collection.update_extent_from_items()
            collection_dict = csc_collection.to_dict()
            converted_collection = json_convert(collection_dict)
            request_point = f"collections/{csc_collection.id}/"

            r = session.put(urljoin(app_host, request_point), headers=log_headers, json=converted_collection)
            r.raise_for_status()
            print(f" + Number of items added: {number_of_items_added}")
            print(" + Updated Collection Extents.")
        else:
            print(" * All items present.")


if __name__ == "__main__":

    """
    The first check for REST API password is from a password file. 
    If a password file is not found, the script prompts the user to give a password through CLI
    """
    pw_filename = 'passwords.txt'
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)
    
    args = parser.parse_args()

    try:
        pw_file = pd.read_csv(pw_filename, header=None)
        pwd = pw_file.at[0,0]
    except FileNotFoundError:
        print("Password not given as an argument and no password file found")
        pwd = getpass.getpass()
        
    start = time.time()

    app_host = f"{args.host}/geoserver/rest/oseo/"
    csc_catalog_client = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/", request_modifier=change_to_https)

    print(f"Updating STAC Catalog at {args.host}")
    update_catalog(app_host, csc_catalog_client)

    end = time.time()
    print(f"Script took {round(end-start, 1)} seconds")