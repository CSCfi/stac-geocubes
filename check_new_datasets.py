import requests
import pandas as pd
import re
import argparse
import pystac_client

def change_user_agent(request: requests.Request) -> requests.Request: 
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

if __name__ == "__main__":

    pw_filename = 'passwords.txt'
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)
    args = parser.parse_args()

    app_host = f"{args.host}/geoserver/rest/oseo/"
    csc_catalog_client = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/", request_modifier=change_user_agent)

    title_regex_pattern = r" \(GeoCubes\)"

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
        if dataset not in collection_csv.keys():
            print(f"New dataset in GeoCubes: {geocubes_datasets[dataset]["name"]}")
            print(f"Folder: {geocubes_datasets[dataset]['folder']}")
            print(f"Metadata: {geocubes_datasets[dataset]['metadata_URL']}")