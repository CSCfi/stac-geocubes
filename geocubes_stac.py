import pystac
import rasterio
import requests
import datetime
import pandas
import re
from bs4 import BeautifulSoup
from rio_stac.stac import create_stac_item
from shapely.geometry import GeometryCollection, shape
from pathlib import Path

def create_collection(collection_info, dataset_info):

    """
        Create collection using the dataset info gathered from the API and the provided collection info from the CSV
        Returns the collection as pystac.Collection
    """

    # The regural expression sub is changing the spaces into underscores
    # For sentinel and NDVI collections, the name is specified a bit different as the names contain the years/months of the data
    col_name = re.sub('\W+','_', collection_info['Name'].lower())
    if "sentinel" in col_name:
        split = col_name.split("_")[:-2]
        col_name = "_".join(split)
    elif "ndvi" in col_name:
        split = col_name.split("_")[:-1]
        col_name = "_".join(split)

    col_id = f"{col_name}_at_geocubes"

    collection = pystac.Collection(
        id = col_id,
        title = collection_info['Name'],
        description = collection_info['Description'],
        license = "CC-BY-4.0",
        #Placeholder extents, updated from items later
        extent = pystac.Extent(
            spatial = pystac.SpatialExtent([[0,0,0,0]]),
            temporal = pystac.TemporalExtent([(
                datetime.datetime.strptime(f"2000-01-01", "%Y-%m-%d"),
                datetime.datetime.strptime(f"2000-12-31", "%Y-%m-%d")
            )])
        ),
        providers = [
            pystac.Provider(
                name = "CSC Finland",
                url = "https://www.csc.fi/",
                roles = ["host"]
            ),
            pystac.Provider(
                name = dataset_info['producer'],
                roles = ["producer"]
            )
        ],
        assets = {
            "meta": pystac.Asset(
                dataset_info["metadata_URL"],
                title = "Metadata",
                roles = ["metadata"]
            )
        },
        summaries = pystac.Summaries(
            summaries = {
                "gsd": []
            }
        )
    )

    print(f"Collection made: {collection.id}")
    
    return collection

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

    datasets = get_datasets()

    try: # Takes the awailable catalog if it exists
        catalog = pystac.Catalog("GeoCubes", "Testing catalog", catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED)
    except:
        catalog = pystac.Catalog.from_file("Geocubes/catalog.json")

    # Information and translations of the GeoCubes
    collection_csv = pandas.read_csv('karttatasot.csv', index_col='Nimi').to_dict('index')

    for col in collection_csv:

        collection_info = collection_csv[col]
        dataset_info = datasets[col]

        collection = create_collection(collection_info, dataset_info)
        catalog.add_child(collection)
        
        for year_path in dataset_info['paths']:

            #TIFs through BeautifulSoup
            page = requests.get(year_path)
            data = page.text
            soup = BeautifulSoup(data, features="html.parser")

            links = [link for link in soup.find_all("a")]

            assets = {}
            item_links = [link.get("href") for link in links if link.get("href").endswith("tif")]
            item_sets = [item.split(".")[0] for item in item_links]
            
            grouped_dict = {}
            for item in item_sets:
                prefix = "_".join(item.split("_")[:4])
                if prefix not in grouped_dict:
                    grouped_dict[prefix] = []
                grouped_dict[prefix].append(item)

            for key in grouped_dict.keys():
                
                # Takes the year from the path
                item_starttime = datetime.datetime.strptime(f"{year_path.split('/')[-2]}-01-01", "%Y-%m-%d")
                item_endtime = datetime.datetime.strptime(f"{year_path.split('/')[-2]}-12-31", "%Y-%m-%d")

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
                    if assets[asset_id].extra_fields["gsd"] not in collection.summaries.lists["gsd"]:
                        collection.summaries.lists["gsd"].append(assets[asset_id].extra_fields["gsd"])
                    min_gsd = min(min_gsd, assets[asset_id].extra_fields["gsd"])

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
                    item_id = f"{collection_info['Name'].lower().replace(' ', '_').replace(',', '')}_{item_info}"

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
                collection.add_item(item)
                print(f"* Item made: {item.id}")

        # Updating the Spatial and Temporal Extents from the data
        bounds = [GeometryCollection([shape(s.geometry) for s in collection.get_all_items()]).bounds]
        start_times = [st.common_metadata.start_datetime for st in collection.get_all_items()]
        end_times = [et.common_metadata.end_datetime for et in collection.get_all_items()]
        temporal = [[min(start_times), max(end_times)]]
        collection.extent.spatial = pystac.SpatialExtent(bounds)
        collection.extent.temporal = pystac.TemporalExtent(temporal)

    catalog.normalize_and_save("GeoCubes")