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

    col_name = re.sub('\W+','_', collection_info['Name'].lower())
    col_id = f"{col_name}_at_geocubes"

    collection = pystac.Collection(
        id = col_id,
        title = collection_info['Name'],
        description = collection_info['Description'],
        license = "CC-BY-4.0",
        extent = pystac.Extent( #Placeholder extents, updated from items later
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

datasets = get_datasets()
try:
    catalog = pystac.Catalog("GeoCubes", "Testing catalog", catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED)
except:
    catalog = pystac.Catalog.from_file("Geocubes/catalog.json")

collection_csv = pandas.read_csv('karttatasot.csv', index_col='Nimi').to_dict('index')

for col in collection_csv:

    collection_info = collection_csv[col]
    dataset_info = datasets[col]
    print(collection_info)
    print(dataset_info)

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
                if assets[asset_id].extra_fields["gsd"] not in collection.summaries.lists["gsd"]:
                    collection.summaries.lists["gsd"].append(assets[asset_id].extra_fields["gsd"])
                min_gsd = min(min_gsd, assets[asset_id].extra_fields["gsd"])

            item_year = key.split("_")[1]
            item = create_stac_item(
                year_path+key+".tif", 
                assets=assets, 
                asset_media_type=pystac.MediaType.TIFF, 
                with_proj=True, 
            )
            item_info = "_".join(item.id.split(".")[0].split("_")[1:])
            item.id = f"{collection_info['Name'].lower().replace(' ', '_').replace(',', '')}_{item_info}"
            item.common_metadata.start_datetime = item_starttime
            item.common_metadata.end_datetime = item_endtime
            item.extra_fields["gsd"] = min_gsd
            collection.add_item(item)
            print(f"* Item made: {item.id}")

    bounds = [GeometryCollection([shape(s.geometry) for s in collection.get_all_items()]).bounds]
    start_times = [st.common_metadata.start_datetime for st in collection.get_all_items()]
    end_times = [et.common_metadata.end_datetime for et in collection.get_all_items()]
    temporal = [[min(start_times), max(end_times)]]
    collection.extent.spatial = pystac.SpatialExtent(bounds)
    collection.extent.temporal = pystac.TemporalExtent(temporal)

catalog.normalize_and_save("GeoCubes")