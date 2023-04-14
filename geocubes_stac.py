import pystac
from rio_stac.stac import create_stac_item
from shapely.geometry import GeometryCollection, shape
import rasterio
import requests
from pathlib import Path
import datetime
from bs4 import BeautifulSoup

vrts = [
    "https://vm0160.kaj.pouta.csc.fi/mml/korkeusmalli/km2/2022/km2_2022.vrt",
    "https://vm0160.kaj.pouta.csc.fi/mml/korkeusmalli/km2/2020/km2_2020.vrt",
    "https://vm0160.kaj.pouta.csc.fi/mml/korkeusmalli/km2/2019/km2_2019.vrt",
    "https://vm0160.kaj.pouta.csc.fi/mml/korkeusmalli/km2/2018/km2_2018.vrt"
]

year_collections = [
    "https://vm0160.kaj.pouta.csc.fi/mml/korkeusmalli/km2/2022/",
    "https://vm0160.kaj.pouta.csc.fi/mml/korkeusmalli/km2/2020/",
    "https://vm0160.kaj.pouta.csc.fi/mml/korkeusmalli/km2/2019/",
    "https://vm0160.kaj.pouta.csc.fi/mml/korkeusmalli/km2/2018/"
]

#TIFs through VRTs
# for vrt in vrts:
#     with rasterio.open(vrt) as r:
#         files = r.files
#         tifs = {}
#         for i,file in enumerate(files):
#             if file.endswith("tif"):
#                 tifs[file.split("/vsicurl/")[-1].split("/")[-1]] = file.split("/vsicurl/")[-1]

catalog = pystac.Catalog("GeoCubes", "Testing catalog", catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED)
collection = pystac.Collection(
    id = "km2_at_geocubes",
    title = "MML Korkeusmalli Test",
    description = "Testing collection for MML Korkeusmalli GeoCubes",
    license  = "CC-BY-4.0",
    extent = pystac.Extent(
        spatial = pystac.SpatialExtent([[0,0,0,0]]), #Placeholder extent, updated from items later
        temporal = pystac.TemporalExtent([(
            datetime.datetime.strptime("2018-01-01", "%Y-%m-%d"), 
            datetime.datetime.strptime("2022-12-31", "%Y-%m-%d")
        )])
    ),
    providers = [
        pystac.Provider(
            name = "CSC Finland",
            url = "https://www.csc.fi/",
            roles = ["host"]
        ),
        pystac.Provider(
            name = "NLS/FGI",
            roles = ["processor"]
        )
    ],
    summaries = pystac.Summaries(
        summaries = {
            "gsd": [1,2,5,10,20,50,100,200,500,1000]
        }
    )
)
print(f"Collection made: {collection.id}")
catalog.add_child(collection)

#TIFs through BeautifulSoup
for year in year_collections:

    page = requests.get(year)
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
        assets = {
            "COG": pystac.Asset(
                href=year+grouped_dict[key][0]+".tif", 
                media_type="image/tiff; application=geotiff; profile=cloud-optimized", 
                title="COG",
                roles=["data"]
            )
        }
        for asset in grouped_dict[key][1:]:
            with rasterio.open(year+asset+".tif") as src:
                assets[asset.split("_")[-1]] = pystac.Asset(
                    href=year+asset+".tif",
                    media_type="image/tiff; application=geotiff", 
                    title=asset.split("_")[-1],
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
        item_year = key.split("_")[1]
        item = create_stac_item(
            year+key+".tif", 
            assets=assets, 
            asset_media_type=pystac.MediaType.TIFF, 
            with_proj=True, 
            input_datetime = datetime.datetime.strptime(f"{item_year}-12-31", "%Y-%m-%d")
        )
        item.id = item.id.split(".")[0]
        item.extra_fields["gsd"] = 1
        assert item.validate()
        collection.add_item(item)
        print(f"* Item made: {item.id}")

    bounds = [GeometryCollection([shape(s.geometry) for s in collection.get_all_items()]).bounds]
    collection.extent.spatial = pystac.SpatialExtent(bounds)

catalog.normalize_and_save("mml-korkeusmalli")