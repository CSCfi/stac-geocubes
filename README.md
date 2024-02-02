# stac-geocubes
Scripts to convert GeoCubes into STAC collections

GeoCubes-folder contains a backup of the completed collections

The collection information and translations are in `karttatasot.csv`. If new datasets are added to GeoCubes, the translations of these datasets need to be added to `karttatasot.csv` before the script takes them into account.

Run `geocubes_stac.py` to turn the GeoCubes into STAC
```bash
python geocubes_stac.py 
```

Run `geocubes_to_geoserver.py` to upload the completed Collections to Geoserver. Provide the host address as an argument.
```bash
python geocubes_to_geoserver.py --host <upload-host-address>
```

Run `update_geocubes.py` to update the GeoCubes collections in the selected host. Provide the host address as an argument.
```bash
python update_geocubes.py --host <update-host-address>
```

The `check_new_datasets.py` script checks if there's any new datasets in GeoCubes.
```bash
python check_new_datasets.py --host <host-address-to-compare-against>
```