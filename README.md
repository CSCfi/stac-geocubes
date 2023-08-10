# stac-geocubes
Scripts to convert GeoCubes into STAC collections

GeoCubes-folder contains a backup of the completed collections

The collection information and translations are in `karttatasot.csv`

Run `geocubes_stac.py` to turn the GeoCubes into STAC
```
python geocubes_stac.py 
```

Run `geocubes_to_geoserver.py` to upload the completed Collections to Geoserver. Provide the host address as an argument.
```
python geocubes_to_geoserver.py --host <upload-host-address>
```