import cloudcatalog
import s3fs
from pathlib import Path
import shutil

#------------------------------------------------------------

target_path = Path('./test')
ids = ['aia_0094', 'aia_0131', 'aia_0171', 'aia_0193', 'aia_0211', 'aia_0304', 'aia_0335']
start = '2016-01-01T00:00:00Z'
stop = '2016-01-01T00:04:00Z'

#------------------------------------------------------------

target_path.mkdir(parents=True, exist_ok=True)
search = cloudcatalog.EntireCatalogSearch()
aia_datasets = search.search_by_id('aia')
for dataset in aia_datasets:
    print(dataset)
fr = cloudcatalog.CloudCatalog("s3://gov-nasa-hdrl-data1/")
fs = s3fs.S3FileSystem(anon=True)

for myid in ids:
    mycat = fr.request_cloud_catalog(myid, start_date=start, stop_date=stop)
    print(f"{len(mycat)} files found")
    for i in range(len(mycat)):
        myitem = mycat.iloc[i]
        filename = myitem['datakey']
        print(filename)
        fs.download(filename, '.')
files = list(Path('.').glob('sdo_aia_h2*.fits'))
for f in files:
    shutil.move(f, target_path / f.name)
