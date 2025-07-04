from pathlib import Path
from datetime import datetime, timedelta
import s3fs
import zarr
import numpy as np
import dask.array as da
from sunpy.map import Map
def s3_connection(path_to_zarr):
    return s3fs.S3Map(
        root=path_to_zarr,
        s3=s3fs.S3FileSystem(anon=True),
        check=False,
    )
def load_single_aws_zarr(
    path_to_zarr,
    cache_max_single_size=None,
):
    return zarr.open(
        zarr.LRUStoreCache(
            store=s3_connection(path_to_zarr),
            max_size=cache_max_single_size,
        ),
        mode="r",
    )

#------------------------------------------------------------

target_path = Path('./test')
wavelengths = ["94A", "131A", "171A", "193A", "211A", "304A", "335A"]

#------------------------------------------------------------

target_path.mkdir(parents=True, exist_ok=True)
root = load_single_aws_zarr(
    path_to_zarr="s3://gov-nasa-hdrl-data1/contrib/fdl-sdoml/fdl-sdoml-v2/sdomlv2.zarr/",
)
for w in wavelengths:
    data = root["2016"][w]
    sorted_indices = np.argsort(data.attrs["T_OBS"])
    img_index = sorted_indices[0]
    t = data.attrs["T_OBS"][img_index]
    t = datetime.strptime(t,'%Y-%m-%dT%H:%M:%S.%fZ')
    t = t + timedelta(seconds=0.5)  # round to nearest second
    t = t.strftime('%Y-%m-%dT%H%M%SZ')
    print(f"Processing {w} at {t}")
    selected_headr = {keys: values[img_index] for keys, values in data.attrs.items()}
    selected_image = da.from_array(data)[img_index, :, :]
    smap_ml = Map((np.array(selected_image), selected_headr))
    smap_ml.save(target_path / f'sdoml_{t}_{w}.fits', overwrite=True)