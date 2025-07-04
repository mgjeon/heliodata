from tqdm import tqdm
from pathlib import Path
from sunpy.util.net import download_file

#------------------------------------------------------------

target_path = Path('./test')
root_url = "https://jsoc1.stanford.edu/data/aia/synoptic/"
date_str = "2016/01/01/H0000/AIA20160101_0000_"
wave_str = ["0094", "0131", "0171", "0193", "0211", "0304", "0335"]

#------------------------------------------------------------

target_path.mkdir(parents=True, exist_ok=True)
for wave in tqdm(wave_str):
    synoptic_url = f"{root_url}{date_str}{wave}.fits"
    download_file(synoptic_url, target_path)