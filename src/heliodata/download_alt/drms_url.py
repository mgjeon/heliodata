# https://github.com/RobertJaro/InstrumentToInstrument/blob/master/itipy/download/download_sdo.py

import drms
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from astropy.io import fits
from sunpy.io._fits import header_to_fits
from sunpy.util import MetaDict
from tqdm import tqdm 
from urllib.request import urlretrieve
def download_url(url, filename):
    desc = url.split('/')[-1]
    def reporthook(block_num, block_size, total_size):
        if pbar.total != total_size:
            pbar.total = total_size
        pbar.update(block_num * block_size - pbar.n)
    with tqdm(unit='B', unit_scale=True, unit_divisor=1024, desc=desc) as pbar:
        urlretrieve(url, filename, reporthook=reporthook)

#------------------------------------------------------------

obstime = '2016-01-01T00:00:00'
target_path = Path('./test')

q_euv = f'aia.lev1_euv_12s[{obstime}][94,131,171,193,211,304,335]'+'{image}'
q_uv = f'aia.lev1_uv_24s[{obstime}][1600,1700]'+'{image}'
q_vis = f'aia.lev1_vis_1h[{obstime}][4500]'+'{image}'
q_hmi_mag = f'hmi.m_720s[{obstime}]'+'{image}'

#------------------------------------------------------------

q = q_euv 

#------------------------------------------------------------

target_path.mkdir(parents=True, exist_ok=True)
c = drms.Client()
keys = c.keys(q)
header, segment = c.query(q, key=','.join(keys), seg='image')
queue = []
for (idx, h), seg in zip(header.iterrows(), segment.image):
    queue += [(h.to_dict(), seg)]
print(len(queue), "files to download")
for sample in queue:
    header, segment = sample
    # Create target filename
    t = datetime.strptime(header['T_OBS'],'%Y-%m-%dT%H:%M:%S.%fZ')
    t = t + timedelta(seconds=0.5)  # round to nearest second
    t = t.strftime('%Y-%m-%dT%H%M%SZ')
    w = header['WAVELNTH']
    file = f'aia.lev1_euv_12s.{t}.{w}.image_lev1.fits'
    # Download the file
    url = 'http://jsoc.stanford.edu' + segment
    download_url(url, target_path/file)
    # Update FITS header
    header['DATE_OBS'] = header['DATE__OBS']
    header = header_to_fits(MetaDict(header))
    with fits.open(target_path/file, 'update') as f:
        hdr = f[1].header
        for k, v in header.items():
            if pd.isna(v):
                continue
            hdr[k] = v
        f.verify('silentfix')
