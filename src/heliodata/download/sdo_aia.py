import argparse
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from sunpy.map import Map

import drms
import numpy as np
from tqdm import tqdm
from urllib.request import urlretrieve
from astropy.io import fits
from sunpy.io._fits import header_to_fits
from sunpy.util import MetaDict
import warnings; warnings.simplefilter("ignore")
import logging, sunpy; logging.getLogger('sunpy').setLevel(logging.ERROR)


# def download_url(url, filepath):
#     desc = url.split('/')[-1]
#     def reporthook(block_num, block_size, total_size):
#         if pbar.total != total_size:
#             pbar.total = total_size
#         pbar.update(block_num * block_size - pbar.n)
#     with tqdm(unit='B', unit_scale=True, unit_divisor=1024, desc=desc, leave=False) as pbar:
#         urlretrieve(url, filepath, reporthook=reporthook)

def update_header(header, filepath):
    header['DATE_OBS'] = header['DATE__OBS']
    header = header_to_fits(MetaDict(header))
    with fits.open(filepath, 'update') as f:
        hdr = f[1].header
        for k, v in header.items():
            if pd.isna(v):
                continue
            hdr[k] = v
        f.verify('silentfix')


if __name__ == '__main__':
    #
    parser = argparse.ArgumentParser()

    parser.add_argument('--temp', default='D:/temp')
    parser.add_argument('--root', default='F:/data/raw/sdo/aia')

    parser.add_argument('--start', default='2011-01-01T00:00:00')
    parser.add_argument('--end',   default='2025-01-01T00:00:00')  # exclusive
    parser.add_argument('--cadence',  default='24h')

    parser.add_argument('--series', default='euv_12s')
    parser.add_argument('--wavelengths', default='94,131,171,193,211,304,335')

    args = parser.parse_args()

    TEMP = Path(args.temp); TEMP.mkdir(exist_ok=True, parents=True)
    ROOT = Path(args.root)    ; ROOT.mkdir(exist_ok=True, parents=True)

    dt_start = datetime.strptime(args.start, '%Y-%m-%dT%H:%M:%S')
    dt_end   = datetime.strptime(args.end,   '%Y-%m-%dT%H:%M:%S')
    times = []
    for t in pd.date_range(dt_start, dt_end, freq=args.cadence, inclusive='left'):
        times.append(t)
    #

    c = drms.Client()
    
    for wavelnth in args.wavelengths.split(','):
        (ROOT / wavelnth).mkdir(exist_ok=True, parents=True)
        CSV_FILE = ROOT / wavelnth / 'info.csv'
        
        if CSV_FILE.exists():
            df = pd.read_csv(CSV_FILE)
        else:
            df_times = [t.strftime('%Y-%m-%dT%H:%M:%S') for t in times]
            df = pd.DataFrame(df_times, columns=['obstime'])
            df['filename'] = 'NULL'
            df.to_csv(CSV_FILE, index=False)

        for t in tqdm(times, desc=f'Dowloading {wavelnth}'):
            t_query = t.strftime('%Y-%m-%dT%H:%M:%S')
            t_file  = t.strftime('%Y-%m-%dT%H%M%S')
            df_filename = df[df['obstime'] == t_query]['filename'].iloc[0]
            if df_filename == 'NULL':  # there is no file
                # query to JSOC
                q = f'aia.lev1_{args.series}[{t_query}][{wavelnth}]' + '{image}'
                keys = c.keys(q)
                header, segment = c.query(q, key=','.join(keys), seg='image')
                header  = header.iloc[0].to_dict()
                segment = segment.iloc[0]['image']

                # download the file
                url = 'http://jsoc.stanford.edu' + segment
                filename = f'aia.{args.series}.{t_file}.fits'
                filepath = TEMP / filename
                urlretrieve(url, filepath)
                update_header(header, filepath)
                
                # check file & move
                smap = Map(filepath)
                shutil.move(filepath, ROOT / wavelnth / filename)

                # update CSV
                df.loc[df['obstime'] == t_query, 'filename'] = filename
                df.to_csv(CSV_FILE, index=False)