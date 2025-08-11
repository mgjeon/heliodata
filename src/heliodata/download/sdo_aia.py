import argparse
import shutil
import itertools
from datetime import datetime
from pathlib import Path
from loguru import logger

import pandas as pd
import drms
import time
from tqdm import tqdm
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from astropy.io import fits
from sunpy.io._fits import header_to_fits
from sunpy.util import MetaDict
import warnings; warnings.filterwarnings("ignore")
import logging 
import sunpy; logging.getLogger('sunpy').setLevel(logging.ERROR)
import urllib3; logging.getLogger("urllib3").setLevel(logging.ERROR)


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

def download_with_retry(url, path, overall_timeout=30, chunk=1<<20, max_retries=3):
    sess = requests.Session()
    retry = Retry(
        total=max_retries, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    sess.mount("http://", HTTPAdapter(max_retries=retry))
    sess.mount("https://", HTTPAdapter(max_retries=retry))

    start = time.time()
    with sess.get(url, stream=True, timeout=(5, 10)) as r:  # (connect=5s, read=10s)
        r.raise_for_status()
        total_size = int(r.headers.get("Content-Length", 0))
        with open(path, "wb") as f, tqdm(
            total=total_size, unit="B", unit_scale=True, unit_divisor=1024,
            desc=str(path),
            leave=False
        ) as pbar:
            for chunk_bytes in r.iter_content(chunk_size=chunk):
                if chunk_bytes:
                    f.write(chunk_bytes)
                    pbar.update(len(chunk_bytes))
                if time.time() - start > overall_timeout:
                    raise TimeoutError("overall timeout exceeded")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', default='F:/data/raw/sdo/aia')
    parser.add_argument('--start', default='2011-01-01T00:00:00')
    parser.add_argument('--end',   default='2025-01-01T00:00:00')  # exclusive
    parser.add_argument('--cadence',  default='24h')
    parser.add_argument('--series', default='euv_12s')
    parser.add_argument('--wavelengths', default='94,131,171,193,211,304,335')
    parser.add_argument('--skip', default='NODATA2')
    args = parser.parse_args()

    ROOT = Path(args.root); ROOT.mkdir(exist_ok=True, parents=True)
    logger.remove()
    logger.add(ROOT / 'info.log')
    logger.info(vars(args))

    dt_start = datetime.strptime(args.start, '%Y-%m-%dT%H:%M:%S')
    dt_end   = datetime.strptime(args.end,   '%Y-%m-%dT%H:%M:%S')
    times = []
    for t in pd.date_range(dt_start, dt_end, freq=args.cadence, inclusive='left'):
        times.append(t)
    #

    wls = args.wavelengths.split(',')
    for wl in wls:
        (ROOT / wl).mkdir(exist_ok=True, parents=True)

    CSV_FILE = ROOT / 'info.csv'

    if CSV_FILE.exists():
        # backup 
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = CSV_FILE.with_name(f'info_{timestamp}.csv')
        shutil.copy2(CSV_FILE, backup_file)
        logger.info(f"Backup created: {backup_file}")

        # load
        df_old = pd.read_csv(CSV_FILE, dtype=str)
        df_old = df_old[df_old['filepath'] != 'NODATA']

        df_times = [t.strftime('%Y-%m-%dT%H:%M:%S') for t in times]
        df_new = pd.DataFrame(
            itertools.product(df_times, wls),
            columns=['obstime', 'wavelength']
        )
        df_new['filepath'] = 'NODATA'
        df = pd.concat([df_old, df_new], ignore_index=True)
        df = df.drop_duplicates(subset=['obstime', 'wavelength'], keep='first')
        df = df.sort_values(by=['obstime', 'wavelength']).reset_index(drop=True)
        df.to_csv(CSV_FILE, index=False)
    else:
        df_times = [t.strftime('%Y-%m-%dT%H:%M:%S') for t in times]
        df = pd.DataFrame(
            itertools.product(df_times, wls),
            columns=['obstime', 'wavelength']
        )
        df['filepath'] = 'NODATA'
        df.to_csv(CSV_FILE, index=False)
    #

    skips = args.skip.split(',')

    c = drms.Client()
    for t in tqdm(times, desc=f'Download {args.wavelengths}'):
        t_query = t.strftime('%Y-%m-%dT%H:%M:%S')
        t_file  = t.strftime('%Y-%m-%dT%H%M%S')
        nodata  = (df[df['obstime'] == t_query]['filepath'] == 'NODATA').any()   # Yet to download
        nodata0 = (df[df['obstime'] == t_query]['filepath'] == 'NODATA0').any()  # Query failed
        nodata1 = (df[df['obstime'] == t_query]['filepath'] == 'NODATA1').any()  # Download failed
        nodata2 = (df[df['obstime'] == t_query]['filepath'] == 'NODATA2').any()  # No data found

        ischeck = False
        if nodata:
            ischeck = True
        if nodata0:
            ischeck = True
        if nodata1 and 'NODATA1' not in skips:
            ischeck = True
        if nodata2 and 'NODATA2' not in skips:
            ischeck = True

        if ischeck:
            # query to JSOC
            q = f'aia.lev1_{args.series}[{t_query}][{args.wavelengths}]' + '{image}'
            logger.info(q)
            try:
                keys = c.keys(q)
                header, segment = c.query(q, key=','.join(keys), seg='image')
            except Exception as e:
                df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA0'
                df.to_csv(CSV_FILE, index=False)
                logger.error(f"NODATA0 : Query failed : {t_query} : {e}")
                time.sleep(5)
                continue
            if len(header) > 0:
                if len(header) != len(wls):
                    wls_in_header = []
                    for idx, h in header.iterrows():
                        h = h.to_dict()
                        w = str(h['WAVELNTH'])
                        wls_in_header.append(w)
                    wls_not_in_header = [wl for wl in wls if wl not in wls_in_header]
                    for w in wls_not_in_header:
                        df.loc[(df['obstime'] == t_query) & (df['wavelength'] == w), 'filepath'] = 'NODATA2'
                        df.to_csv(CSV_FILE, index=False)
                        logger.error(f"NODATA2 : No data found : {t_query} : {w}")
                        continue

                for (idx, h), s in zip(header.iterrows(), segment['image']):
                    h = h.to_dict()
                    w = str(h['WAVELNTH'])
                    if 'NODATA' in df[(df['obstime'] == t_query) & (df['wavelength'] == w)]['filepath'].values[0]:
                        try:
                            # download the file
                            url = 'http://jsoc.stanford.edu' + s
                            filename = f'{t_file}.fits'
                            filepath = ROOT / w / filename
                            download_with_retry(url, filepath)
                            update_header(h, filepath)

                            # update CSV
                            df.loc[(df['obstime'] == t_query) & (df['wavelength'] == w), 'filepath'] = f'{w}/{filename}'
                            df.to_csv(CSV_FILE, index=False)
                        except Exception as e:
                            df.loc[(df['obstime'] == t_query) & (df['wavelength'] == w), 'filepath'] = 'NODATA1'
                            df.to_csv(CSV_FILE, index=False)
                            logger.error(f"NODATA1 : Download failed : {t_query} : {w} : {e}")
                            continue
            else:
                df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA2'
                df.to_csv(CSV_FILE, index=False)
                logger.error(f"NODATA2 : No data found : {t_query} : {args.wavelengths}")
                continue