import argparse
import shutil
import itertools
from datetime import datetime
from pathlib import Path
from loguru import logger

import pandas as pd
import numpy as np
import astropy.units as u
from sunpy.net import Fido, attrs as a
from tqdm import tqdm
import warnings; warnings.filterwarnings("ignore")
import logging
from parfive import Downloader
logging.getLogger('sunpy').setLevel(logging.ERROR)
logging.getLogger('parfive').setLevel(logging.ERROR)
logging.getLogger('zeep').setLevel(logging.ERROR)
import contextlib
import sunpy_soar
class DownloaderLeaveFalse(Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_main_pb(self, total):
        if self.config.progress:
            return self.tqdm(
                total=total,
                unit="file",
                desc="Files Downloaded",
                position=1,
                leave=False   # 여기 추가
            )
        return contextlib.contextmanager(lambda: iter([None]))()

dl = DownloaderLeaveFalse(progress=True, overwrite=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', default='F:/data/raw/solo')
    parser.add_argument('--start', default='2021-01-01T00:00:00')
    parser.add_argument('--end',   default='2025-01-01T00:00:00')  # exclusive
    parser.add_argument('--cadence',  default='24h')
    parser.add_argument('--margin', default=15)  # minutes
    parser.add_argument('--level', default=2)
    parser.add_argument('--product', default='eui-fsi174-image,eui-fsi304-image,phi-fdt-blos')
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

    ds = args.product.split(',')
    for d in ds:
        (ROOT / d).mkdir(exist_ok=True, parents=True)

    CSV_FILE = ROOT / 'info.csv'

    if CSV_FILE.exists():
        # backup 
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = CSV_FILE.with_name(f'info_{timestamp}.csv')
        shutil.copy2(CSV_FILE, backup_file)

        # load
        df_old = pd.read_csv(CSV_FILE, dtype=str)
        df_old = df_old[df_old['filepath'] != 'NODATA']

        df_times = [t.strftime('%Y-%m-%dT%H:%M:%S') for t in times]
        df_new = pd.DataFrame(
            itertools.product(df_times, ds),
            columns=['obstime', 'product']
        )
        df_new['filepath'] = 'NODATA'
        df = pd.concat([df_old, df_new], ignore_index=True)
        df = df.drop_duplicates(subset=['obstime', 'product'], keep='first')
        df = df.sort_values(by=['obstime', 'product']).reset_index(drop=True)
        df.to_csv(CSV_FILE, index=False)
    else:
        df_times = [t.strftime('%Y-%m-%dT%H:%M:%S') for t in times]
        df = pd.DataFrame(
            itertools.product(df_times, ds),
            columns=['obstime', 'product']
        )
        df['filepath'] = 'NODATA'
        df.to_csv(CSV_FILE, index=False)
    # 

    t_margin = pd.Timedelta(minutes=args.margin)
    for t in tqdm(times, desc=f'Download {args.product}', position=0, leave=True):

        t_query = t.strftime('%Y-%m-%dT%H:%M:%S')
        t_file  = t.strftime('%Y-%m-%dT%H%M%S')

        nodata  = (df[df['obstime'] == t_query]['filepath'] == 'NODATA').any()   # Yet to download
        nodata0 = (df[df['obstime'] == t_query]['filepath'] == 'NODATA0').any()  # Query failed
        nodata1 = (df[df['obstime'] == t_query]['filepath'] == 'NODATA1').any()  # Download failed
        # nodata2 = (df[df['obstime'] == t_query]['filepath'] == 'NODATA2').any()  # No data found
        if nodata or nodata0 or nodata1:
            for d in ds:
                inst = str(d)[:3].upper()  # e.g., 'EUI' from 'eui-fsi174-image'
                try:
                    search = Fido.search(
                        a.Time(t - t_margin, t + t_margin),
                        a.Instrument(inst),
                        a.Level(args.level),
                        a.soar.Product(d),
                    )
                except Exception as e:
                    df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA0'
                    df.to_csv(CSV_FILE, index=False)
                    logger.error(f"NODATA0 : Query failed : {t_query} : {d} : {e}")
                    continue

                if len(search['soar']) > 0:
                    search_times = pd.to_datetime(search['soar']['Start time'])
                    diff_times = list(abs(search_times - t).total_seconds())
                    closest_search = search['soar'][np.argmin(diff_times)]
                    try:
                        files = Fido.fetch(closest_search, path=ROOT / d, downloader=dl)
                        if len(files) == 1:
                            file = files[0]
                            filename = f'{t_file}.fits'
                            filepath = ROOT / d/ filename
                            shutil.move(file, filepath)
                            df.loc[(df['obstime'] == t_query) & (df['product'] == d), 'filepath'] = f'{d}/{filename}'
                            df.to_csv(CSV_FILE, index=False)
                        else:
                            df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA1'
                            df.to_csv(CSV_FILE, index=False)
                            logger.error(f"NODATA1 : Files found ({len(files)}) : {t_query} : {d}")
                            continue
                    except Exception as e:
                        df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA1'
                        df.to_csv(CSV_FILE, index=False)
                        logger.error(f"NODATA1 : Error occurred : {t_query} : {d} : {e}")
                        continue
                else:
                    df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA2'
                    df.to_csv(CSV_FILE, index=False)
                    logger.error(f"NODATA2 : No data found : {t_query} : {d}")
                    continue
                