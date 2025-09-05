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
    
def get_times(start_year, end_year, interval):
    """
    Generate a list of time ranges based on the specified interval.
    """
    times = []
    
    if interval == 'year':
        year = start_year
        while year <= end_year:
            times.append(a.Time(f'{year}-01-01T00:00:00', f'{year}-12-31T23:59:59'))
            year = year + 1
    
    elif interval == 'month':
        year = start_year
        while year <= end_year:
            for month in range(1, 13):
                if month < 12:
                    dt = datetime(year, month, 1)
                    dtn = datetime(year, month+1, 1)
                    tr = a.Time(dt.strftime('%Y-%m-%dT%H:%M:%S'), dtn.strftime('%Y-%m-%dT%H:%M:%S'))
                elif month == 12:
                    dt = datetime(year, month, 1)
                    dtn = datetime(year+1, 1, 1)
                    tr = a.Time(dt.strftime('%Y-%m-%dT%H:%M:%S'), dtn.strftime('%Y-%m-%dT%H:%M:%S'))
                times.append(tr)
            year = year + 1
    
    return times

dl = DownloaderLeaveFalse(progress=True, overwrite=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', default='F:/data/raw/solo/phi')
    parser.add_argument('--start', default='2021-01-01T00:00:00')
    parser.add_argument('--end',   default='2025-01-01T00:00:00')  # exclusive
    parser.add_argument('--interval', default='year')
    parser.add_argument('--level', default=2)
    parser.add_argument('--product', default='phi-fdt-blos')
    args = parser.parse_args()

    ROOT = Path(args.root); ROOT.mkdir(exist_ok=True, parents=True)
    logger.add(ROOT / 'info.log')
    logger.info(vars(args))

    dt_start = datetime.strptime(args.start, '%Y-%m-%dT%H:%M:%S')
    dt_end   = datetime.strptime(args.end,   '%Y-%m-%dT%H:%M:%S')
    times = get_times(dt_start.year, dt_end.year - 1, args.interval)
    
    ds = args.product.split(',')
    for d in ds:
        (ROOT / d).mkdir(exist_ok=True, parents=True)

    for tr in tqdm(times, desc=f'Download {args.product}', position=0, leave=True):
        try:
            search = Fido.search(
                tr,
                a.Instrument('PHI'),
                a.Level(args.level),
                a.soar.Product(d),
            )
        except Exception as e:
            logger.error(f"{tr} : {e}")
            continue

        if len(search['soar']) > 0:
            try:
                files = Fido.fetch(search['soar'], path=ROOT / d, downloader=dl)
            except Exception as e:
                logger.error(f"{tr} : {e}")
                continue