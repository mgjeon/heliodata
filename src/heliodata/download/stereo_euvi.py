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
    parser.add_argument('--root', default='F:/data/raw/stereo/euvi')
    parser.add_argument('--start', default='2011-01-01T00:00:00')
    parser.add_argument('--end',   default='2025-01-01T00:00:00')  # exclusive
    parser.add_argument('--cadence',  default='24h')
    parser.add_argument('--margin', default=15)  # minutes
    parser.add_argument('--stereo', default='STEREO_A,STEREO_B')
    parser.add_argument('--wavelengths', default='171,195,284,304')
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

    s2p = {
        'STEREO_A': 'a',
        'STEREO_B': 'b'
    }
    stereo = args.stereo.split(',')
    wls = args.wavelengths.split(',')
    for s in stereo:
        for wl in wls:
            (ROOT / s2p[s] / wl).mkdir(exist_ok=True, parents=True)

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
            itertools.product(df_times, stereo, wls),
            columns=['obstime', 'stereo', 'wavelength']
        )
        df_new['filepath'] = 'NODATA'
        df = pd.concat([df_old, df_new], ignore_index=True)
        df = df.drop_duplicates(subset=['obstime', 'stereo', 'wavelength'], keep='first')
        df = df.sort_values(by=['obstime', 'stereo', 'wavelength']).reset_index(drop=True)
        df.to_csv(CSV_FILE, index=False)
    else:
        df_times = [t.strftime('%Y-%m-%dT%H:%M:%S') for t in times]
        df = pd.DataFrame(
            itertools.product(df_times, stereo, wls),
            columns=['obstime', 'stereo', 'wavelength']
        )
        df['filepath'] = 'NODATA'
        df.to_csv(CSV_FILE, index=False)
    # 

    t_margin = pd.Timedelta(minutes=args.margin)
    for t in tqdm(times, desc=f'Download {args.wavelengths}', position=0, leave=True):

        t_query = t.strftime('%Y-%m-%dT%H:%M:%S')
        t_file  = t.strftime('%Y-%m-%dT%H%M%S')

        for s in stereo:
            if s == 'STEREO_B' and t > pd.Timestamp('2014-10-01'):
                df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA2'
                df.to_csv(CSV_FILE, index=False)
                continue

            nodata  = (df[df['obstime'] == t_query]['filepath'] == 'NODATA').any()   # Yet to download
            nodata0 = (df[df['obstime'] == t_query]['filepath'] == 'NODATA0').any()  # Query failed
            nodata1 = (df[df['obstime'] == t_query]['filepath'] == 'NODATA1').any()  # Download failed
            # nodata2 = (df[df['obstime'] == t_query]['filepath'] == 'NODATA2').any()  # No data found
            if nodata or nodata0 or nodata1:
                for w in wls:
                    try:
                        search = Fido.search(
                            a.Time(t - t_margin, t + t_margin),
                            a.Provider('SSC'),
                            a.Instrument('EUVI'),
                            a.Source(s),
                            a.Wavelength(int(w)*u.AA),
                        )
                    except Exception as e:
                        df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA0'
                        df.to_csv(CSV_FILE, index=False)
                        logger.error(f"NODATA0 : Query failed : {t_query} : {w} : {e}")
                        continue

                    if len(search) > 0:
                        search_times = pd.to_datetime(search['vso']['Start Time'].datetime)
                        diff_times = list(abs(search_times - t).total_seconds())
                        closest_search = search['vso'][np.argmin(diff_times)]
                        try:
                            files = Fido.fetch(closest_search, path=ROOT / s2p[s] / w, downloader=dl)
                            if len(files) == 1:
                                file = files[0]
                                filename = f'{t_file}.fits'
                                filepath = ROOT / s2p[s] / w / filename
                                shutil.move(file, filepath)
                                df.loc[(df['obstime'] == t_query) & (df['stereo'] == s) & (df['wavelength'] == w), 'filepath'] = f'{s2p[s]}/{w}/{filename}'
                                df.to_csv(CSV_FILE, index=False)
                            else:
                                df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA1'
                                df.to_csv(CSV_FILE, index=False)
                                logger.error(f"NODATA1 : Files found ({len(files)}) : {t_query} : {w}")
                                continue
                        except Exception as e:
                            df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA1'
                            df.to_csv(CSV_FILE, index=False)
                            logger.error(f"NODATA1 : Error occurred : {t_query} : {w} : {e}")
                            continue
                    else:
                        df.loc[df['obstime'] == t_query, 'filepath'] = 'NODATA2'
                        df.to_csv(CSV_FILE, index=False)
                        logger.error(f"NODATA2 : No data found : {t_query} : {w}")
                        continue
                    