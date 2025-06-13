import json
import argparse
from pathlib import Path

import pandas as pd
from loguru import logger

from sunpy.net import Fido, attrs as a
import sunpy_soar

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Download SolO/EUI/FSI data from SOAR')

    parser.add_argument('--ds_path', type=str, help='path to the download directory.', required=True)
    parser.add_argument('--start_year', type=int, help='start year in format YYYY.', required=False, default=2021)
    parser.add_argument('--end_year', type=int, help='end year in format YYYY.', required=False, default=2024)
    parser.add_argument('--cadence', type=int, help='sample cadence in hours', required=False, default=24)
    parser.add_argument('--ignore_info', action='store_true', help='ignore info.json file', required=False, default=False)

    parser.add_argument('--wavelengths', type=str, help='wavelengths to download.', required=False, default="174,304")
    parser.add_argument('--margin', type=int, help='time margin in hours', required=False, default=1)
    parser.add_argument('--level', type=int, help='data level', required=False, default=2)

    args = parser.parse_args()
    
    dataroot = Path(args.ds_path)
    log_file = dataroot / 'info.log'
    logger.add(log_file)
    logger.info(vars(args))
    logger.info('-'*20)

    wavelengths = [wl for wl in args.wavelengths.split(',')]
    [(dataroot/str(args.level)/wav).mkdir(exist_ok=True, parents=True) for wav in wavelengths]

    start_year = args.start_year
    end_year = args.end_year
    times = []
    year = start_year
    while year <= end_year:
        times.append(a.Time(f'{year}-01-01T00:00:00', f'{year}-12-31T23:59:59'))
        year = year + 1

    info_path = dataroot / 'info.json'
    if info_path.exists() and not args.ignore_info:
        with open(info_path, 'r') as f:
            info = json.load(f)
    else:
        info = {}
        for tr in times:
            info[str(tr)] = {}
            for wav in wavelengths:
                info[str(tr)][wav] = None

    w2p = {
        '174': 'eui-fsi174-image',
        '304': 'eui-fsi304-image',
    }

    for tr in times:
        logger.info(tr)
        for wav in wavelengths:
            logger.info(wav)

            try:
                n_found_files = info[str(tr)][wav]
            except KeyError:
                info[str(tr)] = {}
                info[str(tr)][wav] = None
                n_found_files = None

            res_path = dataroot/str(args.level)/wav/str(tr.start.datetime.year)
            res_path.mkdir(exist_ok=True, parents=True)
            n_exist_files = len(list((res_path).glob('*.fits')))

            if (n_found_files is None) or (n_found_files != n_exist_files):
                search = Fido.search(
                    tr,
                    a.Instrument('EUI'),
                    a.Level(args.level),
                    a.soar.Product(w2p[wav]),
                )
                if len(search) == 0:
                    n_found_files = 0
                elif len(search['soar']) > 0:
                    dates = pd.date_range(
                        start=tr.start.datetime, 
                        end=tr.end.datetime, 
                        freq=pd.Timedelta(f'{args.cadence}h')
                    )
                    ts = pd.to_datetime(
                        pd.Series(search['soar']['Start time'])
                    )
                    indices = []
                    for d in dates:
                        fs = ts[ts.sub(d).abs() < pd.Timedelta(args.margin, 'h')]
                        if len(fs) > 0:
                            idx = fs.sub(d).abs().idxmin()
                            indices.append(idx)
                    search = search['soar'][indices]
                    n_found_files = len(search)
                info[str(tr)][wav] = n_found_files
            else:
                search = None

            with open(info_path, 'w') as f:
                json.dump(info, f, indent=4)

            logger.info(f'Found {n_found_files}')
            logger.info(f'Exist {n_exist_files}')

            if (n_found_files == 0) or (n_found_files == n_exist_files):
                logger.info('Skip')
                continue

            if search is not None:
                Fido.fetch(search, path=res_path)

    logger.info(f'Finished {tr}')