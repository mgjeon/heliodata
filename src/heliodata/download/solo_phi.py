import json
import argparse
from pathlib import Path

import pandas as pd
import astropy.units as u
import sunpy_soar
from sunpy.net import Fido, attrs as a
from loguru import logger

from heliodata.download.util import get_times, get_respath


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Download SolO/PHI/FDT data from SOAR')

    parser.add_argument('--ds_path', type=str, help='path to the download directory.', required=True)
    parser.add_argument('--start_year', type=int, help='start year in format YYYY.', required=False, default=2022)
    parser.add_argument('--end_year', type=int, help='end year in format YYYY.', required=False, default=2024)
    parser.add_argument('--cadence', type=int, help='sample cadence in hours', required=False, default=24)
    parser.add_argument('--ignore_info', action='store_true', help='ignore info.json file', required=False, default=False)
    parser.add_argument('--interval', choices=['year', 'month'], default='year',
                        help='interval for the time range, either year or month.', required=False)

    parser.add_argument('--product', type=str, help='product to download.', required=False, default="phi-fdt-blos")
    parser.add_argument('--margin', type=int, help='time margin in hours', required=False, default=1)
    parser.add_argument('--level', type=int, help='data level', required=False, default=2)

    args = parser.parse_args()
    
    dataroot = Path(args.ds_path)
    logger.add(dataroot / 'info.log')
    logger.info(vars(args))
    logger.info('-'*20)

    products = [ds for ds in args.product.split(',')]
    [(dataroot/str(args.level)/ds).mkdir(exist_ok=True, parents=True) for ds in products]

    times = get_times(args.start_year, args.end_year, args.interval)

    info_path = dataroot / 'info.json'
    if info_path.exists() and not args.ignore_info:
        with open(info_path, 'r') as f:
            info = json.load(f)
    else:
        info = {}
        for tr in times:
            info[str(tr)] = {}
            for ds in products:
                info[str(tr)][ds] = None

    for tr in times:
        if str(tr) not in info:
            info[str(tr)] = {}
        logger.info(tr)
        for ds in products:
            logger.info(ds)

            try:
                n_found_files = info[str(tr)][ds]
            except KeyError:
                info[str(tr)] = {}
                info[str(tr)][ds] = None
                n_found_files = None

            res_path = get_respath(dataroot/str(args.level)/ds, tr, args.interval)
            n_exist_files = len(list((res_path).glob('*.fits')))

            if (n_found_files is None) or (n_found_files != n_exist_files):
                search = Fido.search(
                    tr,
                    a.Instrument('PHI'),
                    a.Level(args.level),
                    a.soar.Product(args.product),
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
                info[str(tr)][ds] = n_found_files
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