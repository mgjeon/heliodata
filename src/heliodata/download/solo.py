import json
import argparse
from pathlib import Path

import pandas as pd
import astropy.units as u
import sunpy_soar
from sunpy.net import Fido, attrs as a
from loguru import logger

from sunpy.net import attrs as a
from datetime import datetime


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Download SolO/PHI data from SOAR')
    parser.add_argument('--ds_path', type=str, help='path to the download directory.', required=True)
    parser.add_argument('--start_date', type=str, help='start date in format YYYY-MM-DDTHH:MM:SS.', required=True)
    parser.add_argument('--end_date', type=str, help='end date in format YYYY-MM-DDTHH:MM:SS.', required=True)
    parser.add_argument('--cadence', type=int, help='sample cadence in hours', required=False, default=None)
    parser.add_argument('--margin', type=int, help='time margin in hours', required=False, default=1)
    parser.add_argument('--overwrite', action='store_true', help='overwrite existing files', required=False, default=False)
    parser.add_argument('--instrument', type=str, help='instrument to download.', required=False, default='PHI')
    parser.add_argument('--product', type=str, help='product to download.', required=False, default='phi-fdt-blos,phi-hrt-blos')
    parser.add_argument('--level', type=int, help='data level', required=False, default=2)
    args = parser.parse_args()

    dataroot = Path(args.ds_path)
    products = [ds for ds in args.product.split(',')]
    [(dataroot/str(args.level)/ds).mkdir(exist_ok=True, parents=True) for ds in products]

    start_date = datetime.strptime(args.start_date, '%Y-%m-%dT%H:%M:%S')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%dT%H:%M:%S')
    tr = a.Time(start_date, end_date)
    
    logger.info(tr)
    for ds in products:
        logger.info(ds)
        search = Fido.search(
            tr,
            a.Instrument(args.instrument),
            a.soar.Product(ds),
            a.Level(args.level),
        )

        if args.cadence is not None:
            dates = pd.date_range(
                start=tr.start.datetime, 
                end=tr.end.datetime, 
                freq=pd.Timedelta(f'{args.cadence}h')
            )
            ts = pd.to_datetime(
                pd.Series(search['soar']['Start time'])
            )
            # Find the closest file to each date within the margin
            indices = []
            for d in dates:
                fs = ts[ts.sub(d).abs() < pd.Timedelta(args.margin, 'h')]
                if len(fs) > 0:
                    idx = fs.sub(d).abs().idxmin()
                    indices.append(idx)
            search = search['soar'][indices]
        else:
            search = search['soar']

        n_found_files = len(search)
        logger.info(f'Found {n_found_files}')
        Fido.fetch(search, path=dataroot/str(args.level)/ds, overwrite=args.overwrite)
        logger.info(f'Finished {tr}')