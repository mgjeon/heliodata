import json
import argparse
from pathlib import Path

import astropy.units as u
from sunpy.net import Fido, attrs as a
from loguru import logger

from heliodata.download.util import get_times, get_respath


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Download SDO/AIA data from JSOC')

    parser.add_argument('--ds_path', type=str, help='path to the download directory.', required=True)
    parser.add_argument('--start_year', type=int, help='start year in format YYYY.', required=False, default=2010)
    parser.add_argument('--end_year', type=int, help='end year in format YYYY.', required=False, default=2024)
    parser.add_argument('--cadence', type=int, help='sample cadence in hours', required=False, default=24)
    parser.add_argument('--ignore_info', action='store_true', help='ignore info.json file', required=False, default=False)
    parser.add_argument('--interval', choices=['year', 'month'], default='year',
                        help='interval for the time range, either year or month.', required=False)

    parser.add_argument('--email', type=str, help='email address for JSOC.', required=True)
    parser.add_argument('--series', choices=['euv_12s', 'uv_24s', 'vis_1h'], required=False, default='euv_12s')
    parser.add_argument('--segment', choices=['image', 'spike'], required=False, default='image')
    parser.add_argument('--wavelengths', type=str, help='wavelengths to download.', required=False, default='094,131,171,193,211,304,335')

    args = parser.parse_args()

    dataroot = Path(args.ds_path)
    logger.add(dataroot / 'info.log')
    logger.info(vars(args))
    logger.info('-'*20)

    wavelengths = [wl for wl in args.wavelengths.split(',')]
    [(dataroot/wav).mkdir(exist_ok=True, parents=True) for wav in wavelengths]

    times = get_times(args.start_year, args.end_year, args.interval)

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

    for tr in times:
        if str(tr) not in info:
            info[str(tr)] = {}
        logger.info(tr)
        for wav in wavelengths:
            logger.info(wav)
            
            try:
                n_found_files = info[str(tr)][wav]
            except KeyError:
                info[str(tr)][wav] = None
                n_found_files = None

            res_path = get_respath(dataroot/wav, tr, args.interval)
            n_exist_files = len(list((res_path).glob('*.fits')))

            if (n_found_files is None) or (n_found_files != n_exist_files):
                search = Fido.search(
                    tr,
                    a.jsoc.Series(f'aia.lev1_{args.series}'),
                    a.jsoc.Segment(args.segment),
                    a.jsoc.Notify(args.email),
                    a.Wavelength(int(wav)*u.AA),
                    a.Sample(int(args.cadence)*u.hour),
                )
                if len(search) == 0:
                    n_found_files = 0
                elif len(search['jsoc']) > 0:
                    n_found_files = len(search['jsoc'])
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