import json
import argparse
from pathlib import Path

from loguru import logger

import astropy.units as u
from sunpy.net import Fido, attrs as a


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Download SDO/AIA EUV data from JSOC')

    parser.add_argument('--ds_path', type=str, help='path to the download directory.', required=True)
    parser.add_argument('--wavelengths', type=str, help='wavelengths to download.', required=False, default="094,131,171,193,211,304,335")

    parser.add_argument('--start_year', type=int, help='start year in format YYYY.', required=False, default=2011)
    parser.add_argument('--end_year', type=int, help='end year in format YYYY.', required=False, default=2024)
    parser.add_argument('--cadence', type=int, help='sample cadence in hours', required=False, default=24)

    parser.add_argument('--email', type=str, help='email address for JSOC.', required=True)

    args = parser.parse_args()

    dataroot = Path(args.ds_path)
    logger.add(dataroot / 'info.log')
    logger.info(args.ds_path)
    logger.info(args.wavelengths)
    logger.info(args.start_year)
    logger.info(args.end_year)
    logger.info(args.cadence)
    logger.info(args.email)
    logger.info('-'*20)

    wavelengths = [wl for wl in args.wavelengths.split(',')]
    [(dataroot/wav).mkdir(exist_ok=True, parents=True) for wav in wavelengths]

    start_year = args.start_year
    end_year = args.end_year
    times = []
    year = start_year
    while year <= end_year:
        times.append(a.Time(f'{year}-01-01T00:00:00', f'{year}-12-31T23:59:59'))
        year = year + 1

    info_path = dataroot / 'info.json'
    if info_path.exists():
        with open(info_path, 'r') as f:
            info = json.load(f)
    else:
        info = {}
        for tr in times:
            info[str(tr)] = {}
            for wav in wavelengths:
                info[str(tr)][wav] = None

    for tr in times:
        logger.info(tr)
        for wav in wavelengths:
            logger.info(wav)

            n_found_files = info[str(tr)][wav]

            res_path = dataroot/wav/str(tr.start.datetime.year)
            res_path.mkdir(exist_ok=True, parents=True)
            n_exist_files = len(list((res_path).glob('*.fits')))

            if (n_found_files is None) or (n_found_files != n_exist_files):
                search = Fido.search(
                    tr,
                    a.jsoc.Series('aia.lev1_euv_12s'),
                    a.jsoc.Segment('image'),
                    a.jsoc.Notify('mgjeon@khu.ac.kr'),
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