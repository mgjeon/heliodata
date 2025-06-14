import json
import argparse
from pathlib import Path

import astropy.units as u
from astropy.time import Time
import sunpy_soar
from sunpy.net import Fido, attrs as a
from loguru import logger

from heliodata.download.util import get_times, get_respath


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Download STEREO/SECCHI/EUVI data from SSC')

    parser.add_argument('--ds_path', type=str, help='path to the download directory.', required=True)
    parser.add_argument('--start_year', type=int, help='start year in format YYYY.', required=False, default=2010)
    parser.add_argument('--end_year', type=int, help='end year in format YYYY.', required=False, default=2024)
    parser.add_argument('--cadence', type=int, help='sample cadence in hours', required=False, default=24)
    parser.add_argument('--ignore_info', action='store_true', help='ignore info.json file', required=False, default=False)
    parser.add_argument('--interval', choices=['year', 'month'], default='month',
                        help='interval for the time range, either year or month.', required=False)

    parser.add_argument('--wavelengths', type=str, help='wavelengths to download.', required=False, default="171,195,284,304")

    args = parser.parse_args()
    
    dataroot = Path(args.ds_path)
    logger.add(dataroot / 'info.log')
    logger.info(vars(args))
    logger.info('-'*20)

    wavelengths = [wl for wl in args.wavelengths.split(',')]
    [(dataroot/'a'/wav).mkdir(exist_ok=True, parents=True) for wav in wavelengths]
    [(dataroot/'b'/wav).mkdir(exist_ok=True, parents=True) for wav in wavelengths]

    times = get_times(args.start_year, args.end_year, args.interval)

    stereo = ['STEREO_A', 'STEREO_B']
    s2p = {'STEREO_A': 'a', 'STEREO_B': 'b'}

    info_path = dataroot / 'info.json'
    if info_path.exists() and not args.ignore_info:
        with open(info_path, 'r') as f:
            info = json.load(f)
    else:
        info = {}
        for tr in times:
            info[str(tr)] = {}
            for s in stereo:
                info[str(tr)][s] = {}
                for wav in wavelengths:
                    info[str(tr)][s][wav] = None

    for tr in times:
        if str(tr) not in info:
            info[str(tr)] = {}
        logger.info(tr)
        for s in stereo:
            if s == 'STEREO_B' and tr.start > Time('2014-10-01'):
                continue
            logger.info(s)
            for wav in wavelengths:
                logger.info(wav)

                try:
                    n_found_files = info[str(tr)][s][wav]
                except KeyError:
                    info[str(tr)] = {}
                    info[str(tr)][s] = {}
                    info[str(tr)][s][wav] = None
                    n_found_files = None

                res_path = get_respath(dataroot/s2p[s]/wav, tr, args.interval)
                n_exist_files = len(list((res_path).glob('*.fts')))

                if (n_found_files is None) or (n_found_files != n_exist_files):
                    search = Fido.search(
                        tr,
                        a.Provider('SSC'),
                        a.Instrument('EUVI'),
                        a.Source(s),
                        a.Wavelength(int(wav)*u.AA),
                        a.Sample(int(args.cadence)*u.hour),
                    )
                    if len(search) == 0:
                        n_found_files = 0
                    elif len(search['vso']) > 0:
                        n_found_files = len(search['vso'])
                    info[str(tr)][s][wav] = n_found_files
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