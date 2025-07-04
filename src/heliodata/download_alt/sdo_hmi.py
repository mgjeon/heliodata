import logging
import argparse
import multiprocessing
from pathlib import Path
from datetime import datetime

import drms
import numpy as np
import pandas as pd
from astropy.io import fits
from sunpy.io._fits import header_to_fits
from sunpy.util import MetaDict

from sswdata.download.util import download_url


class SDOHMIDownloader:
    """
    Class to download SDO/HMI Continuum & Magnetogram data from JSOC.

    Products:
        http://jsoc.stanford.edu/ajax/lookdata.html
        
        hmi.Ic_720s
        HMI Continuum Intensity 6173 Å, 720s cadence

        hmi.M_720s
        HMI Line-of-sight Magnetograms, 720s cadence

    Args:
        ds_path (str): Path to the directory where the downloaded data should be stored.
        n_workers (int): Number of worker threads for parallel download.
        series (str): Series name of the HMI data.
        quality_check (bool): If True, check whether header['QUALITY'] is 0 before downloading.
    """
    def __init__(self, ds_path, n_workers=1, 
                 series=['Ic_720s', 'M_720s'],
                 quality_check=False):
        self.ds_path = ds_path
        self.n_workers = n_workers
        self.quality_check = quality_check

        self.series = series
        [(Path(ds_path) / s).mkdir(parents=True, exist_ok=True) for s in series]

        self.drms_client = drms.Client()

        logging.basicConfig(level=logging.INFO, 
                            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", force=True, 
                            handlers=[logging.FileHandler(f"{ds_path}/info.log"), logging.StreamHandler()])
        self.logger = logging.getLogger('SDOHMIDownloader')

    def set_dir_desc(self):
        header, segment, t = self.sample
        content = header['CONTENT']
        cadence = str(int(header['CADENCE'])) + 's'
        if content == "CONTINUUM INTENSITY":
            prefix = 'Ic_'
        elif content == "MAGNETOGRAM":
            prefix = 'M_'
        else:
            raise Exception(f'content {content} not supported!')

        s = prefix + cadence
        dir = Path(self.ds_path) / s
        desc = s
        return dir, desc
    
    def download(self, sample):
        """
        Download the data from JSOC.

        Args:
            sample (tuple): Tuple containing the header, segment and time information.

        Returns:
            str: Path to the downloaded file.
        """
        self.sample = sample    
        header, segment, t = sample
        dir, desc = self.set_dir_desc()
        try:
            tt = datetime.strptime(header['T_REC'], "%Y.%m.%d_%H:%M:%S_TAI").strftime("%Y%m%d_%H%M%S")
            map_path = dir / ('%s.fits' % tt)
            if map_path.exists():
                return map_path
            # load map
            url = 'http://jsoc.stanford.edu' + segment
            download_url(url, filename=map_path, desc=desc)

            header['DATE_OBS'] = header['DATE__OBS']
            header = header_to_fits(MetaDict(header))
            with fits.open(map_path, 'update') as f:
                hdr = f[1].header
                for k, v in header.items():
                    if pd.isna(v):
                        continue
                    hdr[k] = v
                f.verify('silentfix')

            return map_path
        except Exception as ex:
            raise ex  

    def downloadDate(self, date):
        """
        Download the data for the given date.

        Args:
            date (datetime): The date for which the data should be downloaded.

        Returns:
            list: List of paths to the downloaded files.
        """
        id = date.isoformat()
        # self.logger.info('Start download: %s' % id)

        time_param = '%sZ' % date.isoformat('_', timespec='seconds')
        queue = []
        for s in self.series:
            if s == 'Ic_720s':
                segment = 'continuum'
            elif s == 'M_720s':
                segment = 'magnetogram'
            else:
                raise Exception(f'series hmi.{s} not supported!')

            ds_hmi = 'hmi.%s[%s]' % (s, time_param) + '{%s}' % segment
            keys_hmi = self.drms_client.keys(ds_hmi)
            header_hmi, segment_hmi = self.drms_client.query(ds_hmi, key=','.join(keys_hmi), seg=segment)
            if self.quality_check:
                if len(header_hmi) != 1 or np.any(header_hmi.QUALITY != 0):
                    self.logger.info("Skipping: %s" % id)
                    return

            if segment == 'continuum':
                queue += [(h.to_dict(), seg, date) for (idx, h), seg in zip(header_hmi.iterrows(), segment_hmi.continuum)]
            elif segment == 'magnetogram':
                queue += [(h.to_dict(), seg, date) for (idx, h), seg in zip(header_hmi.iterrows(), segment_hmi.magnetogram)]
            else:
                raise Exception(f'segment {segment} not supported!')
            
        try:
            if self.n_workers > 1:
                with multiprocessing.Pool(self.n_workers) as p:
                    p.map(self.download, queue)
            else:
                for q in queue:
                    self.download(q)
            self.logger.info('Finished: %s' % id)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error('Failed: %s' % id)


if __name__ == '__main__':
    from sswdata.download.util import get_timedelta, get_t_start_from_log
    import warnings; warnings.filterwarnings("ignore")

    parser = argparse.ArgumentParser(description='Download SDO/HMI Continuum & Magnetogram data from JSOC')

    parser.add_argument('--ds_path', type=str, help='path to the download directory.')
    parser.add_argument('--n_workers', type=int, help='number of parallel threads.', required=False, default=1)
    parser.add_argument('--series', type=str, help='series name of the HMI LOS Magnetogram data.', required=False, default="Ic_720s,M_720s")
    parser.add_argument('--quality_check', action='store_true', help='perform quality check before downloading.', required=False, default=False)

    parser.add_argument('--start_date', type=str, help='start date in format YYYY-MM-DD.')
    parser.add_argument('--end_date', type=str, help='end date in format YYYY-MM-DD.', required=False, default=str(datetime.now()).split(' ')[0])
    parser.add_argument('--cadence', type=str, help='cadence for the download.', required=False, default="1days")
    
    series = [str(s) for s in parser.parse_args().series.split(',')]
    print(series)

    args = parser.parse_args()

    log_file = Path(args.ds_path) / 'info.log'
    t_start = get_t_start_from_log(log_file)
    if t_start is None:
        t_start = datetime.strptime(args.start_date, "%Y-%m-%d")
    
    t_end = datetime.strptime(args.end_date, "%Y-%m-%d")
    td = get_timedelta(args.cadence)
    date_list = [t_start + i * td for i in range((t_end - t_start) // td)]

    downloader = SDOHMIDownloader(
        ds_path=args.ds_path, 
        n_workers=args.n_workers,
        series=series,
        quality_check=args.quality_check
    )

    for d in date_list:
        downloader.downloadDate(d)