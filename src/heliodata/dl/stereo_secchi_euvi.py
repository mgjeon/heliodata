import re
import logging
import argparse
import multiprocessing
from pathlib import Path
from datetime import datetime

import pandas as pd
from astropy.io import fits

from heliodata.dl.util import download_url, get_bs, get_timedelta, get_t_start_from_log


class STEREOEUVIDownloader:
    """
    Class to download STEREO/SECCHI EUVI data from SSC.

    Products:
        https://secchi.lmsal.com/EUVI/

        EUVI 171 Å, 195 Å, 284 Å, 304 Å

    Args:
        ds_path (str): Path to the directory where the downloaded data should be stored.
        n_workers (int): Number of worker threads for parallel download.
        wavelengths (list): List of wavelengths to download.
        quality_check (bool): If True, check whether header['QUALITY'] is 0 before downloading.
    """
    def __init__(self, ds_path, n_workers=1,
                 wavelengths=[171, 195, 284, 304],
                 quality_check=True):
        self.ds_path = ds_path
        self.n_workers = n_workers
        self.quality_check = quality_check

        self.wavelengths = wavelengths
        dirs = [str(wl) for wl in self.wavelengths]
        [(Path(ds_path) / "a" / wl).mkdir(parents=True, exist_ok=True) for wl in dirs]
        [(Path(ds_path) / "b" / wl).mkdir(parents=True, exist_ok=True) for wl in dirs]

        root = "https://stereo-ssc.nascom.nasa.gov/data/ins_data/secchi/L0_YMD/"
        self.root_a = root + "a/img/euvi/"
        self.root_b = root + "b/img/euvi/"

        logging.basicConfig(level=logging.INFO, 
                            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", force=True, 
                            handlers=[logging.FileHandler(f"{ds_path}/info.log"), logging.StreamHandler()])
        self.logger = logging.getLogger('STEREOEUVIDownloader')

    def download(self, sample):
        """
        Download the data from SSC.

        Args:
            sample (pandas Series): pandas Series containing the obstime, wavelength, source, and url.

        Returns:
            str: Path to the downloaded file.
        """
        dir = Path(self.ds_path) / sample.source / str(sample.wavelength)
        # t = sample.obstime
        # tt = t.isoformat('T', timespec='seconds').replace(':', '')
        tt = sample.dateobs
        fits_path = dir / f"{tt}.fits"
        if fits_path.exists():
            return fits_path
        download_url(
            sample.url, 
            filename=fits_path, 
            desc=str(sample.source).upper() + " " + str(sample.wavelength)
        )
        return fits_path
    
    @staticmethod
    def get_idx(fts_list, date):
        # find the first index of the fts file that has the same hour as the date
        for i, f in enumerate(fts_list):
            obstime = datetime.strptime(f.get('href')[:15], "%Y%m%d_%H%M%S")
            if obstime.hour == date.hour:
                return i
    
    def get_data(self, stereo_url, fts_list, source):
        # Create url list until all possible wavelengths are found.
        possible_values = set(self.wavelengths)
        seen_values = set()

        data = []
        for f in fts_list:
            url = stereo_url + f.get('href')
            header = fits.getheader(url)
            if self.quality_check:
                if header['NAXIS1'] < 2048 or header['NAXIS2'] < 2048 or header['NMISSING'] != 0:
                    self.logger.info(f"Invalid file ({source.upper()}): {f.get('href')}")
                    self.logger.info(f"NAXIS1: {header['NAXIS1']} NAXIS2: {header['NAXIS2']} NMISSING: {header['NMISSING']}")
                    continue

            info = {}
            info['obstime'] = datetime.strptime(f.get('href')[:15], "%Y%m%d_%H%M%S")
            info['dateobs'] = datetime.strptime(header['DATE-OBS'], "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y%m%d_%H%M%S")
            info['wavelength'] = header['WAVELNTH']
            info['source'] = source
            info['url'] = url
            data.append(info)

            seen_values.add(int(header['WAVELNTH']))
            if possible_values == possible_values.intersection(seen_values):
                break
        return data
    
    def get_queue(self, date, source="a"):
        queue = []
        
        d = datetime.strftime(date, "%Y/%m/%d/")
        if source == "a":
            stereo_url = self.root_a + d
        elif source == "b":
            stereo_url = self.root_b + d

        bs = get_bs(stereo_url)
        if bs:
            fts_re = re.compile(datetime.strftime(date, "%Y%m%d") + ".*n4.*.fts")
            fts_list = bs.find_all('a', {'href': fts_re})
            if len(fts_list) > 0:
                i = self.get_idx(fts_list, date)
                fts_list = fts_list[i:]
            # self.logger.info(f"Found {len(fts_list)} files for STEREO {source.upper()}")
            data = self.get_data(stereo_url, fts_list, source)
        else:
            self.logger.info(f"No files found for STEREO {source.upper()}")
            return queue
        
        queue = []
        df = pd.DataFrame(data)
        for w in self.wavelengths:
            df_w = df[df['wavelength'] == w].sort_values(by='obstime').reset_index(drop=True)
            queue.append(df_w.iloc[0])
        return queue

    def downloadDate(self, date):
        """
        Download the data for the given date.

        Args:
            date (datetime): The date for which the data should be downloaded.

        Returns:
            list: List of paths to the downloaded files.
        """
        id = date.isoformat()
        # self.logger.info(f"Start download: {t}")

        try:
            stereo_a_queue = self.get_queue(date, source="a")
            stereo_b_queue = self.get_queue(date, source="b")
            queue = stereo_a_queue + stereo_b_queue

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
    import warnings; warnings.filterwarnings("ignore")

    parser = argparse.ArgumentParser(description='Download STEREO/SECCHI EUVI data from SSC')
    parser.add_argument('--ds_path', type=str, help='path to the download directory.')
    parser.add_argument('--n_workers', type=int, help='number of parallel threads.', required=False, default=1)
    parser.add_argument('--wavelengths', type=str, help='wavelengths to download.', required=False, default="171,195,284,304")
    parser.add_argument('--quality_check', action='store_true', help='perform quality check before downloading.', required=False, default=False)

    parser.add_argument('--start_date', type=str, help='start date in format YYYY-MM-DD.')
    parser.add_argument('--end_date', type=str, help='end date in format YYYY-MM-DD.', required=False, default=str(datetime.now()).split(' ')[0])
    parser.add_argument('--cadence', type=str, help='cadence for the download.', required=False, default="1days")

    wavelengths = [int(wl) for wl in parser.parse_args().wavelengths.split(',')]
    print(wavelengths)

    args = parser.parse_args()
    
    log_file = Path(args.ds_path) / 'info.log'
    t_start = get_t_start_from_log(log_file)
    if t_start is None:
        t_start = datetime.strptime(args.start_date, "%Y-%m-%d")

    t_end = datetime.strptime(args.end_date, "%Y-%m-%d")
    td = get_timedelta(args.cadence)
    date_list = [t_start + i * td for i in range((t_end - t_start) // td)]

    downloader = STEREOEUVIDownloader(
        ds_path=args.ds_path, 
        n_workers=args.n_workers,
        wavelengths=wavelengths,
        quality_check=args.quality_check
    )

    for d in date_list:
        downloader.downloadDate(d)