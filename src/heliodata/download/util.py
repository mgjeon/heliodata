import re
from sunpy.net import attrs as a
from datetime import datetime, timedelta
from tqdm import tqdm
from urllib.request import urlopen, urlretrieve
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup


def get_times(start_year, end_year, interval):
    """
    Generate a list of time ranges based on the specified interval.
    """
    from astropy.time import Time
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


def get_respath(resroot, tr, interval):
    """
    Get the result path based on the time range and interval.
    """
    if interval == 'year':
        respath = resroot / str(tr.start.datetime.year)

    elif interval == 'month':
        respath = resroot / str(tr.start.datetime.year) / str(tr.start.datetime.month)

    respath.mkdir(exist_ok=True, parents=True)
    return respath
    

class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""
    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        return self.update(b * bsize - self.n)  # also sets self.n = b * bsize
    
def download_url(url, filename, desc=None):
    desc = url.split('/')[-1] if desc is None else desc
    with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1,
                  desc=desc) as t:  # all optional kwargs
        urlretrieve(url, filename=filename,
                            reporthook=t.update_to, data=None)
        t.total = t.n


def get_bs(url):
    try:
        html = urlopen(url)
    except HTTPError as e:
        # print(e)
        return None
    except URLError as e:
        # print("The server could not be found!")
        return None
    else:
        bs = BeautifulSoup(html.read(), 'html.parser')
        return bs
    

def get_t_start_from_log(log_file):
    if log_file.exists():
        with open(log_file, 'r') as f:
                lines = f.readlines()

        matches = re.findall(r"(?<=Finished )\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "\n".join(lines))
        if matches:
            last_finished = matches[-1]
            parsed_date = datetime.fromisoformat(last_finished)
            print('Last downloaded date:', parsed_date)
            t_start = parsed_date.replace(hour=0)
        else:
            t_start = None
    else:
        t_start = None
    return t_start


def parse_cadence(cadence):
    match = re.match(r"(\d+)\s*(\D+)", cadence)
    if match:
        value = int(match.group(1))
        unit = match.group(2).strip()
    return value, unit


def get_timedelta(cadence):
    value, unit = parse_cadence(cadence)
    if unit == "days":
        return timedelta(days=value)
    elif unit == "hours":
        return timedelta(hours=value)
    elif unit == "minutes":
        return timedelta(minutes=value)
    else:
        raise ValueError("Invalid unit. Only days, hours, and minutes are supported.")