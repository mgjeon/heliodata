from sunpy.net import attrs as a
from datetime import datetime


def get_times(start_year, end_year, interval):
    """
    Generate a list of time ranges based on the specified interval.
    """
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