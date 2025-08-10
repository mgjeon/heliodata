import argparse
import pandas as pd
from datetime import datetime
import itertools
from pathlib import Path
from tqdm import tqdm
import shutil

import drms

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', default='sdo_aia.parquet')
    parser.add_argument('--start', default='2011-01-01T00:00:00')
    parser.add_argument('--end',   default='2025-01-01T00:00:00')  # exclusive
    parser.add_argument('--cadence',  default='24h')
    parser.add_argument('--series', default='euv_12s')
    parser.add_argument('--segments', default='image')
    parser.add_argument('--wavelengths', default='94,131,171,193,211,304,335')
    args = parser.parse_args()

    CSV_FILE = Path(args.file)

    t_start = args.start
    t_end   = args.end
    dt_start = datetime.strptime(t_start, '%Y-%m-%dT%H:%M:%S')
    dt_end   = datetime.strptime(t_end,   '%Y-%m-%dT%H:%M:%S')
    times = []
    for t in pd.date_range(dt_start, dt_end, freq='24h', inclusive='left'):
        times.append(t)

    if CSV_FILE.exists():
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = CSV_FILE.with_name(f'sdo_aia_{timestamp}.parquet')
        shutil.copy2(CSV_FILE, backup_file)
        df = pd.read_parquet(CSV_FILE)
    else:
        times_str = [t.strftime('%Y-%m-%dT%H:%M:%S') for t in times]
        series = args.series
        segments = args.segments
        wavelengths = args.wavelengths
        df = pd.DataFrame(
            itertools.product(times_str, series.split(','), segments.split(','), wavelengths.split(',')),
            columns=['obstime', 'series', 'segment', 'wavelength']
        )
        df['query'] = 'NONE'
        df.to_parquet(CSV_FILE, index=False)

    c = drms.Client()
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        q = f'aia.lev1_{row["series"]}[{row["obstime"]}][{row["wavelength"]}]' + '{' + f'{row["segment"]}' + '}'
        df.at[idx, 'query'] = q
        header, segment = c.query(q, key=drms.JsocInfoConstants.all, seg=row["segment"])
        df.at[idx, 'url'] = segment[row["segment"]].iloc[0]
        if idx == 0:
            df = pd.concat([df, header], axis=1)
        else:
            for col in header.columns:
                df.at[idx, col] = header[col].iloc[0]

        if idx % 10 == 0:
            df.to_parquet(CSV_FILE, index=False)
    df.to_parquet(CSV_FILE, index=False)