import drms
import shutil
from pathlib import Path
import argparse

#------------------------------------------------------------

email = 'mgjeon@khu.ac.kr'
obstime = '2016-01-01T00:00:00'
target_path = Path('./test')

q_euv = f'aia.lev1_euv_12s[{obstime}][94,131,171,193,211,304,335]'+'{image}'
q_uv = f'aia.lev1_uv_24s[{obstime}][1600,1700]'+'{image}'
q_vis = f'aia.lev1_vis_1h[{obstime}][4500]'+'{image}'
q_hmi_mag = f'hmi.m_720s[{obstime}]'+'{image}'

#------------------------------------------------------------

q = q_euv 

#------------------------------------------------------------

target_path.mkdir(parents=True, exist_ok=True)
c = drms.Client(email=email)
r = c.export(q, method='url', protocol='fits')
r.wait()  
if r.status == 0:
    r.download('.')
files = list(Path('.').glob('*.fits'))
for f in files:
    shutil.move(f, target_path / f.name)