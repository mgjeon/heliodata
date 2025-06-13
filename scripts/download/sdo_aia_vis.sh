ds_path="F:/dataset/sdo_aia/vis"
email="mgjeon@khu.ac.kr"
series="vis_1h"
wavelengths="4500"
python -m heliodata.download.sdo_aia --ds_path $ds_path --email $email --series $series --wavelengths $wavelengths