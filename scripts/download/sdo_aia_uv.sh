ds_path="F:/dataset/sdo_aia/uv"
email="mgjeon@khu.ac.kr"
series="uv_24s"
wavelengths="1600,1700"
python -m heliodata.download.sdo_aia --ds_path $ds_path --email $email --series $series --wavelengths $wavelengths