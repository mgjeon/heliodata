ds_path="F:/dataset/sdo_hmi"
email="mgjeon@khu.ac.kr"
series="M_720s"
segment="magnetogram"
python -m heliodata.download.sdo_hmi --ds_path $ds_path --email $email --series $series --segment $segment