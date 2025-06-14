ds_path="F:/dataset/sdo/hmi"
email="mgjeon@khu.ac.kr"
start_year=2011
end_year=2024
series="M_720s"
segment="magnetogram"
python -m heliodata.download.sdo_hmi --ds_path $ds_path --email $email --series $series --segment $segment --start_year $start_year --end_year $end_year