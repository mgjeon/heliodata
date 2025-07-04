ds_path="F:/dataset/sdo_month/aia/euv"
email="mgjeon@khu.ac.kr"
series="euv_12s"
wavelengths="094,131,171,193,211,304,335"
cadence=6
interval='month'
start_year=2011
end_year=2024
python -m heliodata.download.sdo_aia --ds_path $ds_path --email $email --series $series --wavelengths $wavelengths --cadence $cadence --interval $interval --start_year $start_year --end_year $end_year