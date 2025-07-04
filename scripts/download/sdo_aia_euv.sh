ds_path="F:/dataset/sdo/aia/euv"
email="mgjeon@khu.ac.kr"
series="euv_12s"
wavelengths="094,131,171,193,211,304,335"
cadence=24
interval='year'
python -m heliodata.download.sdo_aia --ds_path $ds_path --email $email --series $series --wavelengths $wavelengths --cadence $cadence --interval $interval