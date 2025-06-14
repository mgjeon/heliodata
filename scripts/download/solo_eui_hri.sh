ds_path="F:/dataset/solo/eui/hri"
product="eui-hrieuv174-image,eui-hrilya1216-image"
margin=1
cadence=1
python -m heliodata.download.solo_eui --ds_path $ds_path --product $product --margin $margin --cadence $cadence