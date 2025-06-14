ds_path="F:/dataset/solo/eui/fsi"
product="eui-fsi174-image,eui-fsi304-image"
margin=1
cadence=24
python -m heliodata.download.solo_eui --ds_path $ds_path --product $product --margin $margin --cadence $cadence