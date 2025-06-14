ds_path="F:/dataset/solo/phi/hrt"
product="phi-hrt-blos"
margin=12
cadence=24
python -m heliodata.download.solo_phi --ds_path $ds_path --product $product --margin $margin --cadence $cadence