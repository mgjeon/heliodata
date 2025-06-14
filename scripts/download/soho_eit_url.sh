ds_path="F:/dataset/soho/eit"
start_date="2010-01-01"
end_date="2025-01-01"
cadence="1days"
python -m heliodata.download.soho_eit_url --ds_path $ds_path --start_date $start_date --end_date $end_date --cadence $cadence