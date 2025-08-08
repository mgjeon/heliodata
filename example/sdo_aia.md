```
pip install git+https://github.com/mgjeon/heliodata
```


- SDO/AIA L1 EUV



```
python -m heliodata.download.sdo_aia `
--temp <temporary directory>         `
--root <root directory>              `
--start <start time>                 `
--end <end time>                     `
--cadence <cadence>                  `
--series <series>                    `
--wavelengths <wavelengths>
```
