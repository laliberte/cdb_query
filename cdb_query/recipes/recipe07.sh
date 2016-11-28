#!/bin/bash
#Change to set number of processes to use:
NUM_PROCS=10

#Discover, validate and select only tropospheric pressure levels:
echo $PASSWORD_ESGF | cdb_query CREATEIP ask validate reduce_soft_links download_opendap reduce \
                    --openid=$OPENID_ESGF \
                    --password_from_pipe \
                    --num_procs=${NUM_PROCS} \
                    --log_files \
                    --debug \
                    --institute=NASA-GMAO \
                    --model=GEOS-5 \
                    --experiment=MERRA-2 \
                    --year=1980 --month=01 --day=01 \
                    --num_dl=3 \
                    -k time_frequency -k var \
                    -l year -l month -l day \
                    --ask_var=tas:6hr-atmos,hus:6hr-atmos \
                    --reduce_soft_links_script='nc4sl subset --lonlatbox 0.0 360.0 30.0 90.0' \
                    '' \
                    --out_destination='./out_test/' \
                    reanalyses_pointers_test.nc

rm -r ./out_test/
rm reanalyses_pointers_test.nc
