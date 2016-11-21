#!\bin\bash

function inspectlogs {
    if [ ! -f $1 ]; then
        exit 1
    fi
    if [ ! -f $1.log ]; then
        exit 1
    fi
    if [ $(cat $1.log | grep ERROR | wc -l) -gt 0 ]; then
        cat $1.log | grep ERROR
        exit 1
    fi
}

#Create new cdo grid:
cat >> newgrid_atmos.cdo <<EndOfGrid
gridtype  = lonlat
gridsize  = 55296
xname     = lon
xlongname = longitude
xunits    = degrees_east
yname     = lat
ylongname = latitude
yunits    = degrees_north
xsize     = 288
ysize     = 192
xfirst    = 0
xinc      = 1.25
yfirst    = -90
yinc      = 0.94240837696
EndOfGrid


#latlon box -124.78 -66.95 24.74 49.34 is continental us
echo $PASSWORD_ESGF | cdb_query CMIP5 ask validate record_validate reduce_soft_links download_opendap reduce \
                  --debug \
                  --log_files \
                  --ask_month=3,4,5 \
                  --ask_var=tas:mon-atmos-Amon,pr:mon-atmos-Amon,orog:fx-atmos-fx \
                  --ask_experiment=historical:1950-2005,rcp85:2006-2030,rcp85:2050-2060 \
                  --related_experiments \
                  --Xdata_node=http://esgf2.dkrz.de \
                  --openid=$OPENID_ESGF \
                  --model=ACCESS1.0 \
                  --password_from_pipe \
                  --out_destination=./out_sample/CMIP5/ \
                  --num_procs=3 \
                  --year=2000 --month=3 \
                  --reduce_soft_links_script='nc4sl subset --lonlatbox -150.0 -50.0 20.0 55.0' \
                  'cdo -f nc \
                       -sellonlatbox,-124.78,-66.95,24.74,49.34 \
                       -remapbil,newgrid_atmos.cdo \
                       -selgrid,lonlat,curvilinear,gaussian,unstructured ' \
                  us_pr_tas_MAM_pointers.validate.200003.retrieved.converted.nc
#Testing check: 
inspectlogs us_pr_tas_MAM_pointers.validate.200003.retrieved.converted.nc

#Do only first subset:
exit

echo $PASSWORD_ESGF | cdb_query CMIP5 reduce_soft_links download_opendap reduce \
                  --debug \
                  --log_files \
                  --openid=$OPENID_ESGF \
                  --password_from_pipe \
                  --out_destination=./out/CMIP5/ \
                  --num_procs=10 \
                  --reduce_soft_links_script='nc4sl subset --lonlatbox -150.0 -50.0 20.0 55.0' \
                  'cdo -f nc \
                       -sellonlatbox,-124.78,-66.95,24.74,49.34  \
                       -remapbil,newgrid_atmos.cdo \
                       -selgrid,lonlat,curvilinear,gaussian,unstructured ' \
                  us_pr_tas_MAM_pointers.validate.200003.retrieved.converted.nc.validate \
                  us_pr_tas_MAM_pointers.validate.retrieved.converted.nc
#Testing check: 
inspectlogs us_pr_tas_MAM_pointers.validate.retrieved.converted.nc

rm us_pr_tas_MAM_pointers*
rm -r out/
