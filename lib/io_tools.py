import json
import gzip
import netCDF4

def open_json(options):
    infile=open(options.in_diagnostic_headers_file,'r')
    paths_dict=json.load(infile)
    return paths_dict

def close_json(paths_dict,options):
    outfile = open(options.out_diagnostic_headers_file,'w')

    json.dump({'pointers':paths_dict.pointers.tree,'header':paths_dict.header},outfile,
              sort_keys=True,indent=4, separators=(',', ': '))
    outfile.close()
    return

def open_netcdf(options,project_drs):
    infile=netCDF4.Dataset(options.in_diagnostic_netcdf_file,'r')
    paths_dict=dict()
    paths_dict['header']=dict()
    for att in set(project_drs.header_desc).intersection(infile.ncattrs()):
        paths_dict['header'][att]=json.loads(infile.getncattr(att))
    infile.close()
    return paths_dict
