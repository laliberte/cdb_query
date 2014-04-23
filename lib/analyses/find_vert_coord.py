from netCDF4 import Dataset
import numpy as np

def replicate_netcdf_file(output,data):
    #This function replicates a netcdf file
    for att in data.ncattrs():
        att_val=getattr(data,att)
        if 'encode' in dir(att_val):
            att_val=att_val.encode('ascii','replace')
        setattr(output,att,att_val)
    output.sync()
    return output

def replicate_netcdf_var(output,data,var):
    #This function replicates a netcdf variable 
    for dims in data.variables[var].dimensions:
        if dims not in output.dimensions.keys():
            output.createDimension(dims,len(data.dimensions[dims]))
            dim_var = output.createVariable(dims,data.variables[dims].type,(dims,))
            dim_var[:] = data.variables[dims][:]
            output = replicate_netcdf_var(output,data,dims)

    if var not in output.variables.keys():
        output.createVariable(var,data.variables[var].type,data.variables[var].dimensions,zlib=True)
    for att in data.variables[var].ncattrs():
        att_val=getattr(data.variables[var],att)
        if att[0]!='_':
            if 'encode' in dir(att_val):
                att_val=att_val.encode('ascii','replace')
            setattr(output.variables[var],att,att_val)
    output.sync()
    return output

def convert_hybrid(options):
    data=Dataset(options.in_file)

    formulas=dict()
    for lev_id in ['','_bnds']: 
        level='lev'+lev_id
        formulas[level]=data.variables[level].formula
        terms=[item.replace(":","") for item in data.variables[level].formula_terms.split(" ")]
        target=formulas[level].split('=')[0].replace(' ','')
        terms.append(target)
        if lev_id=='':
            terms.append(target+lev_id+'[$time,$lev,$lat,$lon]')
        elif lev_id=='_bnds':
            terms.append(target+lev_id+'[$time,$lev,$lat,$lon,$bnds]')
        for var_id,var in enumerate(zip(terms[::2],terms[1::2])):
            formulas[level]=formulas[level].replace(var[0],'%'+str(var_id)+'%')
        for var_id,var in enumerate(zip(terms[::2],terms[1::2])):
            formulas[level]=formulas[level].replace('%'+str(var_id)+'%',var[1])
    print ';'.join([formulas['lev'],
                    '*'+formulas['lev_bnds'],
                    'd'+target+'[$time,$lev,$lat,$lon]='+target+'_bnds(:,:,:,:,1)-'+target+'_bnds(:,:,:,:,0);'])

    return


def main():
    import argparse 
    import textwrap

    #Option parser
    description=textwrap.dedent('''\
    This script finds the mid-level pressure, mid-level geopotential
    and layer mass on model level.
    The input contains:
    ta, temperature
    hus, specific humidity
    on the same grid at mid-level.
    It must also contain:
    ps, surface pressure
    orog, orography
    ''')
    epilog='Frederic Laliberte, Paul Kushner 01/2014'
    version_num='0.1'
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            version='%(prog)s '+version_num,
                            epilog=epilog)
    #input_arguments(parser)
    subparsers = parser.add_subparsers(help='commands',dest='command')

    convert_parser=subparsers.add_parser('convert_hybrid',
                                           help='This function takes CMOR output hybrid coordinates and create CDO-compliant hybrid coordinates.',
                                           epilog=epilog,
                                           formatter_class=argparse.RawTextHelpFormatter)
    convert_parser.add_argument('in_file',
                                 help='Input file')
    options=parser.parse_args()

    if options.command=='convert_hybrid':
        convert_hybrid(options)
        
    return 

def input_arguments(parser):
    parser.add_argument('in_file',
                                 help='Input file')
    parser.add_argument('out_file',
                                 help='Output file')
    return

if __name__ == "__main__":
    main()
