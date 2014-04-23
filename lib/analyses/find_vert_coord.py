from netCDF4 import Dataset
import numpy as np
import re
import subprocess

def check_file_consistency(data,var_list):
    if not set(data.groups.keys()).issuperset(var_list.keys()):
        raise ValueError('input file must have groups '+','.join(var_list.keys()))

    for var in var_list.keys():
        if not var in data.groups[var].variables.keys():
            raise ValueError('input file group {0} must contain variable {0}'.format(var))
        if var_list[var]!=data.groups[var].variables[var].dimensions:
            raise ValueError('input file group {0} variable {0} must have dimensions ({1})'.format(var,','.join(var_list[var])))
    return

def convert_hybrid(options):
    data=Dataset(options.in_file)
    print data
    import time
    time.sleep(1000)

    var_list={
                'ta':('time','lev','lat','lon'),
                'hus':('time','lev','lat','lon'),
                'ps':('time','lat','lon')
                }

    check_file_consistency(data,var_list)

    data_grp=data.groups['ta']

    formulas=dict()
    for lev_id in ['','_bnds']: 
        level='lev'+lev_id
        formulas[level]=re.sub(r'\(.*?\)', '',data_grp.variables[level].formula)
        terms=[item.replace(":","") for item in data_grp.variables[level].formula_terms.split(" ")]
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
    data.close()
    first_target=';'.join([formulas['lev'],
                    '*'+formulas['lev_bnds'],
                    'd'+target+'[$time,$lev,$lat,$lon]='+target+'_bnds(:,:,:,:,1)-'+target+'_bnds(:,:,:,:,0);'])

    if target=='p':
        second_target='dz=287.04*(1+0.61)*ta*dp/p'
    elif target=='z':
        second_target='*dlnp=dz/(287.04*(1+0.61)*ta);*lnp_bnds=z_bnds;lnp_bnds=0.0;'
        second_target+='lnp_bnds(:,0,:,:,0)=log(ps);for(*iz=0;iz<$lev.size-2;iz++){'
        second_target+='lnp_bnds(:,iz,:,:,1)=lnp_bnds(:,iz,:,:,0)+dlnp(:,iz,:,:);'
        second_target+='lnp_bnds(:,iz+1,:,:,0)=lnp_bnds(:,iz,:,:,1);'
        second_target+='};'
        second_target+='lnp_bnds(:,$lev.size-1,:,:,1)=lnp_bnds(:,$lev.size-1,:,:,0)+dlnp(:,$lev.size-1,:,:);'
        second_target+='dp=dlnp; dp=exp(lnp_bnds(:,:,:,:,1))-exp(lnp_bnds(:,:,:,:,0));'

    for var in var_list.keys():
        script_to_call='ncrcat -3 -G : -g '+ var + ' -A ' +' '.join([options.in_file,options.out_file])
        out=subprocess.call(script_to_call,shell=True)

    #script_to_call='ncap2 -O -s \''+first_target+second_target+'\' '+options.out_file+' '+options.out_file
    script_to_call='ncap2 -O -s \''+'bnds=bnds.double();'+'\' '+options.out_file+' '+options.out_file
    out=subprocess.call(script_to_call,shell=True)
    script_to_call='ncap2 -3 -O -s \''+first_target+'\' '+options.out_file+' '+options.out_file
    print script_to_call
    out=subprocess.call(script_to_call,shell=True)
    import time
    time.sleep(1000)

    output=Dataset(options.out_file)
    output.close()
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
    input_arguments(convert_parser)
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
