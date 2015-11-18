from netCDF4 import Dataset
import numpy as np
import re
import subprocess
import shutil
import os

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

    var_list={
                'ta':('time','lev','lat','lon'),
                'hus':('time','lev','lat','lon'),
                'ps':('time','lat','lon'),
                'orog':('lat','lon')
                }

    check_file_consistency(data,var_list)

    data_grp=data.groups['ta']

    undesired_strings=['(n,k,j,i)','(k)','(n,j,i)','(k,j,i)','(j,i)']

    formulas=dict()
    for lev_id in ['','_bnds']: 
        level='lev'+lev_id
        #formulas[level]=re.sub(r'\(.*?\)', '',data_grp.variables[level].formula)
        formulas[level]=data_grp.variables[level].formula
        #print data_grp.model_id
        #print formulas[level]
        for string in undesired_strings:
            formulas[level]=formulas[level].replace(string,'')
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
                    'd'+target+'[$time,$lev,$lat,$lon]=('+target+'_bnds(:,:,:,:,1)-'+target+'_bnds(:,:,:,:,0)).float()',
                    'defdim("slev",$lev.size+1)',
                    'slev[$slev]=0.0',
                    'slev(0:$lev.size-1)=lev_bnds(0:$lev.size-1,0)',
                    'slev($lev.size)=lev_bnds($lev.size-1,1)'])+';'

    if target=='p':
        second_target='dz=(-287.04*(1+0.61)*ta*dp/p/9.8).float();'
        second_target+='*z_bnds=p_bnds;z_bnds=0.0;'
        second_target+='for(*it=0;it<$time.size-1;it++){z_bnds(it,0,:,:,0)=orog;};for(*iz=0;iz<$lev.size-2;iz++){'
        second_target+='z_bnds(:,iz,:,:,1)=z_bnds(:,iz,:,:,0)+dz(:,iz,:,:);'
        second_target+='z_bnds(:,iz+1,:,:,0)=z_bnds(:,iz,:,:,1);'
        second_target+='};'
        second_target+='z_bnds(:,$lev.size-1,:,:,1)=z_bnds(:,$lev.size-1,:,:,0)+dz(:,$lev.size-1,:,:);'
        second_target+='z=p;z=0.5*(z_bnds(:,:,:,:,1)+z_bnds(:,:,:,:,0));'

    elif target=='z':
        second_target='*dlnp=-9.8*dz/(287.04*(1+0.61)*ta);'
        second_target+='*lnp_bnds=z_bnds;lnp_bnds=0.0;'
        second_target+='lnp_bnds(:,0,:,:,0)=log(ps);for(*iz=0;iz<$lev.size-2;iz++){'
        second_target+='lnp_bnds(:,iz,:,:,1)=lnp_bnds(:,iz,:,:,0)+dlnp(:,iz,:,:);'
        second_target+='lnp_bnds(:,iz+1,:,:,0)=lnp_bnds(:,iz,:,:,1);'
        second_target+='};'
        second_target+='lnp_bnds(:,$lev.size-1,:,:,1)=lnp_bnds(:,$lev.size-1,:,:,0)+dlnp(:,$lev.size-1,:,:);'
        second_target+='dp=dlnp; dp=exp(lnp_bnds(:,:,:,:,1))-exp(lnp_bnds(:,:,:,:,0));'
        second_target+='p=dp; p=0.5*(exp(lnp_bnds(:,:,:,:,1))+exp(lnp_bnds(:,:,:,:,0)));'

    import time
    for var in var_list.keys():
        script_to_call='ncks -3 -G : -g '+ var + ' -A ' +' '.join([options.in_file,options.out_file])
        out=subprocess.call(script_to_call,shell=True)

    script_to_call='ncap2 -O -3 -s \''+'bnds=bnds.double();'+'\' '+options.out_file+' '+options.out_file
    out=subprocess.call(script_to_call,shell=True)
    #script_to_call='ncap2 -3 -O -s \''+first_target+'\' '+options.out_file+' '+options.out_file
    script_to_call='ncap2 -O -3 -s \''+first_target+second_target+'\' '+options.out_file+' '+options.out_file
    #print script_to_call
    out=subprocess.call(script_to_call,shell=True)

    out_var_list=['dz','dp','z','p']
    for var in out_var_list:
        script_to_call='ncks -4 -L 1 -G '+ var + ' -v '+var+',slev -g / -A ' +' '.join([options.out_file,options.out_file+'.tmp'])
        out=subprocess.call(script_to_call,shell=True)
    
    try:
        shutil.move(options.out_file+'.tmp',options.out_file)
    except OSError:
        pass
    #output=Dataset(options.out_file)
    #output.close()
    return

def convert_half_level_pressures(options):
    data=Dataset(options.in_file)

    var_list={
                'ta':('time','lev','lat','lon'),
                'hus':('time','lev','lat','lon'),
                'pa':('time','lev','lat','lon'),
                'orog':('lat','lon')
                }

    check_file_consistency(data,var_list)
    data.close()

    first_target=';'.join([
                    'dp=ta;'
                    'dp(:,:,:,:)=(pa(:,1:$slev.size-1,:,:)-pa(:,0:$slev.size-2,:,:))',
                    'p=ta;',
                    'p(:,:,:,:)=0.5*(pa(:,1:$slev.size-1,:,:)+pa(:,0:$slev.size-2,:,:))',
                    ])+';'

    second_target='dz=(-287.04*(1+0.61)*ta*dp/p/9.8).float();'
    second_target+='*z_bnds=pa;z_bnds=0.0;'
    second_target+='for(*it=0;it<$time.size-1;it++){z_bnds(it,0,:,:)=orog;};for(*iz=0;iz<$slev.size-2;iz++){'
    second_target+='z_bnds(:,iz+1,:,:)=z_bnds(:,iz,:,:)+dz(:,iz,:,:);'
    second_target+='};'
    second_target+='z=ta;'
    second_target+='z(:,:,:,:)=0.5*(z_bnds(:,1:$slev.size-1,:,:)+z_bnds(:,0:$slev.size-2,:,:));'

    for var in var_list.keys():
        #Bug in netcdf 4.3.3.1. Could change "-3" to "-4" in later versions
        script_to_call='ncks -3 -G : -g '+ var +' '+ ' '.join([options.in_file,options.out_file+'.tmp'])
        out=subprocess.call(script_to_call,shell=True)
        if var=='pa':
            script_to_call='ncrename -v lev,slev -d lev,slev '+options.out_file+'.tmp'
            out=subprocess.call(script_to_call,shell=True)
        script_to_call='ncks -A -4 -L 1 ' + ' '.join([options.out_file+'.tmp',options.out_file])
        out=subprocess.call(script_to_call,shell=True)
        os.remove(options.out_file+'.tmp')

    #script_to_call='ncap2 -3 -O -s \''+first_target+'\' '+options.out_file+' '+options.out_file
    script_to_call='ncap2 -v -O -4 -L 1 -s \''+first_target+second_target+'\' '+options.out_file+' '+options.out_file
    out=subprocess.call(script_to_call,shell=True)

    out_var_list=['dz','dp','z','p']
    for var in out_var_list:
        script_to_call='ncks -4 -L 1 -G '+ var + ' -v '+var+',slev -g / -A ' +' '.join([options.out_file,options.out_file+'.tmp'])
        out=subprocess.call(script_to_call,shell=True)
    
    try:
        shutil.move(options.out_file+'.tmp',options.out_file)
    except OSError:
        pass
    #output=Dataset(options.out_file)
    #output.close()
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

    half_pres_parser=subparsers.add_parser('convert_half_level_pressures',
                                           help='This function takes CMOR output hybrid coordinates and create CDO-compliant hybrid coordinates.',
                                           epilog=epilog,
                                           formatter_class=argparse.RawTextHelpFormatter)
    input_arguments(half_pres_parser)
    options=parser.parse_args()

    if options.command=='convert_hybrid':
        convert_hybrid(options)
    elif options.command=='convert_half_level_pressures':
        convert_half_level_pressures(options)
        
    return 

def input_arguments(parser):
    parser.add_argument('in_file',
                                 help='Input file')
    parser.add_argument('out_file',
                                 help='Output file')
    return

if __name__ == "__main__":
    main()
