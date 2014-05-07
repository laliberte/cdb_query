"""
This python code computes a cyclone mask as in Wernli and Schwierz, 2006.
Cyclones are identified as minima in a 2D field

Frederic Laliberte 07/2011
"""
import cdb_query.netcdf_utils as netcdf_utils

def local_min(nc_input_file,nc_output_file,nc_comp,pres_level_list,num_procs):
    #LOAD THE DATA:
    data_root = Dataset(nc_input_file,'r')

    #CREATE THE OUTPUT FILE
    output_root = Dataset(nc_output_file,'w',format='NETCDF4')
    output_root = netcdf_utils.replicate_netcdf_file(output_root,data_root)

    #ADD DISTRIBUTION WEIGHT
    #var_list = data.variables.keys()
    var_list = data_root.groups.keys()


    #DETERMINE WHICH VARIABLES TO DISTRIBUTE
    for var in var_list:
        output=output_root.createGroup(var+'_mask')
        data=data_root.groups[var]
        output = netcdf_utils.replicate_netcdf_file(output,data)

        #CREATE OUTPUT VAR:
        #FIND WHICH DIMENSIONS TO INCLUDE
        var_dims = list(data.variables[var].dimensions)

        if set(var_dims).issuperset({'lon','lat'}):
            print(var)
            output=netcdf_utils.replicate_netcdf_var_dimensions(output,data,var)
            #CREATE OUTPUT VARIABLE
            final_mask = output.createVariable(var+'_mask','f',tuple(var_dims),zlib=nc_comp)

            #c_to_float=np.vectorize(np.float)
            #slp = c_to_float(data.variables[var][:])
            slp = data.variables[var][:]

            #Add a random machine-precision perturbation:
            slp*=(1.0+np.random.normal(size=slp.shape)*1e-10)
            lat = data.variables['lat'][:]

            lon_ind=var_dims.index('lon')
            lat_ind=var_dims.index('lat')
            
            #COMPUTE MINIMA MASK:
            #Allow an asynchronous implementation:
            print('Using {0} processors'.format(num_procs))
            pool=mproc.Pool(processes=int(num_procs))

            time_split=slp.shape[0]
            #final_mask[:]=np.concatenate(pool.map(local_min_one_dt,
            temp=np.concatenate(pool.map(local_min_one_dt,
                                                  zip(np.vsplit(slp,time_split),
                                                      [pres_level_list for x in range(time_split)],
                                                      [(2*np.pi/slp.shape[lon_ind])*(np.pi/slp.shape[lat_ind]) for x in range(time_split)]),
                                                  chunksize=1),axis=0)
            final_mask[:]=np.where(temp>0.0,1,0)

            pool.close()
            output.sync()
    data_root.close()
    output_root.close()

def local_min_one_dt(passed_tuple):
    #Use this 
    slp=passed_tuple[0]
    pres_level_list=passed_tuple[1]
    area_weight=passed_tuple[2]

    #Create a four-nearest neighbor footprint:
    cross_struct = generate_binary_structure(2,1)

    #Create output:
    slp_mask=np.zeros(slp.shape)

    min_list=[]
    mins_prev=[]
    for pres_lev_ind, pres_lev in enumerate(pres_level_list):
        for periodic in range(2):
            slp=np.concatenate((slp[:,:,-1:],slp,slp[:,:,:1]),axis=2)

            #Find the set of local minima:
            slp_min=np.vstack(map(lambda x: np.reshape(minimum_filter(np.squeeze(x),footprint=cross_struct),x.shape),np.vsplit(slp,slp.shape[0])))
            loc_min_mask=((slp==slp_min)&(slp<pres_lev))

            #Dilate minima for each time step:
            slp=np.vstack(map(lambda (x,y): np.reshape(dilate_minima(np.squeeze(x),np.squeeze(y),pres_lev,cross_struct),x.shape),
                                zip(np.vsplit(slp,slp.shape[0]),np.vsplit(loc_min_mask,loc_min_mask.shape[0]))))
            slp=slp[:,:,1:-1]

        #Create the mask:
        mins=np.unique(slp[slp<pres_lev])
        for min_diff in set(mins_prev).difference(set(mins)):
            #Next, a tolerance is applied.
            #First, we discard any cyclone that has an area in spectral space
            #that is smaller than a T42 spectral grid:
            area_tolerance=(np.pi/64.0)*(np.pi/64.0) #Equivalent to T42
            #Second, we discard cyclones that are shallower than 0hpa.
            #Change this value for a different tolerance:
            difference_min=0.0

            #Finally, attribute the cyclone mask:
            new_mins=np.unique(slp[slp_prev==min_diff])
            for new_min in new_mins:
                region_inter=((slp_prev==min_diff)&(slp==new_min))
                if (area_weight*np.where(region_inter,1.0,0.0)).sum()>area_tolerance and (pres_lev-min_diff)>difference_min:
                    if min_diff not in min_list:
                        slp_mask=np.where(region_inter,min_diff,slp_mask)
                    if new_min not in min_list:
                        min_list.append(new_min)
                        slp_mask=np.where(slp_prev==new_min,new_min,slp_mask)
        mins_prev=mins
        slp_prev=np.copy(slp)

    return slp_mask

def dilate_minima(slp,loc_min_mask,pres_lev,cross_struct): 
    #Find all the local minima values at that time-step:
    loc_min_vals=np.unique(slp[loc_min_mask])

    #Apply binary dilution:
    if loc_min_vals.any(): 
        min_slp=np.dstack(map(lambda x: np.where(binary_dilation(x==slp,mask=(slp<pres_lev),structure=cross_struct,iterations=-1),x,slp),loc_min_vals)).min(2)
    else:
        min_slp=np.copy(slp)
    return min_slp

def replicate_netcdf_file(output,data):
	for att in data.ncattrs():
	    setattr(output,att,getattr(data,att))
        output.history+='\n' 
        output.history+=dt.datetime.now().strftime("%Y-%m-%d %H:%M") #Add time
        output.history+=' joint_distribution.py'
	return output

def replicate_netcdf_var(output,data,var):
	for att in data.variables[var].ncattrs():
	    if att[0]!='_':
	       setattr(output.variables[var],att,getattr(data.variables[var],att))
	return output

if __name__ == "__main__":
    import sys
    import numpy as np
    from netCDF4 import Dataset
    import datetime as dt
    from optparse import OptionParser
    from mpl_toolkits.basemap import Basemap
    from scipy.ndimage.filters import minimum_filter
    from scipy.ndimage.morphology import binary_dilation
    from scipy.ndimage import generate_binary_structure
    import multiprocessing as mproc

    #Option parser
    parser = OptionParser()
    parser.add_option("-i","--input",dest="input_file",
                      help="Input file", metavar="FILE")
    parser.add_option("-o","--output",dest="output_file",
                      help="Output file", metavar="FILE")
    parser.add_option("-z","--zipped",dest="compression",
                      default=False, action="store_true",
                      help="Output file with NetCDF4 compression")
    parser.add_option("-j","--processors",dest="processors",
                      default=1,
                      help="Number of processors to use to process time dimension. Default: 1 processor")
    parser.add_option("-a","--action",dest="action",
                      default="None",
                      help="Action to process. If unspecified, does nothing.")
    (options, args) = parser.parse_args()

    if options.action == "None":
        print('No action specified. Doing nothing')
    elif options.action[:7] == "compute":
        print('Computing local minima')
        #Parse pressure levels list:
        pres_level_list=options.action.split(',')
        pres_level_list.remove('compute')
        pres_level_list=np.sort(np.array([float(s) for s in pres_level_list]))
        #Compute the local min:
        local_min(options.input_file,options.output_file,options.compression,pres_level_list,options.processors)
