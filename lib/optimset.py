import copy
import os

import nc_Database

import retrieval_utils

from operator import itemgetter

import netcdf_utils

import sqlalchemy

import numpy as np


queryable_file_types=['HTTPServer','local_file']

def find_time(pointers,file_expt):
    #session,file_expt,path_name,file_type,propagated_values):
    #Top function to define how the time axis is created:
    if file_expt.file_type in queryable_file_types:
        find_time_file(pointers,file_expt)
    #elif file_expt.file_type in ['GridFTP']:
    #    find_time_file(pointers,file_expt)
    #elif file_expt.file_type in ['local_file','OPeNDAP']:
    #    find_time_opendap(pointers,file_expt)
    return
        
def find_time_file(pointers,file_expt):#session,file_expt,path_name):
    #If the path is a remote file, we must use the time stamp
    filename=os.path.basename(file_expt.path)
    
    #Check if file has fixed time_frequency or is a climatology:
    if file_expt.time_frequency in ['fx']:
        pointers.session.add(file_expt)
        pointers.session.commit()
        return
    else:
        time_stamp=filename.replace('.nc','').split('_')[-1].split('|')[0]
    #time_stamp[0] == 'r':

    #Recover date range from filename:
    years_range = [int(date[:4]) for date in time_stamp.split('-')]
    #Check for yearly data
    if len(time_stamp.split('-')[0])>4:
        months_range=[int(date[4:6]) for date in time_stamp.split('-')]
    else:
        months_range=range(1,13)
    years_list=range(*years_range)
    years_list.append(years_range[1])

    #Record in the database:
    if file_expt.file_type in ['local_file']:
        file_available=True
    else:
        file_available = retrieval_utils.check_file_availability(file_expt.path.split('|')[0])
    for year in years_list:
        for month in range(1,13):
            if not ( (year==years_range[0] and month<months_range[0]) or
                     (year==years_range[1] and month>months_range[1])   ):
                if file_available:
                    file_expt_copy = copy.deepcopy(file_expt)
                    setattr(file_expt_copy,'time',str(year)+str(month).zfill(2))
                    pointers.session.add(file_expt_copy)
                    pointers.session.commit()
    return

def obtain_time_list(diagnostic,project_drs,var_name,experiment,model):
    #Do this without fx variables:
    conditions=([
                 nc_Database.File_Expt.var==var_name,] +
               [ getattr(nc_Database.File_Expt,desc)==model[desc_id]
                 for desc_id,desc in enumerate(project_drs.simulations_desc)]
               )
    for field_id, field in enumerate(project_drs.var_specs):
        conditions.append(getattr(nc_Database.File_Expt,field)==diagnostic.header['variable_list'][var_name][field_id])
    time_list_var=[x[0] for x in diagnostic.nc_Database.session.query(
                             nc_Database.File_Expt.time
                            ).filter(sqlalchemy.and_(*conditions)).distinct().all()]
    return time_list_var

def find_model_list(diagnostic,project_drs,model_list,experiment,min_time):
    period_list = diagnostic.header['experiment_list'][experiment]
    if not isinstance(period_list,list): period_list=[period_list]
    years_list=[]
    for period in period_list:
        years_range=[int(year) for year in period.split(',')]
        years_list.extend(range(*years_range))
        years_list.append(years_range[1])
    time_list=[]
    for year in years_list:
        for month in get_diag_months_list(diagnostic):
            time_list.append(str(year).zfill(4)+str(month).zfill(2))

    #Remove the fixed variables manually:
    model_list_var=copy.copy(model_list)

    for model in model_list_var:
        missing_vars=[]
        if years_list[0]<=10:
            #Fix for experiments without a standard time range:
            min_time_list=[]
            for var_name in diagnostic.header['variable_list'].keys():
                if not diagnostic.header['variable_list'][var_name][0] in ['fx','clim']:
                    time_list_var=obtain_time_list(diagnostic,project_drs,var_name,experiment,model)
                    if len(time_list_var)>0:
                        min_time_list.append(int(np.floor(np.min([int(time) for time in time_list_var])/100.0)*100))
            min_time['_'.join(model)+'_'+experiment]=np.min(min_time_list)
        else:
            min_time['_'.join(model)+'_'+experiment]=0

        for var_name in diagnostic.header['variable_list'].keys():
            if not diagnostic.header['variable_list'][var_name][0] in ['fx','clim']:
                time_list_var=obtain_time_list(diagnostic,project_drs,var_name,experiment,model)
                time_list_var=[str(int(time)-int(min_time['_'.join(model)+'_'+experiment])).zfill(6) for time in time_list_var]
                if not set(time_list).issubset(time_list_var):
                    missing_vars.append(var_name+':'+','.join(
                                        diagnostic.header['variable_list'][var_name])+
                                        ' for some months: '+','.join(
                                        sorted(set(time[:4] for time in set(time_list).difference(time_list_var)))
                                        )
                                       )
        if len(missing_vars)>0:
           #print('\nThe reasons why some simulations were excluded:')
           print('_'.join(model)+' excluded because experiment '+experiment+' is missing variables:\n'),
           for item in missing_vars: print(item)
           model_list.remove(model)
    return model_list, min_time

def get_diag_months_list(diagnostic):
    if 'months_list' in diagnostic.header.keys():
        diag_months_list=diagnostic.header['months_list']
    else:
        diag_months_list=range(1,13)
    return diag_months_list

def optimset_distributed(database,options,semaphores):
    #print 'Starting ',options.institute,options.model,options.ensemble
    filepath=optimset(database,options,semaphores=semaphores)
    #print 'Finished ',options.institute,options.model,options.ensemble
    return filepath

def optimset(database,options,semaphores=None):
    database.load_database(options,find_time)
    #Find the list of institute / model with all the months for all the years / experiments and variables requested:
    intersection(database,options)
    
    dataset=database.nc_Database.write_database(database.header,options,'record_meta_data',semaphores=semaphores)
    database.close_database()
    output=dataset.filepath()
    dataset.close()
    return output

def intersection(database,options):
    #This function finds the models that satisfy all the criteria

    #Step one: find all the institute / model tuples with all the requested variables
    #          for all months of all years for all experiments.
    simulations_list=database.nc_Database.simulations_list()
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    model_list=copy.copy(simulations_list_no_fx)

    min_time=dict()
        #print experiment,model_list
    for experiment in database.header['experiment_list'].keys():
        model_list, min_time = find_model_list(database,database.drs,model_list,experiment,min_time)

    #Step two: create the new paths dictionary:
    variable_list_requested=[]
    for var_name in database.header['variable_list'].keys():
        variable_list_requested.append((var_name,)+tuple(database.header['variable_list'][var_name]))

    #Step three: find the models to remove:
    simulations_desc_indices_without_ensemble=range(0,len(database.drs.simulations_desc))
    simulations_desc_indices_without_ensemble.remove(database.drs.simulations_desc.index('ensemble'))
    models_to_remove=set(simulations_list_no_fx).difference(model_list)

    #Step four: remove from database:
    for model in models_to_remove:
        conditions=[ getattr(nc_Database.File_Expt,field)==model[field_id] for field_id, field in enumerate(itemgetter(*simulations_desc_indices_without_ensemble)(database.drs.simulations_desc))]
        database.nc_Database.session.query(nc_Database.File_Expt).filter(*conditions).delete()
                
    return 
