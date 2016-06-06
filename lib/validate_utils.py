#External:
import copy
import os
from operator import itemgetter
import sqlalchemy
import numpy as np
import datetime

#External but related:
import netcdf4_soft_links.remote_netcdf as remote_netcdf

#Internal:
import nc_Database
import cdb_query_archive_class

queryable_file_types=['OPENDAP','local_file']

def find_time(pointers,file_expt,time_slices=dict(),semaphores=dict(),session=None,remote_netcdf_kwargs=dict()):
    #Will check both availability and queryability:
    find_time_file(pointers,file_expt,time_slices=time_slices,semaphores=semaphores,
                                                    session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
    return

def find_time_available(pointers,file_expt,time_slices=dict(),semaphores=dict(),session=None,remote_netcdf_kwargs=dict()):
    #same as find_time but keeps non-queryable files:
    find_time_file(pointers,file_expt,file_available=True,time_slices=time_slices,semaphores=semaphores,
                                                            session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
    return

def find_time_file(pointers,file_expt,file_available=False,time_slices=dict(),semaphores=dict(),session=None,remote_netcdf_kwargs=dict()):
    #If the path is a remote file, we must use the time stamp
    filename=os.path.basename(file_expt.path)
    
    #Check if file has fixed time_frequency or is a climatology:
    if file_expt.time_frequency in ['fx']:
        pointers.session.add(file_expt)
        pointers.session.commit()
        return
    else:
        time_stamp=filename.replace('.nc','').split('_')[-1].split('|')[0]

    if not file_expt.experiment in pointers.header['experiment_list']:
        return

    #Find the years that were requested:
    years_requested=[int(year) for year in pointers.header['experiment_list'][file_expt.experiment].split(',')]
    years_list_requested=range(*years_requested)
    years_list_requested.append(years_requested[1])

    #Flag to check if the time axis is requested as relative:
    picontrol_min_time=(years_list_requested[0]<=10)

    #Recover date range from filename:
    years_range = [int(date[:4]) for date in time_stamp.split('-')]
    #Check for yearly data
    if len(time_stamp.split('-')[0])>4:
        months_range=[int(date[4:6]) for date in time_stamp.split('-')]
    else:
        months_range=range(1,13)
    years_list=range(*years_range)
    years_list.append(years_range[1])

    #Tweaked to allow for relative years:
    if not picontrol_min_time: years_list=[ year for year in years_list if year in years_list_requested]

    #Time was further sliced:
    if ('year' in time_slices.keys() and
         time_slices['year']!=None):
        years_list=[year for year in years_list if year in time_slices['year']]

    months_list=range(1,13)
    if ('month' in time_slices.keys() and
        time_slices['month']!=None):
        months_list=[month for month in months_list if month in time_slices['month']]

    #Record in the database:
    checked_availability=False
    for year_id,year in enumerate(years_list):
        for month in months_list:
            if  ( not ( (year==years_range[0] and month<months_range[0]) or
                     (year==years_range[1] and month>months_range[1])   ) ):
                if (not file_available and
                   not checked_availability):
                    checked_availability=True
                    if file_expt.file_type in ['local_file']:
                        file_available=True
                    else:
                        remote_data=remote_netcdf.remote_netCDF(file_expt.path.split('|')[0],
                                                                file_expt.file_type,
                                                                semaphores=semaphores,
                                                                session=session,
                                                                **remote_netcdf_kwargs)
                        file_available=remote_data.is_available()
                attribute_time(pointers,file_expt,file_available,year,month)
    return

def attribute_time(pointers,file_expt,file_available,year,month):
    #Record checksum of local files:
    #if file_expt.file_type in ['local_file'] and len(file_expt.path.split('|')[1])==0:
    #    #Record checksum
    #    file_expt.path+=retrieval_utils.md5_for_file(open(file_expt.path.split('|')[0],'r'))

    if file_available:
        #If file is avaible and queryable, keep it:
        file_expt_copy = copy.deepcopy(file_expt)
        setattr(file_expt_copy,'time',str(year).zfill(4)+str(month).zfill(2))
        pointers.session.add(file_expt_copy)
        pointers.session.commit()
    return

def obtain_time_list(diagnostic,project_drs,var_name,experiment,model):
    #Do this without fx variables:
    conditions=(
                 [nc_Database.File_Expt.var==var_name,] +
                 [nc_Database.File_Expt.experiment==experiment,] +
               [ getattr(nc_Database.File_Expt,desc)==model[desc_id]
                 for desc_id,desc in enumerate(project_drs.simulations_desc)]
               )
    for field_id, field in enumerate(project_drs.var_specs):
        conditions.append(getattr(nc_Database.File_Expt,field)==diagnostic.header['variable_list'][var_name][field_id])
    time_list_var=[x[0] for x in diagnostic.nc_Database.session.query(
                             nc_Database.File_Expt.time
                            ).filter(sqlalchemy.and_(*conditions)).distinct().all()]
    return time_list_var

def find_model_list(diagnostic,project_drs,model_list,experiment,options):
    #Time slices:
    time_slices=dict()
    if ( 'record_validate' in dir(options) and
         not options.record_validate):
         #Slice time unless the validate step should be recorded:
         for time_type in ['month','year']:
            if time_type in dir(options):
                time_slices[time_type]=getattr(options,time_type)

    #Determine the requested time list:
    period_list = diagnostic.header['experiment_list'][experiment]
    if not isinstance(period_list,list): period_list=[period_list]
    years_list=[]
    for period in period_list:
        years_range=[int(year) for year in period.split(',')]
        years_list.extend(range(*years_range))
        years_list.append(years_range[1])
    time_list=[]
    for year in years_list:
        if ( ( 'year' in time_slices.keys() and
                   (time_slices['year']==None or
                  year in time_slices['year'])) or
              (not 'year' in time_slices.keys())):
            for month in get_diag_month_list(diagnostic):
                if ( ( 'month' in time_slices.keys() and
                        (time_slices['month']==None or
                          month in time_slices['month'])) or
                     (not 'month' in time_slices.keys())):
                    time_list.append(str(year).zfill(4)+str(month).zfill(2))

    model_list_var=copy.copy(model_list)
    model_list_copy=copy.copy(model_list)

    #Flag to check if the time axis is requested as relative:
    picontrol_min_time=(years_list[0]<=10)

    for model in model_list_var:
        missing_vars=[]
        if picontrol_min_time:
            #Fix for experiments without a standard time range:
            min_time_list=[]
            for var_name in diagnostic.header['variable_list'].keys():
                if not diagnostic.header['variable_list'][var_name][0] in ['fx','clim']:
                    time_list_var=obtain_time_list(diagnostic,project_drs,var_name,experiment,model)
                    if len(time_list_var)>0:
                        min_time_list.append(int(np.floor(np.min([int(time) for time in time_list_var])/100.0)*100))
            #min_time['_'.join(model)+'_'+experiment]=np.min(min_time_list)
            min_time=np.min(min_time_list)
        else:
            #min_time['_'.join(model)+'_'+experiment]=0
            min_time=0

        var_names_no_fx=[var_name for var_name in diagnostic.header['variable_list'].keys()
                            if not diagnostic.header['variable_list'][var_name][0] in ['fx','clim']]
        if 'missing_years' in dir(options) and options.missing_years:
            #When missing years are allowed, ensure that all variables have the same times!
            valid_times=dict()
            for var_name in var_names_no_fx:
                time_list_var=obtain_time_list(diagnostic,project_drs,var_name,experiment,model)
                time_list_var=[str(int(time)-int(min_time)).zfill(6) for time in time_list_var]
                valid_times[var_name]=set(time_list).intersection(time_list_var)
            for var_name in var_names_no_fx:
                for var_name_to_compare in var_names_no_fx:
                    if (var_name!=var_name_to_compare and
                        valid_times[var_name]!=valid_times[var_name_to_compare]):
                        missing_vars.append(var_name+':'+','.join(
                                            diagnostic.header['variable_list'][var_name])+
                                            ' for some months ')
                        break
        else:
            #When missing years are not allowed, ensure that all variables have the requested times!
            for var_name in var_names_no_fx:
                time_list_var=obtain_time_list(diagnostic,project_drs,var_name,experiment,model)
                time_list_var=[str(int(time)-int(min_time)).zfill(6) for time in time_list_var]
                if not set(time_list).issubset(time_list_var):
                    missing_vars.append(var_name+':'+','.join(
                                        diagnostic.header['variable_list'][var_name])+
                                        ' for some months: '+','.join(
                                        sorted(set(time[:4] for time in set(time_list).difference(time_list_var)))
                                        )
                                       )
        if len(missing_vars)>0:
           #print('\nThe reasons why some simulations were excluded:')
           if 'experiment' in project_drs.simulations_desc:
               if experiment==model[project_drs.simulations_desc.index('experiment')]:
                   print('_'.join(model)+' excluded because it is missing variables:\n'),
                   for item in missing_vars: print(item)
           else:
               print('_'.join(model)+' excluded because experiment '+experiment+' is missing variables:\n'),
               for item in missing_vars: print(item)
           model_list_copy.remove(model)
    return model_list_copy

def get_diag_month_list(diagnostic):
    if 'month_list' in diagnostic.header.keys():
        diag_month_list=diagnostic.header['month_list']
    else:
        diag_month_list=range(1,13)
    return diag_month_list

def validate(database,options,q_manager=None,sessions=dict()):
    if 'data_node_list' in dir(database.drs):
        database.header['data_node_list']=database.drs.data_node_list
    else:
        data_node_list, url_list, simulations_list =database.find_data_nodes_and_simulations(options)
        if len(data_node_list)>1 and not options.no_check_availability:
                data_node_list=database.rank_data_nodes(options,data_node_list,url_list)
        database.header['data_node_list']=data_node_list

    semaphores=q_manager.validate_semaphores
    remote_netcdf_kwargs=dict()
    if 'validate_cache' in dir(options) and options.validate_cache:
        remote_netcdf_kwargs['cache']=options.validate_cache.split(',')[0]
        if len(options.validate_cache.split(','))>1:
            remote_netcdf_kwargs['expire_after']=datetime.timedelta(hours=float(options.validate_cache.split(',')[1]))
    if 'validate' in sessions.keys():
        session=sessions['validate']
    else:
        session=None

    time_slices=dict()
    if ( 'record_validate' in dir(options) and
         not options.record_validate):
         for time_type in ['month','year']:
            if time_type in dir(options):
                time_slices[time_type]=getattr(options,time_type)

    if options.no_check_availability:
        #Does not check whether files are available / queryable before proceeding.
        database.load_database(options,find_time_available,time_slices=time_slices,semaphores=semaphores,session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
        #Find the list of institute / model with all the months for all the years / experiments and variables requested:
        intersection(database,options)
        output=database.nc_Database.write_database(database.header,options,'record_paths',semaphores=semaphores,session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
    else:
        #Checks that files are available.
        database.load_database(options,find_time,time_slices=time_slices,semaphores=semaphores,session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
        #Find the list of institute / model with all the months for all the years / experiments and variables requested:
        intersection(database,options)
        output=database.nc_Database.write_database(database.header,options,'record_meta_data',semaphores=semaphores,session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
    database.close_database()
    return output

def intersection(database,options):
    #This function finds the models that satisfy all the criteria

    #Step one: find all the institute / model tuples with all the requested variables
    #          for all months of all years for all experiments.
    simulations_list=database.nc_Database.simulations_list()
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    model_list=copy.copy(simulations_list_no_fx)

    if not 'experiment' in database.drs.simulations_desc:
        for experiment in database.header['experiment_list'].keys():
            model_list = find_model_list(database,database.drs,model_list,experiment,options)
        model_list_combined=model_list
    else:
        model_list_combined=set().union(*[find_model_list(database,database.drs,model_list,experiment,options) 
                                           for experiment in database.header['experiment_list'].keys()])
    
    #Step two: create the new paths dictionary:
    variable_list_requested=[]
    for var_name in database.header['variable_list'].keys():
        variable_list_requested.append((var_name,)+tuple(database.header['variable_list'][var_name]))

    #Step three: find the models to remove:
    models_to_remove=set(simulations_list_no_fx).difference(model_list_combined)

    #Step four: remove from database:
    for model in models_to_remove:
        conditions=[getattr(nc_Database.File_Expt,field)==model[field_id] for field_id, field in enumerate(database.drs.simulations_desc)]
        database.nc_Database.session.query(nc_Database.File_Expt).filter(*conditions).delete()
    
    #Remove fixed variables:
    simulations_list=database.nc_Database.simulations_list()
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    models_to_remove=set(
                        [remove_ensemble(simulation,database.drs) for simulation in simulations_list]
                         ).difference([remove_ensemble(simulation,database.drs) for simulation in simulations_list_no_fx])

    for model in models_to_remove:
        conditions=[ getattr(nc_Database.File_Expt,field)==model[field_id] for field_id, field in enumerate(remove_ensemble(database.drs.simulations_desc,database.drs))]
        database.nc_Database.session.query(nc_Database.File_Expt).filter(*conditions).delete()

    return 

def remove_ensemble(simulation,project_drs):
    simulations_desc_indices_without_ensemble=range(0,len(project_drs.simulations_desc))
    simulations_desc_indices_without_ensemble.remove(project_drs.simulations_desc.index('ensemble'))
    return itemgetter(*simulations_desc_indices_without_ensemble)(simulation)

