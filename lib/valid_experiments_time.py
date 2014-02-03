import copy
import os

from tree_utils import File_Expt
import tree_utils

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
    time_stamp=filename.replace('.nc','').split('_')[-1].split('|')[0]
    if time_stamp[0] == 'r':
        pointers.session.add(file_expt)
        pointers.session.commit()
        return

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
        file_checked=True
        file_available=True
    else:
        file_checked=False
    for year in years_list:
        for month in range(1,13):
            if not ( (year==years_range[0] and month<months_range[0]) or
                     (year==years_range[1] and month>months_range[1])   ):
                if not file_checked:
                    file_available = retrieval_utils.check_file_availability(file_expt.path.split('|')[0])
                    file_checked=True
                if file_available:
                    file_expt_copy = copy.deepcopy(file_expt)
                    setattr(file_expt_copy,'time',str(year)+str(month).zfill(2))
                    pointers.session.add(file_expt_copy)
                    pointers.session.commit()
    return

#def find_time_opendap(pointers,file_expt):
#    #If the file is local  or opendap, it is queryable and we can recover the
#    #indices corresponding to the month. It is slower but it allows for a fast implementation
#    #of scripts over a distant connection.
#
#    if file_expt.time_frequency in ['fx','clim']:
#        #If the time axis is fixed or it is a climatology, do nothing:
#        file_expt_copy = copy.deepcopy(file_expt)
#        setattr(file_expt_copy,'time',str(000000))
#        pointers.session.add(file_expt_copy)
#        pointers.session.commit()
#        return
#
#    year_axis, month_axis = netcdf_utils.get_year_axis(file_expt.path)
#    if year_axis is None or month_axis is None:
#        #File could not be opened and should be excluded from analysis
#        return
#
#    #Record the indices per month:
#    for year in set(year_axis):
#        year_id = np.where(year_axis==year)[0]
#
#        if file_expt.time_frequency in ['yr']:
#            month_id=year_id
#            for month in range(1,13):
#                file_expt_copy = copy.deepcopy(file_expt)
#                setattr(file_expt_copy,'time',str(year)+str(month).zfill(2))
#                setattr(file_expt_copy,'path',file_expt.path+'|'+str(np.min(month_id))+'|'+str(np.max(month_id)))
#                pointers.session.add(file_expt_copy)
#        else:
#            months_year=np.unique(month_axis[year_id])
#            for month in months_year:
#                month_id = year_id[np.where(month==month_axis[year_id])[0]]
#                file_expt_copy = copy.deepcopy(file_expt)
#                setattr(file_expt_copy,'time',str(year)+str(month).zfill(2))
#                setattr(file_expt_copy,'path',file_expt.path+'|'+str(np.min(month_id))+'|'+str(np.max(month_id)))
#                pointers.session.add(file_expt_copy)
#        pointers.session.commit()
#    return

def obtain_time_list(diagnostic,project_drs,var_name,experiment,model):
    #Do this without fx variables:
    conditions=[
                 File_Expt.var==var_name,
                 File_Expt.institute==model[0],
                 File_Expt.model==model[1],
                 File_Expt.ensemble==model[2]
               ]
    for field_id, field in enumerate(project_drs.var_specs):
        conditions.append(getattr(File_Expt,field)==diagnostic.header['variable_list'][var_name][field_id])
    time_list_var=[x[0] for x in diagnostic.pointers.session.query(
                             File_Expt.time
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

#def intersection(self,diag_tree_desc, diag_tree_desc_final):
def intersection(diagnostic,project_drs):
    #This function finds the models that satisfy all the criteria
    diagnostic.pointers.create_database(find_time)

    #Step one: find all the institute / model tuples with all the requested variables
    #          for all months of all years for all experiments.
    simulations_list=diagnostic.simulations_list()
    #model_list=[model for model in copy.copy(simulations_list) if model[2]!='r0i0p0']
    model_list=copy.copy(simulations_list)

    min_time=dict()
        #print experiment,model_list
    for experiment in diagnostic.header['experiment_list'].keys():
        model_list, min_time = find_model_list(diagnostic,project_drs,model_list,experiment,min_time)

    #Step two: create the new paths dictionary:
    variable_list_requested=[]
    for var_name in diagnostic.header['variable_list'].keys():
        variable_list_requested.append((var_name,)+tuple(diagnostic.header['variable_list'][var_name]))

    #Step three: find the models to remove:
    simulations_desc_indices_without_ensemble=range(0,len(project_drs.simulations_desc))
    simulations_desc_indices_without_ensemble.remove(project_drs.simulations_desc.index('ensemble'))
    models_to_remove=set(simulations_list).difference(model_list)

    #Step four: remove from database:
    for model in models_to_remove:
        conditions=[ getattr(File_Expt,field)==model[field_id] for field_id, field in enumerate(itemgetter(*simulations_desc_indices_without_ensemble)(project_drs.simulations_desc))]
        diagnostic.pointers.session.query(File_Expt).filter(*conditions).delete()
                
#    #Delete the original tree to rebuild it with only the elements we want:
#    diagnostic.pointers.clear_tree(project_drs.discovered_drs)
#    
#    #time-less variables:
#    time_less_frequencies=['fx','clim']
#    #Loop through models:
#    for model in model_list:
#        #print '_'.join(model)
#        item_list=diagnostic.pointers.session.query(
#                                  File_Expt
#                                 ).filter(sqlalchemy.and_(
#                                                            File_Expt.institute==model[0],
#                                                            File_Expt.model==model[1],
#                                                         )).all()
#        item_list_final=[item for item in item_list if 
#                                    item.var in diagnostic.header['variable_list'].keys() and
#                                    not item.time_frequency in time_less_frequencies and
#                                    [getattr(item,field) for field in project_drs.var_specs]==diagnostic.header['variable_list'][item.var] and
#                                    item.ensemble==model[2]
#                         ]
#            
#        item_list_final.extend(
#                        [item for item in item_list if 
#                            item.var in diagnostic.header['variable_list'].keys() and
#                            item.time_frequency in time_less_frequencies and
#                            [getattr(item,field) for field in project_drs.var_specs]==diagnostic.header['variable_list'][item.var]
#                        ]
#                        )
#        
#        for item in item_list_final:
#            #Retrieve the demanded years list for this experiment
#            period_list = diagnostic.header['experiment_list'][item.experiment]
#            if not isinstance(period_list,list): period_list=[period_list]
#            for period in period_list:
#                years_range=[int(year) for year in period.split(',')]
#                years_list=range(*years_range)
#                years_list.append(years_range[1])
#
#                time_list=[ str(year).zfill(4)+str(month).zfill(2) for year in years_list for month in get_diag_months_list(diagnostic)]
#                if not item.time_frequency in time_less_frequencies:
#                    min_time_id=[item.institute,item.model,item.ensemble,item.experiment]
#                    #Works if the variable is not fx:
#                    if str(int(item.time)-int(min_time['_'.join(min_time_id)])).zfill(6) in time_list:
#                        diagnostic.pointers.attribute_item(item)
#                        #print('Adding item')
#                        diagnostic.pointers.add_item()
#                        #print('Done adding item')
#                else:
#                    #If fx, we create the time axis for easy retrieval:
#                    for time in time_list:
#                        item.time=time
#                        diagnostic.pointers.attribute_item(item)
#                        diagnostic.pointers.add_item()
#            #print [ getattr(item,val) for val in dir(item) if val[0]!='_']
#            #print('Done item list')
    return 
