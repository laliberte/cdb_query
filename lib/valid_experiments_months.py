import copy
import os
import database_utils
from database_utils import File_Expt

import sqlalchemy

import numpy as np

from netCDF4 import Dataset
from netcdftime import utime


def find_months(session,file_expt,path_name,file_type,propagated_values):
    #Top function to define how the time axis is created:
    if file_type in ['HTTPServer','GridFTP']:
        find_months_file(session,file_expt,path_name)
    elif file_type in ['local_file','OPeNDAP']:
        find_months_opendap(session,file_expt,path_name,propagated_values['frequency'])
    return
        
def find_months_file(session,file_expt,path_name):
    #If the path is a remote file, we must use the time stamp
    filename=os.path.basename(path_name)

    #Check if file has fixed frequency or is a climatology:
    time_stamp=filename.replace('.nc','').split('_')[-1]
    if time_stamp[0] == 'r':
        file_expt_copy = copy.deepcopy(file_expt)
        setattr(file_expt_copy,'path',path_name)
        session.add(file_expt_copy)
        session.commit()
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
    for year in years_list:
        for month in range(1,13):
            if not ( (year==years_range[0] and month<months_range[0]) or
                     (year==years_range[1] and month>months_range[1])   ):
                file_expt_copy = copy.deepcopy(file_expt)
                setattr(file_expt_copy,'year',str(year))
                setattr(file_expt_copy,'month',str(month))
                setattr(file_expt_copy,'path',path_name)

                session.add(file_expt_copy)
                session.commit()
    return

def find_months_opendap(session,file_expt,path_name,frequency):
    #If the file is local  or opendap, it is queryable and we can recover the
    #indices corresponding to the month. It is slower but it allows for a fast implementation
    #of scripts over a distant connection.

    if frequency in ['fx','clim']:
        #If the time axis is fixed or it is a climatology, do nothing:
        file_expt_copy = copy.deepcopy(file_expt)
        setattr(file_expt_copy,'path',path_name)
        setattr(file_expt_copy,'year',str(0))
        setattr(file_expt_copy,'month',str(0))
        session.add(file_expt_copy)
        session.commit()
        return

    year_axis, month_axis = get_year_axis(path_name)
    if year_axis is None or month_axis is None:
        #File could not be opened and should be excluded from analysis
        return

    #Record the indices per month:
    for year in set(year_axis):
        year_id = np.where(year_axis==year)[0]

        if frequency in ['yr']:
            month_id=year_id
            for month in range(1,13):
                file_expt_copy = copy.deepcopy(file_expt)
                setattr(file_expt_copy,'year',str(year))
                setattr(file_expt_copy,'month',str(month))
                setattr(file_expt_copy,'path',path_name+'|'+str(np.min(month_id))+'|'+str(np.max(month_id)))
                session.add(file_expt_copy)
        else:
            months_year=np.unique(month_axis[year_id])
            for month in months_year:
                month_id = year_id[np.where(month==month_axis[year_id])[0]]
                file_expt_copy = copy.deepcopy(file_expt)
                setattr(file_expt_copy,'year',str(year))
                setattr(file_expt_copy,'month',str(month))
                setattr(file_expt_copy,'path',path_name+'|'+str(np.min(month_id))+'|'+str(np.max(month_id)))
                session.add(file_expt_copy)
        session.commit()
    return

def get_year_axis(path_name):
    try:
        #print 'Loading file... ',
        #print path_name
        data=Dataset(path_name)
        dimensions_list=data.dimensions.keys()
        if 'time' not in dimensions_list:
            raise Error('time is missing from variable')
        time_axis = data.variables['time'][:]
        if 'calendar' in dir(data.variables['time']):
            cdftime=utime(data.variables['time'].units,calendar=data.variables['time'].calendar)
        else:
            cdftime=utime(data.variables['time'].units)
        #print ' Done!'
        data.close()
    except:
        return None, None

    date_axis=cdftime.num2date(time_axis)
    year_axis=np.array([date.year for date in date_axis])
    month_axis=np.array([date.month for date in date_axis])
    return year_axis, month_axis

def intersection(paths_dict,diag_tree_desc, diag_tree_desc_final):
    #This function finds the models that satisfy all the criteria

    diag_tree_desc.append('file_type')
    diag_tree_desc.append('year')
    diag_tree_desc.append('month')
    diag_tree_desc.append('path')

    session, time_db = database_utils.load_database(diag_tree_desc)
    file_expt = File_Expt(diag_tree_desc)

    #Create database
    top_name='data_pointers'
    database_utils.create_database_from_tree(session,file_expt,paths_dict[top_name],top_name,{},find_months,'Processed ')

    #Step one: find all the center / model tuples with all the requested variables
    #          for all months of all years for all experiments.
    model_list=[tuple(simulation.split('_')) for simulation in paths_dict['simulations_list']]

    if 'months_list' in paths_dict['diagnostic'].keys():
        diag_months_list=paths_dict['diagnostic']['months_list']
    else:
        diag_months_list=range(1,13)

    for experiment in paths_dict['diagnostic']['experiment_list'].keys():
        period_list = paths_dict['diagnostic']['experiment_list'][experiment]
        if not isinstance(period_list,list): period_list=[period_list]
        years_list=[]
        for period in period_list:
            years_range=[int(year) for year in period.split(',')]
            years_list.extend(range(*years_range))
            years_list.append(years_range[1])
        months_list=[]
        for year in years_list:
            for month in diag_months_list:
                months_list.append((year,month))

        model_list_var=copy.copy(model_list)
        for model in model_list_var:
            missing_vars=[]
            for var_name in paths_dict['diagnostic']['variable_list'].keys():
                if not paths_dict['diagnostic']['variable_list'][var_name][0] in ['fx','clim']:
                            #Do this without fx variables:
                            conditions=[
                                         File_Expt.var==var_name,
                                         File_Expt.frequency==paths_dict['diagnostic']['variable_list'][var_name][0],
                                         File_Expt.realm==paths_dict['diagnostic']['variable_list'][var_name][1],
                                         File_Expt.mip==paths_dict['diagnostic']['variable_list'][var_name][2],
                                         File_Expt.experiment==experiment,
                                         File_Expt.center==model[0],
                                         File_Expt.model==model[1],
                                         File_Expt.rip==model[2]
                                       ]
                            months_list_var=[(int(x),int(y)) for (x,y) in session.query(
                                                     File_Expt.year,
                                                     File_Expt.month
                                                    ).filter(sqlalchemy.and_(*conditions)).distinct().all()]
                            if not set(months_list).issubset(months_list_var):
                                missing_vars.append(var_name+':'+','.join(
                                                    paths_dict['diagnostic']['variable_list'][var_name])+
                                                    ' for some months in years: '+','.join(
                                                    sorted(set(str(year) for (year,month) in set(months_list).difference(months_list_var)))
                                                    )
                                                   )
            if len(missing_vars)>0:
               #print('\nThe reasons why some simulations were excluded:')
               print('_'.join(model)+' excluded because experiment '+experiment+' is missing variables:\n'),
               for item in missing_vars: print(item)
               model_list.remove(model)

    #Step two: create the new paths dictionary:
    diag_tree_desc_final.append('path')
    new_paths_dict={}
    new_paths_dict['diagnostic']=paths_dict['diagnostic']
    new_paths_dict['data_pointers']={}

    variable_list_requested=[]
    for var_name in paths_dict['diagnostic']['variable_list'].keys():
        variable_list_requested.append((var_name,)+tuple(paths_dict['diagnostic']['variable_list'][var_name]))

    #Create a simulation list
    new_paths_dict['simulations_list']=[]

    #Loop through models:
    for model in model_list:
        new_paths_dict['simulations_list'].append('_'.join(model))

        months_list=session.query(
                                  File_Expt
                                 ).filter(sqlalchemy.and_(
                                                            File_Expt.center==model[0],
                                                            File_Expt.model==model[1],
                                                         )).all()
        for item in months_list:
            if item.var in paths_dict['diagnostic']['variable_list'].keys():
                if [item.frequency,item.realm,item.mip]==paths_dict['diagnostic']['variable_list'][item.var]:
                    #Retrieve the demanded years list for this experiment
                    period_list = paths_dict['diagnostic']['experiment_list'][item.experiment]
                    if not isinstance(period_list,list): period_list=[period_list]
                    for period in period_list:
                        years_range=[int(year) for year in period.split(',')]
                        years_list=range(*years_range)
                        years_list.append(years_range[1])

                        if not item.frequency in ['fx','clim']:
                            #Works if the variable is not fx:
                            if item.rip==model[2]:
                                if int(item.year) in years_list:
                                    if int(item.month) in diag_months_list:
                                        new_paths_dict['data_pointers']=database_utils.create_tree(item,diag_tree_desc_final,new_paths_dict['data_pointers'])
                        else:
                            #If fx, we create the time axis for easy retrieval:
                            for year in years_list:
                                for month in diag_months_list:
                                    item.year=year
                                    item.month=month
                                    new_paths_dict['data_pointers']=database_utils.create_tree(item,diag_tree_desc_final,new_paths_dict['data_pointers'])

    new_paths_dict['simulations_list'].sort()
    return new_paths_dict
