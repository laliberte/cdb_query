import os
import copy
from pyesgf.search import SearchConnection

import database_utils
from database_utils import File_Expt

def descend_tree(search_path,file_type_list,variable_list,experiment_list,diag_tree_desc):
    #This function generates the database
    diag_tree_desc.append('file_type')
    diag_tree_desc.append('path')

    session, time_db = database_utils.load_database(diag_tree_desc)
    file_expt = File_Expt(diag_tree_desc)

    #Create the database:
    for experiment in experiment_list.keys():
        for var_name in variable_list.keys():
            experiment_variable_search(session,file_expt,search_path,file_type_list,experiment,var_name,*variable_list[var_name])

    paths_dict={}
    for item in session.query(File_Expt).all():
        paths_dict=database_utils.create_tree(item,diag_tree_desc,paths_dict)

    return paths_dict

def experiment_variable_search(session,file_expt,search_path,file_type_list,experiment,var_name,frequency,realm,mip):

    #conn = SearchConnection(search_path, distrib=False)
    conn = SearchConnection(search_path, distrib=True)

    #Search the ESGF:
    ctx = conn.new_context()
    ctx = ctx.constrain(project='CMIP5',
                        experiment=experiment,
                        time_frequency=frequency,
                        realm=realm,
                        cmor_table=mip,
                        variable=var_name)

    keys_dict={}
    keys_dict['experiment']=experiment
    keys_dict['var']=var_name
    keys_dict['realm']=realm
    keys_dict['frequency']=frequency
    #Should put this constraint in the search but we put it explicitly...

    remote_file_types=['HTTPServer','GridFTP']
    for result in ctx.search():
        if len(set(file_type_list).intersection(set(remote_file_types)))>0:
            #If remote file types were requested
            fil_ctx = result.file_context()
            fil = fil_ctx.search()
            for item in fil:
                variable=item.filename.split('_')[0]
                if variable==var_name:
                    for key in item.urls.viewkeys():
                        if key in file_type_list:
                            url_name = item.urls[key][0][0]
                            file_description=url_name.split('/')[-10:-1]
                            file_expt_copy = copy.deepcopy(file_expt)
                            keys_dict['path']=url_name
                            keys_dict['file_type']=key
                            keys_dict['center']=file_description[0]
                            keys_dict['model']=file_description[1]
                            keys_dict['rip']=file_description[6]
                            keys_dict['mip']=file_description[5]
                            keys_dict['version']=file_description[7]
                            #Create database entry:
                            database_utils.create_entry(session,file_expt_copy,keys_dict)

        if 'OPeNDAP' in file_type_list:
            #OPeNDAP files were requested:
            agg_ctx = result.aggregation_context()
            agg = agg_ctx.search()
            for item in agg:
                file_description=item.json['title'].split('.aggregation')[0].split('.')
                variable=file_description[-2]
                if variable==var_name:
                    #print file_description
                    url_name=item.opendap_url
                    if url_name!=None:
                        file_expt_copy = copy.deepcopy(file_expt)
                        keys_dict['path']=url_name
                        keys_dict['file_type']='OPeNDAP'
                        keys_dict['center']=file_description[-9]
                        keys_dict['model']=file_description[-8]
                        keys_dict['rip']=file_description[-3]
                        keys_dict['mip']=file_description[-4]
                        keys_dict['version']='v'+file_description[-1].replace('v','')
                        #Create database entry:
                        database_utils.create_entry(session,file_expt_copy,keys_dict)

    return

