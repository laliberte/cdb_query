import os
import copy
from pyesgf.search import SearchConnection

import database_utils
from tree_utils import File_Expt

def descend_tree(pointers,header,search_path):
    #Create the database:
    for experiment in header['experiment_list'].keys():
        for var_name in header['variable_list'].keys():
            experiment_variable_search(pointers,search_path,header['file_type_list'],
                                        experiment,var_name,*header['variable_list'][var_name])

    return

def experiment_variable_search(pointers,search_path,file_type_list,
                                experiment,var_name,frequency,realm,mip):

    print 'Searching path ', search_path
    conn = SearchConnection(search_path, distrib=False)

    #Search the ESGF:
    ctx = conn.new_context(project='CMIP5',
                        experiment=experiment,
                        time_frequency=frequency,
                        realm=realm,
                        cmor_table=mip)

    pointers.file_expt.experiment=experiment
    pointers.file_expt.var=var_name
    pointers.file_expt.realm=realm
    pointers.file_expt.frequency=frequency
    pointers.file_expt.search=search_path
    pointers.file_expt.time='all'

    remote_file_types=['HTTPServer','GridFTP']
    for result in ctx.search(variable=var_name):
        if len(set(file_type_list).intersection(set(remote_file_types)))>0:
            #If remote file types were requested
            fil_ctx = result.file_context()
            #try:
            fil = fil_ctx.search(variable=var_name)
            #except:
            #    continue
            #fil = fil_ctx.search()
            for item in fil:
                for key in item.urls.viewkeys():
                    if key in file_type_list:
                        url_name = item.urls[key][0][0]
                        file_description=url_name.split('/')[-10:-1]
                        known_description=[ file_description[ind] for ind in [2,3,4,5,8] ]
                        if known_description==[experiment,frequency,realm,mip,var_name]:
                            pointers.file_expt.path=url_name+'|'+item.checksum
                            pointers.file_expt.file_type=key
                            pointers.file_expt.center=file_description[0]
                            pointers.file_expt.model=file_description[1]
                            pointers.file_expt.rip=file_description[6]
                            pointers.file_expt.mip=file_description[5]
                            pointers.file_expt.version=file_description[7]
                            #for item in dir(pointers.file_expt):
                            #    if not item[0]=='_':
                            #        print item, getattr(pointers.file_expt,item)
                            pointers.add_item()

        if 'OPeNDAP' in file_type_list:
            #OPeNDAP files were requested:
            agg_ctx = result.aggregation_context()
            agg = agg_ctx.search(variable=var_name)
            #agg = agg_ctx.search()
            for item in agg:
                file_description=item.json['title'].split('.aggregation')[0].split('.')
                variable=file_description[-2]
                if variable==var_name:
                    #print file_description
                    url_name=item.opendap_url
                    if url_name!=None:
                        pointers.file_expt.path=url_name
                        pointers.file_expt.file_type='OPeNDAP'
                        pointers.file_expt.center=file_description[-9]
                        pointers.file_expt.model=file_description[-8]
                        pointers.file_expt.rip=file_description[-3]
                        pointers.file_expt.mip=file_description[-4]
                        pointers.file_expt.version='v'+file_description[-1].replace('v','')
                        pointers.add_item()

    print 'Done searching for variable '+var_name
    return

