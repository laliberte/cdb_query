import copy
from tree_utils import File_Expt


import sqlalchemy


##### PATH #####
def find_path(pointers,file_expt):
    pointers.session.add(file_expt)
    pointers.session.commit()
    return

def intersection(diagnostic):
    #Find the dintersection of all models that have at least one file in each experiment
    diagnostic.pointers.create_database(find_path)

    #Step one: find all the center / model tuples with all the requested variables
    simulations_list=diagnostic.simulations_list()
    model_list=copy.copy(simulations_list)

    for experiment in diagnostic.header['experiment_list'].keys():
        for var_name in diagnostic.header['variable_list'].keys():
            if not diagnostic.header['variable_list'][var_name][2][:2] in ['fx','en']:
                #Do this without fx variables:
                conditions=[
                             File_Expt.var==var_name,
                             File_Expt.frequency==diagnostic.header['variable_list'][var_name][0],
                             File_Expt.realm==diagnostic.header['variable_list'][var_name][1],
                             File_Expt.mip==diagnostic.header['variable_list'][var_name][2],
                             File_Expt.experiment==experiment,
                           ]
                model_list_var=diagnostic.pointers.session.query(
                                         File_Expt.center,
                                         File_Expt.model,
                                         File_Expt.rip
                                        ).filter(sqlalchemy.and_(*conditions)).distinct().all()
                model_list=set(model_list).intersection(set(model_list_var))

        #Do it for fx variables:
        model_list_fx=[model[:-1] for model in model_list]
        for var_name in diagnostic.header['variable_list'].keys():
            if diagnostic.header['variable_list'][var_name][2][:2] in ['fx','en']:
                conditions=[
                             File_Expt.var==var_name,
                             File_Expt.frequency==diagnostic.header['variable_list'][var_name][0],
                             File_Expt.realm==diagnostic.header['variable_list'][var_name][1],
                             File_Expt.mip==diagnostic.header['variable_list'][var_name][2],
                             File_Expt.experiment==experiment
                           ]
                model_list_var=diagnostic.pointers.session.query(
                                         File_Expt.center,
                                         File_Expt.model,
                                           ).filter(sqlalchemy.and_(*conditions)).distinct().all()
                model_list_fx=set(model_list_fx).intersection(set(model_list_var))


    #Step two: create the new paths dictionary:

    variable_list_requested=[]
    for var_name in diagnostic.header['variable_list'].keys():
        variable_list_requested.append((var_name,)+tuple(diagnostic.header['variable_list'][var_name]))

    model_list_combined=[model for model in model_list if model[:-1] in model_list_fx]
    model_list_combined.extend([model + ('r0i0p0',) for model in model_list_fx])

    #Delete the original tree to rebuild it with only the elements we want:
    diagnostic.pointers.clear_tree(diagnostic.header['drs'])
    for model in model_list_combined:
            runs_list=diagnostic.pointers.session.query(
                                      File_Expt
                                     ).filter(sqlalchemy.and_(
                                                                File_Expt.center==model[0],
                                                                File_Expt.model==model[1],
                                                                File_Expt.rip==model[2],
                                                             )).all()
            for item in runs_list:
                for val in dir(item):
                    if val[0]!='_':
                        setattr(diagnostic.pointers.file_expt,val,getattr(item,val))
                diagnostic.pointers.add_item()
                
    return

