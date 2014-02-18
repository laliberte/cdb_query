import copy
from tree_utils import File_Expt
from operator import itemgetter


import sqlalchemy

##### PATH #####
def find_path(pointers,file_expt):
    for val in dir(file_expt):
        if val[0]!='_' and val!='case_id':
            getattr(file_expt,val)
    pointers.session.add(file_expt)
    pointers.session.commit()
    return

def intersection(diagnostic,project_drs):
    #Find the dintersection of all models that have at least one file in each experiment
    diagnostic.pointers.create_database(find_path)

    #Step one: find all the institute / model tuples with all the requested variables
    simulations_list=diagnostic.simulations_list()

    #Remove fixed variables:
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[project_drs.simulations_desc.index('ensemble')]!='r0i0p0']
    model_list=copy.copy(simulations_list_no_fx)

    for experiment in diagnostic.header['experiment_list'].keys():
        for var_name in diagnostic.header['variable_list'].keys():
            time_frequency=diagnostic.header['variable_list'][var_name][project_drs.var_specs.index('time_frequency')]
            if not time_frequency in ['fx','en']:
                #Do this without fx variables:
                conditions=[
                             File_Expt.var==var_name,
                             File_Expt.experiment==experiment
                           ]
                for field_id, field in enumerate(project_drs.var_specs):
                    conditions.append(getattr(File_Expt,field)==diagnostic.header['variable_list'][var_name][field_id])

                model_list_var=diagnostic.pointers.session.query(
                                         File_Expt.institute,
                                         File_Expt.model,
                                         File_Expt.ensemble
                                        ).filter(sqlalchemy.and_(*conditions)).distinct().all()
                model_list=set(model_list).intersection(set(model_list_var))

        #Do it for fx variables:
        model_list_fx=[model[:-1] for model in model_list]
        for var_name in diagnostic.header['variable_list'].keys():
            time_frequency=diagnostic.header['variable_list'][var_name][project_drs.var_specs.index('time_frequency')]
            if time_frequency in ['fx','en']:
                conditions=[
                             File_Expt.var==var_name,
                             File_Expt.experiment==experiment
                           ]
                for field_id, field in enumerate(project_drs.var_specs):
                    conditions.append(getattr(File_Expt,field)==diagnostic.header['variable_list'][var_name][field_id])
                model_list_var=diagnostic.pointers.session.query(
                                         File_Expt.institute,
                                         File_Expt.model,
                                           ).filter(sqlalchemy.and_(*conditions)).distinct().all()
                model_list_fx=set(model_list_fx).intersection(set(model_list_var))


    #Step two: find the models to remove:
    model_list_combined=[model for model in model_list if remove_ensemble(model,project_drs) in model_list_fx]
    models_to_remove=set(simulations_list_no_fx).difference(model_list_combined)

    #Step three: remove from database:
    for model in models_to_remove:
        conditions=[ getattr(File_Expt,field)==model[field_id] for field_id, field in enumerate(project_drs.simulations_desc)]
        diagnostic.pointers.session.query(File_Expt).filter(*conditions).delete()

    #Step four: remove remaining models that only have fixed variables:
    simulations_list=diagnostic.simulations_list()

    #Remove fixed variables:
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[project_drs.simulations_desc.index('ensemble')]!='r0i0p0']

    #Remove fixed variables:
    models_to_remove=set(
                        [remove_ensemble(simulation,project_drs) for simulation in simulations_list]
                         ).difference([remove_ensemble(simulation,project_drs) for simulation in simulations_list_no_fx])

    for model in models_to_remove:
        conditions=[ getattr(File_Expt,field)==model[field_id] for field_id, field in enumerate(remove_ensemble(project_drs.simulations_desc,project_drs))]
        diagnostic.pointers.session.query(File_Expt).filter(*conditions).delete()

    return

def remove_ensemble(simulation,project_drs):
    simulations_desc_indices_without_ensemble=range(0,len(project_drs.simulations_desc))
    simulations_desc_indices_without_ensemble.remove(project_drs.simulations_desc.index('ensemble'))
    return itemgetter(*simulations_desc_indices_without_ensemble)(simulation)

