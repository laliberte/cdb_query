import os

def union_headers(diag_headers):
    #Create the diagnostic description dictionary:
    diag_desc={}
    #Find all the requested realms, frequencies and variables:
    variable_list=['var_list','frequency_list','realm_list','mip_list']
    for list_name in variable_list: diag_desc[list_name]=[]
    for var_name in diag_headers['variable_list'].keys():
        diag_desc['var_list'].append(var_name)
        for list_id, list_name in enumerate(list(variable_list[1:])):
            diag_desc[list_name].append(diag_headers['variable_list'][var_name][list_id])

    #Find all the requested experiments and years:
    experiment_list=['experiment_list','years_list']
    for list_name in experiment_list: diag_desc[list_name]=[]
    for experiment_name in diag_headers['experiment_list'].keys():
        diag_desc['experiment_list'].append(experiment_name)
        for list_name in list(experiment_list[1:]):
            diag_desc[list_name].append(diag_headers['experiment_list'][experiment_name])
            
    #Find the unique members:
    for list_name in diag_desc.keys(): diag_desc[list_name]=list(set(diag_desc[list_name]))
    return diag_desc

#def find_optimset(diagnostic_headers_file, diagnostic_paths_file):
def find_optimset(options):
    import valid_experiments_path
    import json
    #Tree description following the CMIP5 DRS:
    diag_tree_desc=['search','center','model','experiment','frequency','realm','mip','rip',
                    'version','var']

    #Load the diagnostic header file:
    paths_dict={}
    paths_dict['diagnostic']=json.load(options.in_diagnostic_headers_file)
    
    #Create the diagnostic description dictionary with the union of all the requirements.
    #This will make the description easier and allow to appply the intersection of requirements
    #offline.
    diag_desc=union_headers(paths_dict['diagnostic'])

    #Build the simple pointers to files:
    paths_dict['data_pointers']={}
    paths_dict['data_pointers']['_name']='search'
    for search_path in paths_dict['diagnostic']['search_list']:
        top_path=os.path.expanduser(os.path.expandvars(search_path))
        if os.path.exists(top_path):
            #Local filesystem archive
            paths_dict['data_pointers'][search_path]=search_filesystem(
                                                                       diag_desc,
                                                                       diag_tree_desc[1:],
                                                                       top_path
                                                                       )
        else:
            #ESGF search
            paths_dict['data_pointers'][search_path]=search_esgf(search_path,
                                                                 paths_dict['diagnostic']['file_type_list'],
                                                                 paths_dict['diagnostic']['variable_list'],
                                                                 paths_dict['diagnostic']['experiment_list'],
                                                                 diag_tree_desc[1:]
                                                                 )

    #Find the list of center / model with all experiments and variables requested:
    diag_tree_desc_path=[
                            'center','model','experiment','rip','frequency','realm','mip',
                            'var','version','search','file_type'
                         ]
    paths_dict=valid_experiments_path.intersection(paths_dict,diag_tree_desc,diag_tree_desc_path)

    #Write dictionary to file using JSON encoding.
    outfile = open(options.out_paths_file,'w')
    json.dump(paths_dict,outfile)

    outfile.close()

def find_optimset_months(options):
    import json
    import valid_experiments_months
    paths_dict=json.load(options.in_diagnostic_headers_file)

    #Open the output file.If the gzip flag was used,
    #output directly to a compressed file:
    if options.gzip:
        import gzip
        outfile = gzip.open(options.out_paths_file+'.gz','w')
    else:
        outfile = open(options.out_paths_file,'w')

    diag_tree_desc_path=[
                            'center','model','experiment','rip','frequency','realm','mip',
                            'var','version','search','file_type'
                         ]
    #Find the list of center / model with all the months for all the years / experiments and variables requested:
    diag_tree_desc_months=[
                            'experiment','center','model','rip','frequency','realm','mip',
                            'var','year','month','file_type','version','search'
                         ]
    paths_dict=valid_experiments_months.intersection(paths_dict,diag_tree_desc_path,diag_tree_desc_months)

    #Write dictionary to file using JSON encoding.
    json.dump(paths_dict,outfile)

    outfile.close()
    

def search_filesystem(diag_desc,diag_tree_desc,top_path):
    import filesystem_query
    return filesystem_query.descend_tree(diag_desc,diag_tree_desc,top_path)

def search_esgf(search_path,file_type_list,variable_list,experiment_list,diag_tree_desc):
    import esgf_query
    return esgf_query.descend_tree(search_path,file_type_list,variable_list,experiment_list,diag_tree_desc)

def retrieval(retrieval_desc):
    import json
    #Reads the file created by find_optimset and gives the path to file:
    if retrieval_desc.in_diagnostic_pointers_file[-3:]=='.gz':
        import gzip
        infile=gzip.open(retrieval_desc.in_diagnostic_pointers_file,'r')
    else:
        infile=open(retrieval_desc.in_diagnostic_pointers_file,'r')
    paths_dict=json.load(infile)

    path_name=tree_retrieval(retrieval_desc,paths_dict['diagnostic'],paths_dict['data_pointers'])
    if retrieval_desc.file_type_flag:
        print path_name
    else:
        print path_name
    return

def tree_retrieval(retrieval_desc,diag_desc,paths_dict):
    #Reads the tree recursively:
    if isinstance(paths_dict,dict):
        #Read the level name:
        level_name=paths_dict['_name']
        if level_name in dir(retrieval_desc):
            #If the level name was specified in the retrieval, use this specification:
            path_name=tree_retrieval(retrieval_desc,diag_desc,paths_dict[str(getattr(retrieval_desc,level_name))])
        elif level_name=='version':
            #the 'version' field is peculiar. Here, we use the most recent, or largest version number:
            version_list=[]
            for version in paths_dict.keys():
                if version[0]!='_':
                    version_list.append(int(version[1:]))
            version='v'+str(max(version_list))
            path_name=tree_retrieval(retrieval_desc,diag_desc,paths_dict[version])
        else:
            #The level was not specified but an ordered list was provided in the diagnostic header.
            #Go through the list and pick the first avilable one:
            level_ordering=diag_desc[level_name+'_list']
            for level in level_ordering:
                if level in paths_dict.keys():
                    if level_name=='file_type' and retrieval_desc.file_type_flag:
                        #if the file_type_flag was used, the script outputs only the file_type
                        return level
                    path_name=tree_retrieval(retrieval_desc,diag_desc,paths_dict[level])
                    break
    else:
        path_name=paths_dict[0]
    return path_name

def simulations(simulations_desc):
    import json
    #Reads the file created by find_optimset and gives the path to file:
    if simulations_desc.in_diagnostic_pointers_file[-3:]=='.gz':
        import gzip
        infile=gzip.open(simulations_desc.in_diagnostic_pointers_file,'r')
    else:
        infile=open(simulations_desc.in_diagnostic_pointers_file,'r')
    paths_dict=json.load(infile)

    for item in paths_dict['simulations_list']:
        print item
    return

def slice_data(options):
    import json
    import database_utils
    import copy
    #Reads the file created by find_optimset and gives the path to file:
    if options.in_diagnostic_pointers_file[-3:]=='.gz':
        import gzip
        infile=gzip.open(options.in_diagnostic_pointers_file,'r')
    else:
        infile=open(options.in_diagnostic_pointers_file,'r')
    paths_dict=json.load(infile)

    #Slice the database:
    paths_dict['data_pointers']=database_utils.slice_data(options,paths_dict['data_pointers'])


    #Remove the simulations that were excluded:
    simulation_desc=['center','model','rip']
    intersection_simulations=set(simulation_desc).intersection(dir(options))
    simulations_list=copy.copy(paths_dict['simulations_list'])
    if len(intersection_simulations)>0:
        for simulation in paths_dict['simulations_list']:
            for desc in intersection_simulations:
                if simulation.split('_')[simulation_desc.index(desc)]!=getattr(options,desc):
                    simulations_list.remove(simulation)
                    break
    paths_dict['simulations_list']=simulations_list

    outfile=open(options.out_paths_file,'w')
    json.dump(paths_dict,outfile)
    return

def main():
    import argparse 
    import textwrap

    #Option parser
    description=textwrap.dedent('''\
    This script queries an ESGF archive. It can query a
    local POSIX-based archive following the CMIP5 DRS
    filename convention and directory structure.

    In the future it should become able to query the THREDDS
    catalog of the ESGF and provide a simple interface to
    the OPEnDAP services.
    ''')
    epilog='Frederic Laliberte, Paul Kushner 10/2012'
    version_num='0.2'
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            version='%(prog)s '+version_num,
                            epilog=epilog)

    subparsers = parser.add_subparsers(help='commands',dest='command')
    function_command={}

    #Find Optimset
    epilog_optimset=textwrap.dedent(epilog+'\n\nThe output can be pretty printed by using:\n\
                          cat out_paths_file | python -mjson.tool')
    optimset_parser=subparsers.add_parser('optimset',
                                           help='Returns pointers to models that have all the requested experiments and variables.\n\
                                                 It is good practice to check the results with \'simulations\' before\n\
                                                 proceeding with \'optimset_months\'.',
                                           epilog=epilog_optimset,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    function_command['optimset']=find_optimset
    optimset_parser.add_argument('in_diagnostic_headers_file',
                                 type=argparse.FileType('r'),
                                 help='Diagnostic headers file (input)')
    optimset_parser.add_argument('out_paths_file',
                                 help='Diagnostic paths file (output)')
    #Find Optimset Months
    epilog_optimset_months=textwrap.dedent(epilog+'\n\nThe output can be pretty printed by using:\n\
                          cat out_paths_file | python -mjson.tool')
    optimset_months_parser=subparsers.add_parser('optimset_months',
                                           help='Take as an input the results from \'optimset\'.\n\
                                                 Returns pointers to models that have all the\n\
                                                 requested experiments and variables for all requested years.\n\
                                                 It is required to use the \'retrieve\' command.\n\
                                                 It can be slow, particularly if \'OPeNDAP\' files are\n\
                                                 requested.',
                                           epilog=epilog_optimset_months,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    function_command['optimset_months']=find_optimset_months
    optimset_months_parser.add_argument('in_diagnostic_headers_file',
                                 type=argparse.FileType('r'),
                                 help='Diagnostic headers file (input)')
    optimset_months_parser.add_argument('out_paths_file',
                                 help='Diagnostic paths file (output)')
    optimset_months_parser.add_argument('-z','--gzip',
                                 default=False, action='store_true',
                                 help='Compress the output using gzip. Because the output is mostly repeated text, this leads to large compression.')

    #Retrieve data
    retrieve_parser=subparsers.add_parser('retrieve',
                                           help='Retrieve a path (on file system or url) to files containing:\n\
                                                 a set of variables from a single month, year, model, experiment.\n\
                                                 It retrieves the latest version.',
                                           )
    function_command['retrieve']=retrieval
    retrieve_parser.add_argument('center',type=str,help='Modelling center name')
    retrieve_parser.add_argument('model',type=str,help='Model name')
    retrieve_parser.add_argument('experiment',type=str,help='Experiment name')
    retrieve_parser.add_argument('rip',type=str,help='RIP identifier, e.g. r1i1p1')
    retrieve_parser.add_argument('var',type=str,help='Variable name, e.g. tas')
    retrieve_parser.add_argument('frequency',type=str,help='Frequency, e.g. day')
    retrieve_parser.add_argument('realm',type=str,help='Realm, e.g. atmos')
    retrieve_parser.add_argument('mip',type=str,help='MIP table name, e.g. day')
    retrieve_parser.add_argument('year',type=int,help='Year')
    retrieve_parser.add_argument('month',type=int,help='Month as an integer ranging from 1 to 12')
    retrieve_parser.add_argument('in_diagnostic_pointers_file',
                                 help='Diagnostic headers file with data pointers (input)')

    retrieve_parser.add_argument('-f','--file_type_flag',
                                 default=False,
                                 action='store_true',
                                 help='prints only the file_type if selected')

    #Slice data
    slice_parser=subparsers.add_parser('slice',
                                           help='Slice the data according the passed keywords.',
                                           argument_default=argparse.SUPPRESS
                                           )
    function_command['slice']=slice_data
    slice_parser.add_argument('--center',type=str,help='Modelling center name')
    slice_parser.add_argument('--model',type=str,help='Model name')
    slice_parser.add_argument('--experiment',type=str,help='Experiment name')
    slice_parser.add_argument('--rip',type=str,help='RIP identifier, e.g. r1i1p1')
    slice_parser.add_argument('--var',type=str,help='Variable name, e.g. tas')
    slice_parser.add_argument('--frequency',type=str,help='Frequency, e.g. day')
    slice_parser.add_argument('--realm',type=str,help='Realm, e.g. atmos')
    slice_parser.add_argument('--mip',type=str,help='MIP table name, e.g. day')
    slice_parser.add_argument('--year',type=int,help='Year')
    slice_parser.add_argument('--month',type=int,help='Month as an integer ranging from 1 to 12')
    slice_parser.add_argument('in_diagnostic_pointers_file',
                                 help='Diagnostic headers file with data pointers (input)')
    slice_parser.add_argument('out_paths_file',
                                 help='Diagnostic paths file (output)')

    slice_parser.add_argument('--remove',
                                 default=False,
                                 action='store_true',
                                 help='Deletes only the indentified data instead of keeping only the identified data.')

    #Simulations
    simulations_parser=subparsers.add_parser('simulations',
                                           help='Prints the (center_model_rip) triples available in the pointers file.'
                                           )
    function_command['simulations']=simulations
    simulations_parser.add_argument('in_diagnostic_pointers_file',
                                 help='Diagnostic headers file with data pointers (input)')

    options=parser.parse_args()

    #Run the command:
    function_command[options.command](options)

if __name__ == "__main__":
    #import cProfile
    #cProfile.run('main()','optimset.prof')
    main()
