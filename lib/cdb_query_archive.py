import os
#import md5
import hashlib

def open_json(options):
    import json
    if options.in_diagnostic_headers_file[-3:]=='.gz':
        import gzip
        infile=gzip.open(options.in_diagnostic_headers_file,'r')
    else:
        infile=open(options.in_diagnostic_headers_file,'r')
    paths_dict=json.load(infile)
    return paths_dict

def close_json(paths_dict,options):
    import json
    if options.gzip:
        import gzip
        outfile = gzip.open(options.out_paths_file+'.gz','w')
    else:
        outfile = open(options.out_paths_file,'w')

    json.dump(paths_dict,outfile)
    outfile.close()
    return

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
def find_optimset(in_paths_dict,options):
    import valid_experiments_path

    #Attribute the loaded path description:
    paths_dict={}
    paths_dict['diagnostic']=in_paths_dict

    #Tree description following the CMIP5 DRS:
    diag_tree_desc=['search','center','model','experiment','frequency','realm','mip','rip',
                    'version','var']
    
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

    return paths_dict

def search_filesystem(diag_desc,diag_tree_desc,top_path):
    import filesystem_query
    return filesystem_query.descend_tree(diag_desc,diag_tree_desc,top_path)

def search_esgf(search_path,file_type_list,variable_list,experiment_list,diag_tree_desc):
    import esgf_query
    return esgf_query.descend_tree(search_path,file_type_list,variable_list,experiment_list,diag_tree_desc)

def find_optimset_months(paths_dict,options):
    import valid_experiments_months
    import database_utils
    diag_tree_desc_path=[
                            'center','model','experiment','rip','frequency','realm','mip',
                            'var','version','search','file_type'
                         ]
    #Find the list of center / model with all the months for all the years / experiments and variables requested:
    diag_tree_desc_months=[
                            'experiment','center','model','rip','frequency','realm','mip',
                            'var','year','month','version','file_type','search'
                         ]
    paths_dict=valid_experiments_months.intersection(paths_dict,diag_tree_desc_path,diag_tree_desc_months)

    #Find the unique tree:
    paths_dict=database_utils.unique_tree(paths_dict,paths_dict['diagnostic'])
    return paths_dict

def list_paths(paths_dict,options):
    list_of_paths=list_unique_paths(paths_dict,options)
    if not list_of_paths is None:
        for path in list_of_paths:
            print path
    return

def list_unique_paths(paths_dict,options):
    sliced_paths_dict=slice_data(paths_dict,options)
    
    #Go down the tree and retrieve requested fields:
    if sliced_paths_dict['data_pointers']==None: return

    path_names=tree_retrieval(sliced_paths_dict['data_pointers'],options)
    unique_paths=[]
    for path in sorted(set(path_names)):
        if 'wget' in dir(options) and options.wget:
            unique_paths.append('\''+'/'.join(path.split('|')[0].split('/')[-10:])+'\' \''+path.split('|')[0]+'\' \'MD5\' \''+path.split('|')[1]+'\'')
        else:
            unique_paths.append(path)
    return unique_paths

def tree_retrieval(paths_dict,options):
    path_names=[]
    #Reads the tree recursively:
    if isinstance(paths_dict,dict):
        #Read the level name:
        level_name=paths_dict['_name']
        for level in paths_dict.keys():
            if level[0]!='_':
                setattr(options,level_name,level)
                path_names.extend(tree_retrieval(paths_dict[level],options))
    else:
        path_names=[paths_dict[0]]
    return path_names

def find_local(paths_dict,options):
    import database_utils

    #In the search paths, find which one are local:
    local_search_path=[ top_path for top_path in 
                            [os.path.expanduser(os.path.expandvars(search_path)) for search_path in paths_dict['diagnostic']['search_list']]
                            if os.path.exists(top_path)]
    local_search_path=[ os.path.abspath(top_path) for top_path in local_search_path]

    path_names=list_unique_paths(paths_dict,options)
    path_equivalence=dict()
    for path in path_names:
        path_equivalence[path]=None
        for search_path in local_search_path:
            md5checksum=path.split('|')[1]
            path_to_file=search_path+'/'+'/'.join(path.split('|')[0].split('/')[-10:])
            try:
                f=open(path_to_file)
                if md5_for_file(f)==md5checksum:
                    path_equivalence[path]=path_to_file
                    break
            except:
                pass

    restricted_paths_dict=paths_dict
    restricted_paths_dict['data_pointers'], options=database_utils.replace_path(paths_dict['data_pointers'],options,path_equivalence)
    return restricted_paths_dict
                
def md5_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()

def simulations(paths_dict,options):
    for item in paths_dict['simulations_list']:
        print item
    return

def slice_data(paths_dict,options):
    import database_utils
    import copy

    new_paths_dict=copy.deepcopy(paths_dict)
    #Slice the database:
    new_paths_dict['data_pointers']=database_utils.slice_data(new_paths_dict['data_pointers'],options)

    #Remove the simulations that were excluded:
    new_paths_dict['simulations_list']=simulations_list(new_paths_dict,options)
    return new_paths_dict

def simulations_list(paths_dict,options):
    import database_utils
    import copy
    temp_options=copy.deepcopy(options)
    simulations_list=[]
    for center in database_utils.list_level(paths_dict['data_pointers'],temp_options,'center'):
        setattr(temp_options,'center',center)
        for model in database_utils.list_level(paths_dict['data_pointers'],temp_options,'model'):
            setattr(temp_options,'model',model)
            for rip in database_utils.list_level(paths_dict['data_pointers'],temp_options,'rip'):
                if rip!='r0i0p0':
                    simulations_list.append('_'.join([center,model,rip]))
    return simulations_list

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
                                 help='Diagnostic headers file (input)')
    optimset_parser.add_argument('out_paths_file',
                                 help='Diagnostic paths file (output)')
    optimset_parser.add_argument('-z','--gzip',
                                 default=False, action='store_true',
                                 help='Compress the output using gzip. Because the output is mostly repeated text, this leads to large compression.')
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
                                 help='Diagnostic headers file (input)')
    optimset_months_parser.add_argument('out_paths_file',
                                 help='Diagnostic paths file (output)')
    optimset_months_parser.add_argument('-z','--gzip',
                                 default=False, action='store_true',
                                 help='Compress the output using gzip. Because the output is mostly repeated text, this leads to large compression.')

    #Define the data slicing arguments in a dictionnary:
    slicing_args={
                  'center': [str,'Modelling center name'],
                  'model': [str,'Model name'],
                  'experiment': [str,'Experiment name'],
                  'rip': [str,'RIP identifier, e.g. r1i1p1'],
                  'var': [str,'Variable name, e.g. tas'],
                  'frequency': [str,'Frequency, e.g. day'],
                  'realm': [str,'Realm, e.g. atmos'],
                  'mip': [str,'MIP table name, e.g. day'],
                  'year': [int,'Year'],
                  'month': [int,'Month as an integer ranging from 1 to 12'],
                  'file_type': [str,'File type: OPEnDAP, local_file, HTTPServer, GridFTP']
                  }

    #List_paths
    list_parser=subparsers.add_parser('list_paths',
                                           help='List paths (on file system or url) to files containing:\n\
                                                 ',
                                           )
    function_command['list_paths']=list_paths
    for arg in slicing_args.keys():
        list_parser.add_argument('--'+arg,type=slicing_args[arg][0],help=slicing_args[arg][1])
    list_parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')

    list_parser.add_argument('-w','--wget',
                                 default=False,
                                 action='store_true',
                                 help='Prints the paths with a structure that can be passed to a wget script, with MD5 checksum.')

    #Slice data
    slice_parser=subparsers.add_parser('slice',
                                           help='Slice the data according the passed keywords.',
                                           argument_default=argparse.SUPPRESS
                                           )
    function_command['slice']=slice_data
    for arg in slicing_args.keys():
        slice_parser.add_argument('--'+arg,type=slicing_args[arg][0],help=slicing_args[arg][1])
    slice_parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')
    slice_parser.add_argument('out_paths_file',
                                 help='Diagnostic paths file (output)')
    slice_parser.add_argument('-z','--gzip',
                                 default=False, action='store_true',
                                 help='Compress the output using gzip. Because the output is mostly repeated text, this leads to large compression.')

    #find_local
    find_local_parser=subparsers.add_parser('find_local',
                                           help='Find the local files that were downloaded'
                                           )
    function_command['find_local']=find_local
    for arg in slicing_args.keys():
        find_local_parser.add_argument('--'+arg,type=slicing_args[arg][0],help=slicing_args[arg][1])
    find_local_parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')
    find_local_parser.add_argument('out_paths_file',
                                 help='Diagnostic paths file (output)')
    find_local_parser.add_argument('-z','--gzip',
                                 default=False, action='store_true',
                                 help='Compress the output using gzip. Because the output is mostly repeated text, this leads to large compression.')


    #Simulations
    simulations_parser=subparsers.add_parser('simulations',
                                           help='Prints the (center_model_rip) triples available in the pointers file.'
                                           )
    function_command['simulations']=simulations
    simulations_parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')


    options=parser.parse_args()

    #Load pointer file:
    paths_dict=open_json(options)
    #Run the command:
    paths_dict=function_command[options.command](paths_dict,options)
    #Close the file:
    if not paths_dict==None:
        close_json(paths_dict,options)

if __name__ == "__main__":
    main()
