import os
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

class SimpleTree:
    def __init__(self,tree):
        self.pointers=dict()
        self.header=dict()

        if isinstance(tree,dict):
            if 'data_pointers' in tree.keys():
                self.pointers=tree['data_pointers']
            elif 'diagnostic' in tree.keys():
                self.header=tree['diagnostic']

        self.cmip5_drs=['search','center','model','experiment','frequency','realm','mip','rip','version','var']
        return

    def union_header(self):
        #Create the diagnostic description dictionary:
        header_desc={}
        #Find all the requested realms, frequencies and variables:
        variable_list=['var_list','frequency_list','realm_list','mip_list']
        for list_name in variable_list: header_desc[list_name]=[]
        for var_name in self.header['variable_list'].keys():
            header_desc['var_list'].append(var_name)
            for list_id, list_name in enumerate(list(variable_list[1:])):
                header_desc[list_name].append(self.header['variable_list'][var_name][list_id])

        #Find all the requested experiments and years:
        experiment_list=['experiment_list','years_list']
        for list_name in experiment_list: header_desc[list_name]=[]
        for experiment_name in self.header['experiment_list'].keys():
            header_desc['experiment_list'].append(experiment_name)
            for list_name in list(experiment_list[1:]):
                header_desc[list_name].append(self.header['experiment_list'][experiment_name])
                
        #Find the unique members:
        for list_name in header_desc.keys(): header_desc[list_name]=list(set(header_desc[list_name]))
        
        self.header=header_desc
        return

    def find_optimset(self,options):
        import valid_experiments_path

        self.union_headers()
        self.pointers['_name']='search'

        #Local filesystem archive
        local_paths=[search_path for search_path in 
                        [ os.path.expanduser(os.path.expandvars(path)) for path in self.header['search_list']]
                        if os.path.exists(search_path)]
        for path in local_paths:
            self.pointers[search_path]=search_filesystem(
                                                       self.header,
                                                       diag_tree_desc[1:],
                                                       top_path
                                                       )
        #ESGF search
        remote_paths=[search_path for search_path in self.header['search_list']
                            if not os.path.exists(os.path.expanduser(os.path.expandvars(search_path)))]
        for search_path in remote_paths:
            self.pointers[search_path]=search_esgf(search_path,
                                                     self.header['file_type_list'],
                                                     self.header['variable_list'],
                                                     self.header['experiment_list'],
                                                     self.cmip5_drs[1:]
                                                     )
            
        #Find the list of center / model with all experiments and variables requested:
        diag_tree_desc_path=[
                                'center','model','experiment','rip','frequency','realm','mip',
                                'var','version','search','file_type'
                             ]
        self.pointers=valid_experiments_path.intersection(self.pointers,
                                                          diag_tree_desc,
                                                          diag_tree_desc_path
                                                          )
        return

    def find_optimset_months(self,options):
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
        self.pointers=valid_experiments_months.intersection(self.pointers,
                                                         diag_tree_desc_path,
                                                         diag_tree_desc_months)

        #Find the unique tree:
        self.pointers=database_utils.unique_tree(self.pointers,self.headers)
        return

    def simulations_list(self,options):
        import database_utils
        import copy

        simulations_list=[]
        for center in self.pointers.list_level('center'):
            setattr(temp_options,'center',center)
            for model in database_utils.list_level(paths_dict['data_pointers'],temp_options,'model'):
                setattr(temp_options,'model',model)
                for rip in database_utils.list_level(paths_dict['data_pointers'],temp_options,'rip'):
                    if rip!='r0i0p0':
                        simulations_list.append('_'.join([center,model,rip]))
        return simulations_list

def search_filesystem(diag_desc,diag_tree_desc,top_path):
    import filesystem_query
    return filesystem_query.descend_tree(diag_desc,diag_tree_desc,top_path)

def search_esgf(search_path,file_type_list,variable_list,experiment_list,diag_tree_desc):
    import esgf_query
    return esgf_query.descend_tree(search_path,file_type_list,variable_list,experiment_list,diag_tree_desc)

def list_paths(paths_dict,options):
    for path in list_unique_paths(paths_dict,options):
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
    #Apply the user-requested slicing:
    restricted_paths_dict=slice_data(paths_dict,options)

    #In the search paths, find which one are local:
    local_search_path=[ top_path for top_path in 
                            [os.path.expanduser(os.path.expandvars(search_path)) for search_path in paths_dict['diagnostic']['search_list']]
                            if os.path.exists(top_path)]
    local_search_path=[ os.path.abspath(top_path) for top_path in local_search_path]

    #Find the file types in the paths_dict:
    file_types=database_utils.list_level(restricted_paths_dict['data_pointers'],options,'file_type')

    #For each remote file type, find the paths and see if a local copy exists:
    path_equivalence=dict()
    for file_type in set(file_types).intersection(['HTTPServer','GridFTP']):
        options.file_type=file_type
        sliced_paths_dict=slice_data(restricted_paths_dict,options)
        path_names=sorted(set(tree_retrieval(sliced_paths_dict['data_pointers'],options)))
    
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
    options.file_type=None
    converted_paths_dict, options=database_utils.replace_path(restricted_paths_dict['data_pointers'],options,path_equivalence)
    restricted_paths_dict['data_pointers']=converted_paths_dict
    return restricted_paths_dict
                
def md5_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()


def slice_data(paths_dict,options):
    import database_utils
    import copy

    new_paths_dict=copy.deepcopy(paths_dict)
    #Slice the database:
    new_paths_dict['data_pointers']=database_utils.slice_data(new_paths_dict['data_pointers'],options)

    #Remove the simulations that were excluded:
    new_paths_dict['simulations_list']=simulations_list(new_paths_dict,options)
    return new_paths_dict


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

    #Retrieve data
    #retrieve_parser=subparsers.add_parser('retrieve',
    #                                       help='Retrieve a path (on file system or url) to files containing:\n\
    #                                             a set of variables from a single month, year, model, experiment.\n\
    #                                             It retrieves the latest version.',
    #                                       )
    #function_command['retrieve']=retrieval
    #for arg in slicing_args.keys():
    #    retrieve_parser.add_argument(arg,type=slicing_args[arg][0],help=slicing_args[arg][1])
    #retrieve_parser.add_argument('in_diagnostic_headers_file',
    #                             help='Diagnostic headers file with data pointers (input)')
    #
    #retrieve_parser.add_argument('-f','--file_type_flag',
    #                             default=False,
    #                             action='store_true',
    #                             help='Prints only the file_type if selected')

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
