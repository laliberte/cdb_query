import os
import hashlib

import filesystem_query
import esgf_query

import valid_experiments_path
import valid_experiments_time

import copy

#import database_utils
import json
import gzip

import tree_utils
from tree_utils import File_Expt

def open_json(options):
    if options.in_diagnostic_headers_file[-3:]=='.gz':
        infile=gzip.open(options.in_diagnostic_headers_file,'r')
    else:
        infile=open(options.in_diagnostic_headers_file,'r')
    paths_dict=json.load(infile)
    if options.drs!=None:
        paths_dict['header']['drs']=options.drs
    return paths_dict

def close_json(paths_dict,options):
    if options.gzip:
        outfile = gzip.open(options.out_diagnostic_headers_file+'.gz','w')
    else:
        outfile = open(options.out_diagnostic_headers_file,'w')

    json.dump({'pointers':paths_dict.pointers.tree,'header':paths_dict.header},outfile)
    outfile.close()
    return

class SimpleTree:
    def __init__(self,tree):

        if 'header' in tree.keys():
            self.header =  tree['header']

        #Create tree
        self.pointers=tree_utils.Tree(self.header['drs'])

        if 'pointers' in tree.keys():
            self.pointers.tree=tree['pointers']
        return

    def union_header(self):
        #This method creates a simplified header

        #Create the diagnostic description dictionary:
        self.header_simple={}
        #Find all the requested realms, frequencies and variables:
        variable_list=['var_list','frequency_list','realm_list','mip_list']
        for list_name in variable_list: self.header_simple[list_name]=[]
        for var_name in self.header['variable_list'].keys():
            self.header_simple['var_list'].append(var_name)
            for list_id, list_name in enumerate(list(variable_list[1:])):
                self.header_simple[list_name].append(self.header['variable_list'][var_name][list_id])

        #Find all the requested experiments and years:
        experiment_list=['experiment_list','years_list']
        for list_name in experiment_list: self.header_simple[list_name]=[]
        for experiment_name in self.header['experiment_list'].keys():
            self.header_simple['experiment_list'].append(experiment_name)
            for list_name in list(experiment_list[1:]):
                self.header_simple[list_name].append(self.header['experiment_list'][experiment_name])
                
        #Find the unique members:
        for list_name in self.header_simple.keys(): self.header_simple[list_name]=list(set(self.header_simple[list_name]))
        return

    def simulations_list(self):
        simulations_list=self.pointers.session.query(
                                                         File_Expt.center,
                                                         File_Expt.model,
                                                         File_Expt.rip
                                                        ).distinct().all()
        return simulations_list

    def optimset(self,options):
        #First simplify the header
        self.union_header()

        #Local filesystem archive
        local_paths=[search_path for search_path in 
                        self.header['search_list']
                        if os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
                        #[ os.path.abspath(os.path.expanduser(os.path.expandvars(path))) for path in self.header['search_list']]
        for search_path in local_paths:
            filesystem_query.descend_tree(self.pointers,self.header,self.header_simple,search_path)


        #ESGF search
        remote_paths=[search_path for search_path in 
                        self.header['search_list']
                        if not os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
                        #[ os.path.expanduser(os.path.expandvars(path)) for path in self.header['search_list']]
        for search_path in remote_paths:
            esgf_query.descend_tree(self.pointers,self.header,search_path,options)

            
        #Redefine the DRS:
        self.header['drs']=[
                'center','model','experiment','rip','frequency','realm','mip',
                'var','time','version','search','file_type','path'
                 ]

        valid_experiments_path.intersection(self)
        return

    def optimset_time(self,options):
        #First slice the input:
        self.pointers.slice(options)

        #Find the list of center / model with all the months for all the years / experiments and variables requested:
        self.header['drs']=[
                                'experiment','center','model','rip','frequency','realm','mip',
                                'var','time','version','file_type','search','path'
                             ]
        valid_experiments_time.intersection(self)

        #Find the unique tree:
        self.pointers.simplify(self.header)
        return

    def slice(self,options):
        self.pointers.slice(options)
        return

    def simulations(self,options):
        self.pointers.slice(options)
        self.pointers.create_database(find_simulations)

        simulations_list=self.simulations_list()
        for simulation in simulations_list:
            if simulation[2]!='r0i0p0':
                print '_'.join(simulation)
        return

    def list_paths(self,options):
        #slice with options:
        self.pointers.slice(options)
        for path in sorted(list(set(self.pointers.level_list_last()))):
            if 'wget' in dir(options) and options.wget:
                print('\''+
                      '/'.join(path.split('|')[0].split('/')[-10:])+
                      '\' \''+path.split('|')[0]+
                      '\' \'MD5\' \''+path.split('|')[1]+'\'')
            else:
                print path
        return

    def find_local(self,options):
        import database_utils
        self.pointers.slice(options)

        #In the search paths, find which one are local:
        local_search_path=[ top_path for top_path in 
                                [os.path.expanduser(os.path.expandvars(search_path)) for search_path in self.header['search_list']]
                                if os.path.exists(top_path)]
        local_search_path=[ os.path.abspath(top_path) for top_path in local_search_path]

        #For each remote file type, find the paths and see if a local copy exists:
        path_equivalence=dict()
        options.file_type=['HTTPServer','GridFTP']
        pointers_copy=copy.deepcopy(self.pointers.tree)
        self.pointers.slice(options)

        for path in sorted(list(set(self.pointers.level_list_last()))):
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

        #Replace original tree:
        options.file_type=None
        self.pointers.tree=pointers_copy

        self.pointers.replace_last(path_equivalence)
        return
                
def md5_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()

def find_simulations(pointers,file_expt):
    #for item in dir(file_expt):
    #    if item[0]!='_':
    #        print getattr(file_expt,item)
    pointers.session.add(file_expt)
    pointers.session.commit()
    return

def input_arguments(parser):
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file (input)')
    parser.add_argument('out_diagnostic_headers_file',
                                 help='Diagnostic paths file (output)')
    parser.add_argument('-z','--gzip',
                                 default=False, action='store_true',
                                 help='Compress the output using gzip. Because the output is mostly repeated text, this leads to large compression.')
    return

def optimset_slicing_arguments(parser):
    #Define the data slicing arguments in a dictionnary:
    slicing_args={
                  'center': [str,'Modelling center name'],
                  'model': [str,'Model name'],
                  'rip': [str,'RIP identifier, e.g. r1i1p1']
                  }
    for arg in slicing_args.keys():
        parser.add_argument('--'+arg,type=slicing_args[arg][0],help=slicing_args[arg][1])
    return

def slicing_arguments(parser):
    optimset_slicing_arguments(parser)
    #Define the data slicing arguments in a dictionnary:
    slicing_args={
                  'experiment': [str,'Experiment name'],
                  'var': [str,'Variable name, e.g. tas'],
                  'frequency': [str,'Frequency, e.g. day'],
                  'realm': [str,'Realm, e.g. atmos'],
                  'mip': [str,'MIP table name, e.g. day'],
                  'year': [int,'Year'],
                  'month': [int,'Month as an integer ranging from 1 to 12'],
                  'file_type': [str,'File type: OPEnDAP, local_file, HTTPServer, GridFTP']
                  }
    for arg in slicing_args.keys():
        parser.add_argument('--'+arg,type=slicing_args[arg][0],help=slicing_args[arg][1])
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

    #Find Optimset
    epilog_optimset=textwrap.dedent(epilog+'\n\nThe output can be pretty printed by using:\n\
                          cat out_diagnostic_headers_file | python -mjson.tool')
    optimset_parser=subparsers.add_parser('optimset',
                                           help='Returns pointers to models that have all the requested experiments and variables.\n\
                                                 It is good practice to check the results with \'simulations\' before\n\
                                                 proceeding with \'optimset_time\'.',
                                           epilog=epilog_optimset,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    input_arguments(optimset_parser)
    optimset_slicing_arguments(optimset_parser)
    optimset_parser.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processors to query the archive. NOT WORKING YET.')
    optimset_parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')
    optimset_parser.set_defaults(drs=['time','search','file_type','center','model','experiment','frequency','realm','mip','rip','version','var','path'])

    #Find Optimset Months
    epilog_optimset_time=textwrap.dedent(epilog+'\n\nThe output can be pretty printed by using:\n\
                          cat out_diagnostic_headers_file | python -mjson.tool')
    optimset_time_parser=subparsers.add_parser('optimset_time',
                                           help='Take as an input the results from \'optimset\'.\n\
                                                 Returns pointers to models that have all the\n\
                                                 requested experiments and variables for all requested years.\n\
                                                 It is required to use the \'retrieve\' command.\n\
                                                 It can be slow, particularly if \'OPeNDAP\' files are\n\
                                                 requested.',
                                           epilog=epilog_optimset_time,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    input_arguments(optimset_time_parser)
    slicing_arguments(optimset_time_parser)
    optimset_time_parser.set_defaults(drs=None)


    #List_paths
    list_parser=subparsers.add_parser('list_paths',
                                           help='List paths (on file system or url) to files containing:\n\
                                                 ',
                                           )
    slicing_arguments(list_parser)
    list_parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')

    list_parser.add_argument('-w','--wget',
                                 default=False,
                                 action='store_true',
                                 help='Prints the paths with a structure that can be passed to a wget script, with MD5 checksum.')
    list_parser.set_defaults(drs=None)

    #Slice data
    slice_parser=subparsers.add_parser('slice',
                                           help='Slice the data according the passed keywords.',
                                           argument_default=argparse.SUPPRESS
                                           )
    slice_parser.set_defaults(drs=None)
    input_arguments(slice_parser)
    slicing_arguments(slice_parser)

    #find_local
    find_local_parser=subparsers.add_parser('find_local',
                                           help='Find the local files that were downloaded'
                                           )
    find_local_parser.set_defaults(drs=None)
    input_arguments(find_local_parser)
    slicing_arguments(find_local_parser)

    #Simulations
    simulations_parser=subparsers.add_parser('simulations',
                                           help='Prints the (center_model_rip) triples available in the pointers file.'
                                           )
    slicing_arguments(simulations_parser)
    simulations_parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')
    simulations_parser.set_defaults(drs=None)

    options=parser.parse_args()

    #Slicing time is perculiar
    for time_opt in ['year','month']:
        if time_opt in dir(options) and getattr(options,time_opt):
            options.time=True

    #Load pointer file:
    paths_dict=SimpleTree(open_json(options))
    #Run the command:
    getattr(paths_dict,options.command)(options)
    #print paths_dict.pointers.tree
    #Close the file:
    if 'out_diagnostic_headers_file' in dir(options):
        close_json(paths_dict,options)

if __name__ == "__main__":
    main()
