#External:
import argparse 
import textwrap
import datetime
import copy
import os
import numpy as np

#External but related:
import netcdf4_soft_links.manage_soft_links_parsers as manage_soft_links_parsers

def absolute_path(path):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(path)))

def input_arguments(parser,project_drs):
    parser.add_argument('in_netcdf_file',type=absolute_path,
                                 help='NETCDF Diagnostic paths file (input)')
    return

def output_arguments(parser,project_drs):
    parser.add_argument('out_netcdf_file',type=absolute_path,
                                 help='NETCDF Diagnostic paths file (output)')
    return

def processing_arguments(parser,project_drs):
    proc_group = parser.add_argument_group('These arguments set threading options and swap space')
    proc_group.add_argument('--num_procs',
                                 default=1, type=int,
                                 help=textwrap.dedent(
                                 '''If num_procs>1:
                                    (1) Use num_procs-1 processes to perform the (ask, validate, download_files, reduce_soft_links) suite of functions, if they are requested,
                                    (2) Use num_procs-1 processes to perform the download_opendap, if requested,
                                    (3) Use num_procs-1 processes to perform the reduce function, if requested,
                                    (4) Use 1 process to perform record functions.
                                    As a rule of thumb, setting num_procs ~ (2/3)*(number of compute cores) should result in full usage of computing capacity.
                                  '''))
    return

def functions_arguments(parser,functions_list):
    authorized_functions=['ask','validate',
                          'download_files','reduce_soft_links',
                          'download_opendap','reduce']
    for function in authorized_functions:
        if function in functions_list:
            parser.add_argument('--'+function, default=True,help=argparse.SUPPRESS)
        else:
            parser.add_argument('--'+function, default=False,help=argparse.SUPPRESS)
    return

def basic_control_arguments(parser,project_drs):
    parser.add_argument('-s','--silent',default=False,action='store_true',help='Make not verbose.')
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception. Default: 3.')
    parser.add_argument('--not_failsafe',default=False,action='store_true',help='Disable the failsafe feature.')
    parser.add_argument('--log_files',default=False,action='store_true',help='Create one log file per process.')
    parser.add_argument('--swap_dir',type=writeable_dir,default='.',
                                 help='Use this directory as a swap directory.')
    return

def slicing_arguments(parser,project_drs,exclude_args=[],action_type='store'):
    #Define the data slicing arguments in a dictionnary:
    for arg in project_drs.slicing_args.keys():
        if not arg in exclude_args:
            parser.add_argument('--'+arg,action=action_type,
                                type=project_drs.slicing_args[arg][0],
                                help=project_drs.slicing_args[arg][1]
                                )
    return

def excluded_slicing_arguments(parser,project_drs,exclude_args=[],action_type='store'):
    #Define the data slicing arguments in a dictionnary:
    for arg in project_drs.slicing_args.keys():
        if not arg in exclude_args:
            parser.add_argument('--'+'X'+arg,action=action_type,
                                type=project_drs.slicing_args[arg][0],
                                help='Exclude '+project_drs.slicing_args[arg][1]
                                )
    return

def basic_slicing(parser,project_drs):
    inc_group = parser.add_argument_group('Inclusions')
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions (inactive in ESGF search)')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    return

def complex_slicing(parser,project_drs,action_type='append'):
    comp_group = parser.add_argument_group('Complex Query')
    complex_query_type=(lambda x: tuple(x.split(',')))
    comp_group.add_argument('--complex_query',action=action_type,type=complex_query_type,
                        help='Complex inclusion with fields specified by --field',
                        default=[])
    comp_group.add_argument('--Xcomplex_query',action=action_type, type=complex_query_type,
                        help='Complex exclusion with fields specified by --field.',
                        default=[])
    return comp_group

def complex_slicing_with_fields(parser,project_drs):
    comp_group=complex_slicing(parser,project_drs)
    comp_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.official_drs_no_version,
                                           help='Complex query fields.' )
    return

def record_validate(parser,project_drs):
    parser.add_argument('--record_validate',default=False,action='store_true',help='Record validate results in out_netcdf_file.validate')
    return

def ask_shared_arguments(parser,project_drs):
    query_group = parser.add_argument_group('Scientific query setup')
    default_experiment=['historical:1950,2005',]
    query_group.add_argument('--Experiment',
                             default=default_experiment,
                             nargs='*',
                             help='A list of \'experiment:start_year,end_year\' triples.\n\
                                   Note that specifiying 1<=start_year<10 means that\n\
                                   the years are relative to the first year in the simulation.\n\
                                   For example, \'piControl:1,101\' will find the first hundred\n\
                                   years of the piControl experiment.\n\
                                   Default {0}'.format(' '.join(default_experiment)))
    query_group.add_argument('--Month',
                             default=range(1,13),
                             type=int,
                             choices=range(1,13),
                             nargs='*',
                             help='Months to be considered. Default: All months.')
    search_path_list=[
    'https://esgf-index1.ceda.ac.uk/esg-search/',
    'https://esgf-node.ipsl.upmc.fr/esg-search/',
    'https://esgf-data.dkrz.de/esg-search/',
    'https://pcmdi.llnl.gov/esg-search/',
    'https://esgf-node.jpl.nasa.gov/esg-search/',
    'https://esg-dn1.nsc.liu.se/esg-search/'
    ]
    query_group.add_argument('--Search_path',
                             default=search_path_list,
                             nargs='*',
                             help='List of search paths. Can be a local directory, an ESGF index node, a FTP server.\n\
                                   Default: {0}'.format(' '.join(search_path_list)))
    query_group.add_argument('--XSearch_path',
                             default=[],
                             nargs='*',
                             help='List of search paths to exclude.')
    default_var=['tas:mon,atmos,Amon',]
    query_group.add_argument('--Var',
                             default=default_var,
                             nargs='*',
                             help='A list of \'variable:time_frequency,realm,cmor_table\' tuples.\n\
                                   Default: {0}'.format(' '.join(default_var)))
    file_type_list=['local_file','OPENDAP','HTTPServer']
    query_group.add_argument('--File_type',
                             default=file_type_list,
                             choices=file_type_list,
                             nargs='*',
                             help='A list of \'variable:time_frequency,realm,cmor_table\' tuples.\n\
                                   Default: {0}'.format(' '.join(file_type_list)))

    query_group.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')
    query_group.add_argument('--ask_cache',help='Cache file for ask queries')
    return

def ask_chained_arguments(parser,project_drs):
    parser.add_argument('--list_only_field',default=None, help=argparse.SUPPRESS)
    return

def ask_process_arguments(parser,project_drs):
    parser.add_argument('--list_only_field',default=None, choices=project_drs.remote_fields,
                              help='When this option is used, the ask function prints only the specified field \n\
                                  for which published data COULD match the query. Does nothing to the output file.\n\
                                  Listing separate fields is usually much quicker than the discovery step.')
    return


def related_experiments(parser,project_drs):
    parser.add_argument('--related_experiments',
                                 default=False, action='store_true',
                                 help='When this option is activated, queried experiments are assumed to be related.\n\
                                       In this situation, cdb_query will discard ({0}) tuples that do not have variables for\n\
                                       ALL of the requested experiments'.format(','.join(project_drs.simulations_desc)))
    return

def validate_shared_arguments(parser,project_drs):
    parser.add_argument('--no_check_availability',
                     default=False, action='store_true',
                     help='When this option is activated, checks only that the time stamp is within \n\
                           the requested years and months.')

    parser.add_argument('--missing_years',
                     default=False, action='store_true',
                     help='When this option is activated, do not exclude models if they are missing years.')

    parser.add_argument('--validate_cache',help='Cache file for validate queries')
    return

def reduce_soft_links_chained_arguments(parser,project_drs):
    parser.add_argument('--reduce_soft_links_script',default='',
                                 help='Script to apply to soft links.')

    return 

def reduce_soft_links_process_arguments(parser,project_drs):
    parser.add_argument('reduce_soft_links_script',default='',help="Command-line script")
    return

def fields_selection(parser,project_drs):
    select_group = parser.add_argument_group('These arguments specify the structure of the output')
    select_group.add_argument('--add_fixed',default=False, action='store_true',help='include fixed variables')
    select_group.add_argument('-k','--keep_field',action='append', type=str, choices=project_drs.official_drs_no_version,
                                       default=[],
                                       help='Keep these fields in the applied file.' )
    return

def reduce_process_arguments(parser,project_drs):
    parser.add_argument('in_extra_netcdf_files',nargs='*',
                                 help='NETCDF extra retrieved files (input).')
    return 

def reduce_shared_arguments(parser,project_drs):
    parser.add_argument('script',default='',help="Command-line script")
    parser.add_argument('--out_destination',default='./',
                             help='Destination directory for conversion.')
    parser.add_argument('--start_server',default=False,action='store_true',
                        help='Start a simple server so that reduce will be handled by \'reduce_from_server\'.')

    return

def loop_control(parser,project_drs):
    parser.add_argument('-l','--loop_through_time',action='append', type=str, choices=['year','month','day','hour'],
                                       default=[],
                                       help='Loop through time. This option is a bit tricky.\n\
                                            For example, \'-l year -l month\' loops through all month, one at a time.\n\
                                            \'-l month\', on the other hand, would loop through the 12 months, passing all years\n\
                                            to \'reduce\'.' )
    return parser

def add_dummy_process_parser(parser,description,epilog):
    subparsers = parser.add_subparsers(help='',dest='command_1')
    new_parser=subparsers.add_parser('process',description=description,
                                            formatter_class=argparse.RawTextHelpFormatter,
                                            epilog=epilog)
    new_parser.prog=' '.join(new_parser.prog.split(' ')[:-1])
    return new_parser

#Utilities
def certificates(subparsers,epilog,project_drs):
    description=textwrap.dedent('Recovers ESGF certificates')
    parser=subparsers.add_parser('certificates',
                           description=description,
                           epilog=epilog
                         )
    parser=add_dummy_process_parser(parser,description,epilog)
    manage_soft_links_parsers.certificates_arguments(parser,project_drs)
    return

def list_fields(subparsers,epilog,project_drs):
    #List_paths
    description=textwrap.dedent('List the unique combination of fields in the file.\n\
    For example:\n\
    1) to list the paths, use option \'-f path\'\n\
    2) to list the simulations tuples use options \'-f institute -f model -f ensemble\'\n\
         ').strip()
    parser=subparsers.add_parser('list_fields',
                                            description=description,
                                            formatter_class=argparse.RawTextHelpFormatter,
                                            epilog=epilog
                                           )
    parser=add_dummy_process_parser(parser,description,epilog)
    input_arguments(parser,project_drs)
    select_group = parser.add_argument_group('These arguments specify the structure of the output')
    select_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.base_drs,
                                       help='List the field (or fields if repeated) found in the file' )

    manage_soft_links_parsers.data_node_restriction(parser,project_drs)
    basic_slicing(parser,project_drs)
    complex_slicing(parser,project_drs)
    return

def merge(subparsers,epilog,project_drs):
    #Merge outputs from other functions
    description=textwrap.dedent('Merge the outputs from other functions').strip()
    parser=subparsers.add_parser('merge',
                                            description=description,
                                            formatter_class=argparse.RawTextHelpFormatter,
                                            epilog=epilog
                                           )
    parser=add_dummy_process_parser(parser,description,epilog)
    input_arguments(parser,project_drs)
    parser.add_argument('in_extra_netcdf_files',nargs='*',
                                 help='NETCDF extra files (input).')
    output_arguments(parser,project_drs)
    basic_slicing(parser,project_drs)
    complex_slicing_with_fields(parser,project_drs)
    return

#Functions

def reduce(subparsers,epilog,project_drs):
    epilog_reduce=textwrap.dedent(epilog)
    parser=subparsers.add_parser('reduce',
                                       description=textwrap.dedent('Take as an input retrieved data and reduce bash script'),
                                       epilog=epilog_reduce
                                         )
    functions_arguments(parser,['reduce'])
    return 

def generate_subparsers(parser,epilog,project_drs):
    #Discover tree
    subparsers = parser.add_subparsers(help='Commands to discover available data on the archive',dest='command')

    certificates(subparsers,epilog,project_drs)
    list_fields(subparsers,epilog,project_drs)
    merge(subparsers,epilog,project_drs)

    description=dict()
    arguments_handles=dict()
    start_arguments_handles=dict()
    process_arguments_handles=dict()
    chained_arguments_handles=dict()

    description['ask']=textwrap.dedent(
                            '''Returns pointers to models that have as a subset the requested experiments and variables.\n\
                             It is good practice to check the results with \'list_fields\' before
                             proceeding with \'validate\'.
                             The output of \'validate\' might depend on the order of the header attribute
                             \'data_node_list\' in the output file of \'ask\'. It is good practice to
                             reorder this attribute before proceeding with \'validate\'.
                             
                             Unlike \'validate\' this function should NOT require appropriate certificates
                             to function properly. If it fails it is possible the servers are down.''')
    arguments_handles['ask']=[
                       basic_control_arguments,
                       processing_arguments,
                       manage_soft_links_parsers.certificates_arguments,
                       basic_slicing,
                       ask_shared_arguments,
                       related_experiments,
                       ]
    start_arguments_handles['ask']=[]
    process_arguments_handles['ask']=[ask_process_arguments,output_arguments]
    chained_arguments_handles['ask']=[ask_chained_arguments,]
    description['validate']=textwrap.dedent('''Take as an input the results from \'ask\'.\n\
                             Returns pointers to models that have ALL the\n\
                             requested experiments and variables for ALL requested years.\n\
                             \n\
                             Can be SLOW.\n\
                             \n\
                             Note that if this function fails it is likely that approriate\n\
                             certificates have not been installed on this machine.''')
    arguments_handles['validate']=[
                                   basic_control_arguments,
                                   processing_arguments,
                                   manage_soft_links_parsers.certificates_arguments,
                                   basic_slicing,
                                   complex_slicing_with_fields,
                                   manage_soft_links_parsers.time_selection_arguments,
                                   validate_shared_arguments,
                                   related_experiments,
                                   ]
                                   #fields_selection,
    start_arguments_handles['validate']=[input_arguments,]
    process_arguments_handles['validate']=[output_arguments]
    chained_arguments_handles['validate']=[]
    description['download_files']=textwrap.dedent('''Take as an input the results from \'validate\' and returns a\n\
                                    soft links file with the opendap data filling the database.\n\
                                    Must be called after \'download_files\' in order to prevent missing data.''')
    arguments_handles['download_files']=[
                            basic_control_arguments,
                            manage_soft_links_parsers.certificates_arguments,
                            basic_slicing,
                            complex_slicing_with_fields,
                            fields_selection,
                            manage_soft_links_parsers.data_node_restriction,
                            manage_soft_links_parsers.time_selection_arguments,
                            manage_soft_links_parsers.download_files_arguments_no_io,
                            manage_soft_links_parsers.download_arguments_no_io,
                            ]
    start_arguments_handles['download_files']=[input_arguments,]
    process_arguments_handles['download_files']=[output_arguments]
    chained_arguments_handles['download_files']=[]
    description['reduce_soft_links']=textwrap.dedent('Take as an input retrieved data and reduce_soft_links bash script')
    arguments_handles['reduce_soft_links']=[
                                            basic_control_arguments,
                                            processing_arguments,
                                            basic_slicing,
                                            complex_slicing_with_fields,
                                            fields_selection,
                                            manage_soft_links_parsers.data_node_restriction,
                                            manage_soft_links_parsers.time_selection_arguments,
                                            ]
    start_arguments_handles['reduce_soft_links']=[input_arguments,]
    process_arguments_handles['reduce_soft_links']=[reduce_soft_links_process_arguments,output_arguments]
    chained_arguments_handles['reduce_soft_links']=[reduce_soft_links_chained_arguments]
    description['download_opendap']=textwrap.dedent('''Take as an input the results from \'validate\',
                                \'download_files\' or \'reduce_soft_links\'\n\
                                and returns a soft links file with the opendap data filling the database.\n\
                                Must be called after \'download_files\' in order to prevent missing data.''')
    arguments_handles['download_opendap']=[
                            basic_control_arguments,
                            manage_soft_links_parsers.certificates_arguments,
                            basic_slicing,
                            complex_slicing_with_fields,
                            manage_soft_links_parsers.time_selection_arguments,
                            manage_soft_links_parsers.data_node_restriction,
                            fields_selection,
                            manage_soft_links_parsers.download_opendap_arguments_no_io,
                            manage_soft_links_parsers.download_arguments_no_io,
                            ]
    start_arguments_handles['download_opendap']=[input_arguments,]
    process_arguments_handles['download_opendap']=[output_arguments]
    chained_arguments_handles['download_opendap']=[]

    description['reduce']=textwrap.dedent('Take as an input retrieved data and reduce bash script')
    arguments_handles['reduce']=[basic_control_arguments,
                                 processing_arguments,
                                 basic_slicing,
                                 complex_slicing_with_fields,
                                 fields_selection,
                                 manage_soft_links_parsers.time_selection_arguments,
                                 manage_soft_links_parsers.data_node_restriction,
                                 loop_control,
                                 reduce_shared_arguments,
                                 ]
    start_arguments_handles['reduce']=[input_arguments]
    process_arguments_handles['reduce']=[reduce_process_arguments,output_arguments]
    chained_arguments_handles['reduce']=[]

    childs={'ask':['validate'],
            'validate':['download_files','reduce_soft_links','download_opendap','reduce'],
            'download_files':['reduce_soft_links','download_opendap','reduce'],
            'reduce_soft_links':['download_opendap','reduce'],
            'download_opendap':['reduce'],
            'reduce':[]
            }
    for function_name in childs.keys():
        parser=subparsers.add_parser(function_name,description=description[function_name],epilog=epilog)
        create_subparsers_recursive(parser,epilog,project_drs,[function_name,],childs,description,
                                                              start_arguments_handles,
                                                              arguments_handles,
                                                              process_arguments_handles,
                                                              chained_arguments_handles,[])
    return

def create_subparsers_recursive(parser,epilog,project_drs,functions_list,childs,description,
                                                start_arguments_handles,
                                                  arguments_handles,
                                                  process_arguments_handles,
                                                  chained_arguments_handles,
                                                  previous_arguments_handles):
    function_name=functions_list[-1]

    subparsers = parser.add_subparsers(help='Commands to discover available data on the archive',
                                       dest='command_{0}'.format(len(functions_list)))
    if len(functions_list)==1:
        create_process_subparser(subparsers,project_drs,functions_list,
                                                    arguments_handles[function_name]+
                                                    start_arguments_handles[function_name]+
                                                    process_arguments_handles[function_name])
        current_arguments_handles=(
                                  start_arguments_handles[function_name]+
                                  arguments_handles[function_name])
                                                    
    else:
        create_process_subparser(subparsers,project_drs,functions_list,
                                                    previous_arguments_handles+
                                                    arguments_handles[function_name]+
                                                    process_arguments_handles[function_name])
        current_arguments_handles=arguments_handles[function_name]

    for sub_function_name in childs[function_name]:
        new_parser=subparsers.add_parser(sub_function_name,description=description[sub_function_name],epilog=epilog)
        create_subparsers_recursive(new_parser,epilog,project_drs,
                                    functions_list+[sub_function_name,],childs,description,
                                            start_arguments_handles,
                                              arguments_handles,
                                              process_arguments_handles,
                                              chained_arguments_handles,
                                              previous_arguments_handles+current_arguments_handles+
                                              chained_arguments_handles[function_name])
    return

def create_process_subparser(subparsers,project_drs,functions_list,previous_arguments_handles):
    parser=subparsers.add_parser('process')
    functions_arguments(parser,functions_list)
    parser.prog=' '.join(parser.prog.split(' ')[:-1])

    arguments_attributed=[]
    for argument in previous_arguments_handles:
        if not argument in arguments_attributed:
            argument(parser,project_drs)
            arguments_attributed.append(argument)
    if ('validate' in functions_list and
       'validate' != functions_list[-1]):
       if not record_validate in arguments_attributed:
           record_validate(parser,project_drs)
    return

def writeable_dir(prospective_dir):
  if not os.path.isdir(prospective_dir):
    raise Exception("{0} is not a temporary directory".format(prospective_dir))
  if os.access(prospective_dir, os.W_OK):
    return prospective_dir
  else:
    raise Exception("{0} is not a writable dir".format(prospective_dir))

def int_list(input):
    return [ int(item) for item in input.split(',')]
