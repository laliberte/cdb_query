#External:
import argparse 
import textwrap
import datetime
import copy
import os

#External but related:
import netcdf4_soft_links.manage_soft_links_parsers as manage_soft_links_parsers

def absolute_path(path):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(path)))

def input_arguments_json(parser):
    parser.add_argument('in_headers_file',type=absolute_path,
                                 help='Diagnostic headers file (input)')
    return

def input_arguments(parser):
    parser.add_argument('in_netcdf_file',type=absolute_path,
                                 help='NETCDF Diagnostic paths file (input)')
    return

def output_arguments(parser):
    parser.add_argument('out_netcdf_file',type=absolute_path,
                                 help='NETCDF Diagnostic paths file (output)')
    parser.add_argument('--swap_dir',type=writeable_dir,default='.',
                                 help='Use this directory as a swap directory.')
    return

def processing_arguments(parser,project_drs):
    proc_group = parser.add_argument_group('These arguments set threading options and swap space')
    proc_group.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processes to perform the computation.')

    return proc_group

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

def complex_slicing(parser,project_drs,action_type='store'):
    complex_query_type=(lambda x: tuple(x.split(',')))
    parser.add_argument('--complex_query',action=action_type,type=complex_query_type,
                        help='Complex inclusion with fields specified by --field',
                        default=[])
    parser.add_argument('--Xcomplex_query',action=action_type, type=complex_query_type,
                        help='Complex exclusion with fields specified by --field.',
                        default=[])
    return

def generate_subparsers(parser,epilog,project_drs):
    #Discover tree
    subparsers = parser.add_subparsers(help='Commands to discover available data on the archive',dest='command')

    certificates(subparsers,epilog,project_drs)
    list_fields(subparsers,epilog,project_drs)
    merge(subparsers,epilog,project_drs)

    ask(subparsers,epilog,project_drs)
    validate(subparsers,epilog,project_drs)

    download_files(subparsers,epilog,project_drs)

    #revalidate(subparsers,epilog,project_drs)

    reduce_soft_links(subparsers,epilog,project_drs)

    download_opendap(subparsers,epilog,project_drs)

    reduce(subparsers,epilog,project_drs)

    av(subparsers,epilog,project_drs)
    avdr(subparsers,epilog,project_drs)
    drdr(subparsers,epilog,project_drs)
    avdrdr(subparsers,epilog,project_drs)

    gather(subparsers,epilog,project_drs)

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

def ask_arguments(parser,project_drs):
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

    query_group.add_argument('--related_experiments',
                                 default=False, action='store_true',
                                 help='When this option is activated, queried experiments are assumed to be related.\n\
                                       In this situation, cdb_query will discard ({0}) tuples that do not have variables for\n\
                                       ALL of the requested experiments'.format(','.join(project_drs.simulations_desc)))
    return
    

#QUERY PARSERS
def ask(subparsers,epilog,project_drs):
    #Find data
    epilog_ask=epilog
    parser=subparsers.add_parser('ask',
                       description=textwrap.dedent(
                            '''Returns pointers to models that have as a subset the requested experiments and variables.\n\
                             It is good practice to check the results with \'list_fields\' before
                             proceeding with \'validate\'.
                             The output of \'validate\' might depend on the order of the header attribute
                             \'data_node_list\' in the output file of \'ask\'. It is good practice to
                             reorder this attribute before proceeding with \'validate\'.
                             
                             Unlike \'validate\' this function should NOT require appropriate certificates
                             to function properly. If it fails it is possible the servers are down.'''),
                       epilog=epilog_ask
                     )
    functions_arguments(parser,['ask'])
    parser.add_argument('-s','--silent',default=False,action='store_true',help='Make not verbose.')
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception.')
    parser.add_argument('--log_files',default=False,action='store_true',help='Create one log file per process.')

    #input_arguments_json(parser)
    output_arguments(parser)
    ask_arguments(parser,project_drs)

    parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')

    manage_soft_links_parsers.certificates_arguments(parser,project_drs)
    processing_arguments(parser,project_drs)

    parser.add_argument('--list_only_field',default=None, choices=project_drs.remote_fields,
                              help='When this option is used, the ask function prints only the specified field \n\
                                  for which published data COULD match the query. Does nothing to the output file.\n\
                                  Listing separate fields is usually much quicker than the discovery step.')

    #ask_group = parser.add_argument_group('These arguments specify the query')
    #ask_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.base_drs,
    #                                   help='List the field (or fields if repeated) found in the file' )

    #SIMPLE SLICING:
    inc_group = parser.add_argument_group('Inclusions')
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions (inactive in ESGF search)')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    return parser

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
    input_arguments(parser)
    select_group = parser.add_argument_group('These arguments specify the structure of the output')
    select_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.base_drs,
                                       help='List the field (or fields if repeated) found in the file' )
    #select_group.add_argument('--data_nodes',default=False,action='store_true',
    #                                   help='List the data nodes found in the file. Disables the --field options.' )

    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')

    inc_group = parser.add_argument_group('Inclusions')
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    comp_group = parser.add_argument_group('Complex Query')
    complex_slicing(comp_group,project_drs,action_type='append')
    return

def merge(subparsers,epilog,project_drs):
    #Merge outputs from other functions
    description=textwrap.dedent('Merge the outputs from other functions').strip()
    parser=subparsers.add_parser('merge',
                                            description=description,
                                            formatter_class=argparse.RawTextHelpFormatter,
                                            epilog=epilog
                                           )
    parser.add_argument('in_netcdf_file',type=absolute_path,
                                 help='NETCDF Diagnostic paths file (input)')
    parser.add_argument('in_extra_netcdf_files',nargs='*',
                                 help='NETCDF extra files (input).')
    output_arguments(parser)
    extended_slicing_arguments(parser,project_drs)
    return

def validate(subparsers,epilog,project_drs):
    #Find Optimset Months
    epilog_validate=textwrap.dedent(epilog)
    parser=subparsers.add_parser('validate',
                                   description=textwrap.dedent('Take as an input the results from \'ask\'.\n\
                                         Returns pointers to models that have ALL the\n\
                                         requested experiments and variables for ALL requested years.\n\
                                         \n\
                                         Can be SLOW.\n\
                                         \n\
                                         Note that if this function fails it is likely that approriate\n\
                                         certificates have not been installed on this machine.'),
                                   epilog=epilog_validate,
                                 )

    functions_arguments(parser,['validate'])
    parser.add_argument('-s','--silent',default=False,action='store_true',help='Make not verbose.')
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception.')
    parser.add_argument('--log_files',default=False,action='store_true',help='Create one log file per process.')

    input_arguments(parser)
    output_arguments(parser)

    manage_soft_links_parsers.certificates_arguments(parser,project_drs)
    processing_arguments(parser,project_drs)

    parser.add_argument('--related_experiments',
                                 default=False, action='store_true',
                                 help='When this option is activated, queried experiments are assumed to be related.\n\
                                       In this situation, cdb_query will discard ({0}) tuples that do not have variables for\n\
                                       ALL of the requested experiments'.format(','.join(project_drs.simulations_desc)))

    validate_arguments(parser,project_drs)

    extended_slicing_arguments(parser,project_drs)

    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')
    return

def validate_arguments(parser,project_drs):
    parser.add_argument('--no_check_availability',
                     default=False, action='store_true',
                     help='When this option is activated, checks only that the time stamp is within \n\
                           the requested years and months.')

    parser.add_argument('--missing_years',
                     default=False, action='store_true',
                     help='When this option is activated, do not exclude models if they are missing years.')
    #parser.add_argument('--num_dl',default=1,type=int,help='Number of simultaneous download from EACH data node. Default=1.')
    return

def extended_slicing_arguments(parser,project_drs):
    inc_group = parser.add_argument_group('Inclusions')
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    comp_group = parser.add_argument_group('Complex Query')
    comp_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.official_drs_no_version,
                                       help='Complex query fields.' )
    complex_slicing(comp_group,project_drs,action_type='append')
    return 

def download_files(subparsers,epilog,project_drs):
    parser=manage_soft_links_parsers.download_files(subparsers,epilog,project_drs)
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception.')
    parser.add_argument('--swap_dir',type=writeable_dir,default='.',
                                 help='Use this directory as a swap directory.')
    functions_arguments(parser,['download_files'])

    extended_slicing_arguments(parser,project_drs)
    return

def reduce_soft_links(subparsers,epilog,project_drs):
    epilog_reduce_soft_links=textwrap.dedent(epilog)
    parser=subparsers.add_parser('reduce_soft_links',
                                       description=textwrap.dedent('Take as an input retrieved data and reduce_soft_links bash script'),
                                       epilog=epilog_reduce_soft_links
                                         )
    functions_arguments(parser,['reduce_soft_links'])
    parser.add_argument('reduce_soft_links_script',default='',help="Command-line script")
    
    input_arguments(parser)
    output_arguments(parser)
    processing_arguments(parser,project_drs)

    reduce_soft_links_arguments(parser,project_drs)

    manage_soft_links_parsers.time_selection_arguments(parser)
    extended_slicing_arguments(parser,project_drs)

    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')
    return 

def reduce_soft_links_arguments(parser,project_drs):
    parser.add_argument('--reduce_soft_links_script',default='',
                                 help='Script to apply to soft links.')

    select_group = parser.add_argument_group('These arguments specify the structure of the output')
    select_group.add_argument('--add_fixed',default=False, action='store_true',help='include fixed variables')
    select_group.add_argument('-k','--keep_field',action='append', type=str, choices=project_drs.official_drs_no_version,
                                       default=[],
                                       help='Keep these fields in the applied file.' )
    return select_group

def download_opendap(subparsers,epilog,project_drs):
    parser=manage_soft_links_parsers.download_opendap(subparsers,epilog,project_drs)
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception.')
    parser.add_argument('--swap_dir',type=writeable_dir,default='.',
                                 help='Use this directory as a swap directory.')
    functions_arguments(parser,['download_opendap'])

    extended_slicing_arguments(parser,project_drs)
    return

def gather(subparsers,epilog,project_drs):
    epilog_gather=textwrap.dedent(epilog)
    #Load is essentially a special case of reduce
    parser=subparsers.add_parser('gather',
                                       description=textwrap.dedent('Take as an input retrieved data and load bash script'),
                                       epilog=epilog_gather
                                         )
    functions_arguments(parser,['reduce'])
    parser.add_argument('--script',default='',help=argparse.SUPPRESS)
    
    input_arguments(parser)
    output_arguments(parser)
    parser.add_argument('--out_destination',default='./',
                             help='Destination directory for conversion.')
    parser.add_argument('-l','--loop_through_time',action='append', type=str, choices=['year','month','day','hour'],
                                       default=[],
                                       help='Loop through time. This option is a bit tricky.\n\
                                            For example, \'-l year -l month\' loops through all month, one at a time.\n\
                                            \'-l month\', on the other hand, would loop through the 12 months, passing all years\n\
                                            to \'reduce\'.' )

    processing_arguments(parser,project_drs)

    manage_soft_links_parsers.time_selection_arguments(parser)
    extended_slicing_arguments(parser,project_drs)

    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')
    return

def reduce(subparsers,epilog,project_drs):
    epilog_reduce=textwrap.dedent(epilog)
    parser=subparsers.add_parser('reduce',
                                       description=textwrap.dedent('Take as an input retrieved data and reduce bash script'),
                                       epilog=epilog_reduce
                                         )
    functions_arguments(parser,['reduce'])
    parser.add_argument('-s','--silent',default=False,action='store_true',help='Make not verbose.')
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception.')
    parser.add_argument('--log_files',default=False,action='store_true',help='Create one log file per process.')
    parser.add_argument('script',default='',help="Command-line script")
    
    input_arguments(parser)
    output_arguments(parser)
    proc_group=processing_arguments(parser,project_drs)
    proc_group.add_argument('--serial',default=False,action='store_true',help='Downloads the files serially.')

    reduce_arguments(parser,project_drs)

    manage_soft_links_parsers.time_selection_arguments(parser)
    extended_slicing_arguments(parser,project_drs)

    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')
    return 

def reduce_arguments(parser,project_drs):
    parser.add_argument('in_extra_netcdf_files',nargs='*',
                                 help='NETCDF extra retrieved files (input).')
    parser.add_argument('--out_destination',default='./',
                             help='Destination directory for conversion.')

    select_group = parser.add_argument_group('These arguments specify the structure of the output')
    select_group.add_argument('--add_fixed',default=False, action='store_true',help='include fixed variables')
    select_group.add_argument('-k','--keep_field',action='append', type=str, choices=project_drs.official_drs_no_version,
                                       default=[],
                                       help='Keep these fields in the applied file.' )
    select_group.add_argument('-l','--loop_through_time',action='append', type=str, choices=['year','month','day','hour'],
                                       default=[],
                                       help='Loop through time. This option is a bit tricky.\n\
                                            For example, \'-l year -l month\' loops through all month, one at a time.\n\
                                            \'-l month\', on the other hand, would loop through the 12 months, passing all years\n\
                                            to \'reduce\'.' )
    return select_group


def av(subparsers,epilog,project_drs):
    epilog_av=textwrap.dedent(epilog)
    parser=subparsers.add_parser('av',
                                       description=textwrap.dedent('Ask -> Validate'),
                                       epilog=epilog_av
                                         )
    functions_arguments(parser,['ask','validate'])

    #ASK
    ask_arguments(parser,project_drs)
    output_arguments(parser)
    parser.add_argument('--list_only_field',default=None, choices=project_drs.remote_fields,
                              help=argparse.SUPPRESS)

    manage_soft_links_parsers.certificates_arguments(parser,project_drs)
    processing_arguments(parser,project_drs)

    parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')

    parser.add_argument('-s','--silent',default=False,action='store_true',help='Make not verbose.')
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception.')
    parser.add_argument('--log_files',default=False,action='store_true',help='Create one log file per process.')

    #VALIDATE
    validate_arguments(parser,project_drs)
    extended_slicing_arguments(parser,project_drs)

    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')
    return 

def avdr(subparsers,epilog,project_drs):
    epilog_avdr=textwrap.dedent(epilog)
    parser=subparsers.add_parser('avdr',
                                       #description=textwrap.dedent('Ask -> Validate -> Download_files (-> Reduce_soft_links) -> Download_opendap'),
                                       description=textwrap.dedent('Ask -> Validate -> Download_files (-> Reduce_soft_links)'),
                                       epilog=epilog_avdr
                                         )
    functions_arguments(parser,['ask','validate','download_files','reduce_soft_links'])
    #functions_arguments(parser,['ask','validate','download_files'])
    #functions_arguments(parser,['ask','validate'])


    #ASK
    ask_arguments(parser,project_drs)
    output_arguments(parser)
    parser.add_argument('--list_only_field',default=None, choices=project_drs.remote_fields,
                              help=argparse.SUPPRESS)

    manage_soft_links_parsers.certificates_arguments(parser,project_drs)
    processing_arguments(parser,project_drs)

    parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')

    parser.add_argument('-s','--silent',default=False,action='store_true',help='Make not verbose.')
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception.')
    parser.add_argument('--log_files',default=False,action='store_true',help='Create one log file per process.')

    #VALIDATE
    validate_arguments(parser,project_drs)
    extended_slicing_arguments(parser,project_drs)

    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')

    #DOWNLOAD
    serial_group = parser.add_argument_group('Specify asynchronous behavior')
    serial_group.add_argument('--serial',default=False,action='store_true',help='Downloads the files serially.')
    serial_group.add_argument('--num_dl',default=1,type=int,help='Number of simultaneous download from EACH data node. Default=1.')
    if project_drs==None:
        default_dir='.'
    else:
        default_dir='./'+project_drs.project
    parser.add_argument('--out_download_dir',default='.',
                             help='Destination directory for retrieval.')
    parser.add_argument('--do_not_revalidate',default=False,action='store_true',
                        help='Do not revalidate. Only advanced users will use this option.\n\
                              Using this option might can lead to ill-defined time axes.')
    parser.add_argument('--download_all',default=False,action='store_true',help=argparse.SUPPRESS)

    #REDUCE SOFT LINKS
    parser.add_argument('--reduce_soft_links_script',default='',
                                 help='Script to apply to soft links.')
    parser.add_argument('-k','--keep_field',action='append', type=str, choices=project_drs.official_drs_no_version,
                                       default=[],
                                       help='Keep these fields in the applied file.' )

    manage_soft_links_parsers.time_selection_arguments(parser)
    return 

def drdr(subparsers,epilog,project_drs):
    epilog_drdr=textwrap.dedent(epilog)
    parser=subparsers.add_parser('drdr',
                                       description=textwrap.dedent('Download_files (-> Reduce_soft_links)-> Download_opendap -> Reduce'),
                                       epilog=epilog_drdr
                                         )
    functions_arguments(parser,['download_files','reduce_soft_links','download_opendap','reduce'])

    parser.add_argument('-s','--silent',default=False,action='store_true',help='Make not verbose.')
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception.')
    parser.add_argument('--log_files',default=False,action='store_true',help='Create one log file per process.')
    parser.add_argument('script',default='',help="Command-line script")
    input_arguments(parser)

    manage_soft_links_parsers.certificates_arguments(parser,project_drs)
    processing_arguments(parser,project_drs)

    extended_slicing_arguments(parser,project_drs)
    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')

    #DOWNLOAD
    serial_group = parser.add_argument_group('Specify asynchronous behavior')
    serial_group.add_argument('--serial',default=False,action='store_true',help='Downloads the files serially.')
    serial_group.add_argument('--num_dl',default=1,type=int,help='Number of simultaneous download from EACH data node. Default=1.')
    if project_drs==None:
        default_dir='.'
    else:
        default_dir='./'+project_drs.project
    parser.add_argument('--out_download_dir',default='.',
                             help='Destination directory for retrieval.')
    parser.add_argument('--do_not_revalidate',default=False,action='store_true',
                        help='Do not revalidate. Only advanced users will use this option.\n\
                              Using this option might can lead to ill-defined time axes.')
    parser.add_argument('--download_all',default=False,action='store_true',help=argparse.SUPPRESS)

    manage_soft_links_parsers.time_selection_arguments(parser)

    #REDUCE SOFT LINKS
    parser.add_argument('--reduce_soft_links_script',default='',
                                 help='Script to apply to soft links.')

    #REDUCE
    reduce_arguments(parser,project_drs)
    output_arguments(parser)
    return

def avdrdr(subparsers,epilog,project_drs):
    epilog_avdrdr=textwrap.dedent(epilog)
    parser=subparsers.add_parser('avdrdr',
                                       description=textwrap.dedent('Ask -> Validate -> Download_files (-> Reduce_soft_links)-> Download_opendap -> Reduce'),
                                       epilog=epilog_avdrdr
                                         )
    #functions_arguments(parser,['ask','validate','download_files','download_opendap','reduce'])
    functions_arguments(parser,['ask','validate','download_files','reduce_soft_links','download_opendap','reduce'])

    #ASK
    parser.add_argument('script',default='',help="Command-line script")
    ask_arguments(parser,project_drs)
    parser.add_argument('--list_only_field',default=None, choices=project_drs.remote_fields,
                              help=argparse.SUPPRESS)

    manage_soft_links_parsers.certificates_arguments(parser,project_drs)
    processing_arguments(parser,project_drs)

    parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')

    parser.add_argument('-s','--silent',default=False,action='store_true',help='Make not verbose.')
    parser.add_argument('--max_trial',type=int,default=3,help='Try a function that number of time because raising an exception.')
    parser.add_argument('--log_files',default=False,action='store_true',help='Create one log file per process.')
    parser.add_argument('--start_server',default=False,action='store_true',help='Start a simple server so that reduce will be handled by \'reduce_from_server\'.')

    #VALIDATE
    validate_arguments(parser,project_drs)
    extended_slicing_arguments(parser,project_drs)
    parser.add_argument('--record_validate',default=False,action='store_true',help='Record validate results in out_netcdf_file.validate')

    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')

    #DOWNLOAD
    serial_group = parser.add_argument_group('Specify asynchronous behavior')
    serial_group.add_argument('--serial',default=False,action='store_true',help='Downloads the files serially.')
    serial_group.add_argument('--num_dl',default=1,type=int,help='Number of simultaneous download from EACH data node. Default=1.')
    if project_drs==None:
        default_dir='.'
    else:
        default_dir='./'+project_drs.project
    parser.add_argument('--out_download_dir',default='.',
                             help='Destination directory for retrieval.')
    parser.add_argument('--do_not_revalidate',default=False,action='store_true',
                        help='Do not revalidate. Only advanced users will use this option.\n\
                              Using this option might can lead to ill-defined time axes.')
    parser.add_argument('--download_all',default=False,action='store_true',help=argparse.SUPPRESS)

    manage_soft_links_parsers.time_selection_arguments(parser)

    #REDUCE SOFT LINKS
    parser.add_argument('--reduce_soft_links_script',default='',
                                 help='Script to apply to soft links.')

    #REDUCE
    reduce_arguments(parser,project_drs)
    output_arguments(parser)
    return 

def certificates(subparsers,epilog,project_drs):
    manage_soft_links_parsers.certificates(subparsers,epilog,project_drs)
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

