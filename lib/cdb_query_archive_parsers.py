import argparse 
import textwrap
import datetime
import copy

def input_arguments_json(parser):
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file (input)')
    return

def input_arguments(parser):
    parser.add_argument('in_diagnostic_netcdf_file',
                                 help='NETCDF Diagnostic paths file (input)')
    return

def output_arguments(parser):
    parser.add_argument('out_diagnostic_netcdf_file',
                                 help='NETCDF Diagnostic paths file (output)')
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
    #quick_ask(subparsers,epilog,project_drs)
    ask(subparsers,epilog,project_drs)
    list_fields(subparsers,epilog,project_drs)

    #Optimset tree
    validate(subparsers,epilog,project_drs)
    download(subparsers,epilog,project_drs)
    download_raw(subparsers,epilog,project_drs)
    apply(subparsers,epilog,project_drs)
    convert(subparsers,epilog,project_drs)
    certificates(subparsers,epilog,project_drs)
    return

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
    parser.add_argument('--related_experiments',
                                 default=False, action='store_true',
                                 help='When this option is activated, queried experiments are assumed to be related.\n\
                                       In this situation, cdb_query will discard ({0}) tuples that do not have variables for\n\
                                       ALL of the requested experiments'.format(','.join(project_drs.simulations_desc)))
    parser.add_argument('--update',
                                 type=str,action='append',
                                 help='Update the specified file. Will only ask for simulations that were not previously found.')
    input_arguments_json(parser)
    output_arguments(parser)
    parser.add_argument('--list_only_field',default=None, choices=project_drs.remote_fields,
                              help='When this option is used, the ask function prints only the specified field \n\
                                  for which published data COULD match the query. Does nothing to the output file.\n\
                                  Listing separate fields is usually much quicker than the discovery step.')
    parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')
    proc_group = parser.add_argument_group('These arguments set threading options')
    proc_group.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processes to perform the computation.')
    inc_group = parser.add_argument_group('Inclusions')
    slicing_arguments(inc_group,project_drs)
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
    parser.add_argument('--related_experiments',
                                 default=False, action='store_true',
                                 help='When this option is activated, queried experiments are assumed to be related.\n\
                                       In this situation, cdb_query will discard ({0}) tuples that do not have variables for\n\
                                       ALL of the requested experiments'.format(','.join(project_drs.simulations_desc)))

    parser.add_argument('--no_check_availability',
                     default=False, action='store_true',
                     help='When this option is activated, checks only that the time stamp is within \n\
                           the requested years and months.')
    parser.add_argument('--check_queryability',
                     default=False, action='store_true',
                     help='When this option is activated, checks if a file is queryable before proceeding. More robust but slower.')


    input_arguments(parser)
    output_arguments(parser)
    parser.add_argument('--in_diagnostic_headers_file',
                                 help='Alternative diagnostic headers file (to modify target validate)',\
                                 type=str,default=None)

    cert_group = parser.add_argument_group('Use these arguments to let cdb_query manage your ESGF credentials')
    cert_group.add_argument('--username',default=None,
                     help='If you set this value to your user name for registering service given in --service \n\
                           cdb_query will prompt you once for your password and will ensure that your credentials \n\
                           are active for the duration of the process.')
    cert_group.add_argument('--password_from_pipe',default=False,action='store_true',
                        help='If activated it is expected that the user is passing a password through piping\n\
                              Example: echo $PASS | cdb_query_'+project_drs.project+' ...')
    cert_group.add_argument('--service',default='badc',choices=['badc'],
                     help='Registering service. At the moment works only with badc.')


    proc_group = parser.add_argument_group('These arguments set threading options')
    proc_group.add_argument('--num_procs',
                                 default=1, type=int,#choices=xrange(1,6),
                                 help=textwrap.dedent('Use num_procs processes to perform the computation. This function might not work with your installation.'))
                                 #\n\
                                 #      Has been found to be rather unstable with more than 5 processses.'))

    data_node_group = parser.add_argument_group('Restrict search to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')

    #inc_group = parser.add_argument_group('Inclusions')
    #slicing_arguments(inc_group,project_drs)
    #exc_group = parser.add_argument_group('Exclusions')
    #excluded_slicing_arguments(exc_group,project_drs)
    inc_group = parser.add_argument_group('Inclusions')
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    comp_group = parser.add_argument_group('Complex Query')
    comp_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.base_drs,
                                       help='List the field (or fields if repeated) found in the file' )
    complex_slicing(comp_group,project_drs,action_type='append')
    return

def download(subparsers,epilog,project_drs):
    epilog_validate=textwrap.dedent(epilog)
    parser=subparsers.add_parser('download',
                                           description=textwrap.dedent('Take as an input the results from \'validate\' and returns a\n\
                                                 netcdf file with the data retrieved.'),
                                           epilog=epilog_validate,
                                         )
    input_arguments(parser)
    output_arguments(parser)

    cert_group = parser.add_argument_group('Use these arguments to let cdb_query manage your ESGF credentials')
    cert_group.add_argument('--username',default=None,
                     help='If you set this value to your user name for registering service given in --service \n\
                           cdb_query will prompt you once for your password and will ensure that your credentials \n\
                           are active for the duration of the process.')
    cert_group.add_argument('--password_from_pipe',default=False,action='store_true',
                        help='If activated it is expected that the user is passing a password through piping\n\
                              Example: echo $PASS | cdb_query_'+project_drs.project+' ...')
    cert_group.add_argument('--service',default='badc',choices=['badc'],
                     help='Registering service. At the moment works only with badc.')

    serial_group = parser.add_argument_group('Specify asynchronous behavior')
    serial_group.add_argument('--serial',default=False,action='store_true',help='Downloads the files serially.')

    source_group = parser.add_argument_group('Specify sources')
    source_group.add_argument('--source_dir',default=None,help='local cache of data retrieved using \'download_raw\'')

    inc_group = parser.add_argument_group('Inclusions')
    inc_group.add_argument('--year',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated years.')
    inc_group.add_argument('--month',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated months (1 to 12).')
    inc_group.add_argument('--day',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated calendar days.')
    inc_group.add_argument('--previous',
                                 default=False, action='store_true',
                                 help='Retrieve data from specified year, month, day AND the time step just BEFORE this retrieved data.')
    inc_group.add_argument('--next',
                                 default=False, action='store_true',
                                 help='Retrieve data from specified year, month, day AND the time step just AFTER this retrieved data.')
    #slicing_arguments(inc_group,project_drs)
    #exc_group = parser.add_argument_group('Exclusions')
    #excluded_slicing_arguments(exc_group,project_drs)
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    #comp_group = parser.add_argument_group('Complex Query')
    #comp_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.base_drs,
    #                                   help='List the field (or fields if repeated) found in the file' )
    #complex_slicing(comp_group,project_drs,action_type='append')

    data_node_group = parser.add_argument_group('Limit download from specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Retrieve only from the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not retrieve from the specified data nodes')

    return

def download_raw(subparsers,epilog,project_drs):
    epilog_download_raw=textwrap.dedent(epilog)
    parser=subparsers.add_parser('download_raw',
                                           description=textwrap.dedent('Take as an input the results from \'validate\' and downloads the data.'),
                                           epilog=epilog_download_raw,
                                         )
    input_arguments(parser)
    parser.add_argument('out_destination',
                             help='Destination directory for retrieval.')
    #proc_group = parser.add_argument_group('These arguments set threading options')
    #proc_group.add_argument('--num_procs',
    #                             default=1, type=int,
    #                             help='Use num_procs processes to perform the computation.')

    cert_group = parser.add_argument_group('Use these arguments to let cdb_query manage your ESGF credentials')
    cert_group.add_argument('--username',default=None,
                     help='If you set this value to your user name for registering service given in --service \n\
                           cdb_query will prompt you once for your password and will ensure that your credentials \n\
                           are active for the duration of the process.')
    cert_group.add_argument('--password_from_pipe',default=False,action='store_true',
                        help='If activated it is expected that the user is passing a password through piping\n\
                              Example: echo $PASS | cdb_query_'+project_drs.project+' ...')
    cert_group.add_argument('--service',default='badc',choices=['badc'],
                     help='Registering service. At the moment works only with badc.')


    serial_group = parser.add_argument_group('Specify asynchronous behavior')
    serial_group.add_argument('--serial',default=False,action='store_true',help='Downloads the files serially.')

    source_group = parser.add_argument_group('Specify sources')
    source_group.add_argument('--source_dir',default=None,help='local cache of data retrieved using \'download_raw\'')

    data_node_group = parser.add_argument_group('Limit download from specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Retrieve only from the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not retrieve from the specified data nodes')

    inc_group = parser.add_argument_group('Inclusions')
    inc_group.add_argument('--year',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated years.')
    inc_group.add_argument('--month',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated months (1 to 12). \n\
                                       If the list of months is composed only of continuous sublists (e.g. 1,2,12)\n\
                                       it ensures that continuous months are retrieved.')
    inc_group.add_argument('--day',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated calendar days.')
    inc_group.add_argument('--previous',
                                 default=False, action='store_true',
                                 help='Retrieve data from specified year, month, day AND the time step just BEFORE this retrieved data.')
    inc_group.add_argument('--next',
                                 default=False, action='store_true',
                                 help='Retrieve data from specified year, month, day AND the time step just AFTER this retrieved data.')
    #slicing_arguments(inc_group,project_drs)
    #exc_group = parser.add_argument_group('Exclusions')
    #excluded_slicing_arguments(exc_group,project_drs)
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    #comp_group = parser.add_argument_group('Complex Query')
    #comp_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.base_drs,
    #                                   help='List the field (or fields if repeated) found in the file' )
    #complex_slicing(comp_group,project_drs,action_type='append')
    return

def convert(subparsers,epilog,project_drs):
    epilog_convert=textwrap.dedent(epilog)
    parser=subparsers.add_parser('convert',
                                           description=textwrap.dedent('Take as an input the results from \'download\' and converts the data.'),
                                           epilog=epilog_convert,
                                         )
    input_arguments(parser)
    parser.add_argument('out_destination',
                             help='Destination directory for retrieval.')
    proc_group = parser.add_argument_group('These arguments set threading options')
    proc_group.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processes to perform the computation.')

    inc_group = parser.add_argument_group('Inclusions')
    slicing_arguments(inc_group,project_drs)
    exc_group = parser.add_argument_group('Exclusions')
    excluded_slicing_arguments(exc_group,project_drs)
    comp_group = parser.add_argument_group('Complex Query')
    comp_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.base_drs,
                                       help='List the field (or fields if repeated) found in the file' )
    complex_slicing(comp_group,project_drs,action_type='append')
    return


def apply(subparsers,epilog,project_drs):
    epilog_apply=textwrap.dedent(epilog)
    parser=subparsers.add_parser('apply',
                                           description=textwrap.dedent('Take as an input retrieved data and apply bash script'),
                                           epilog=epilog_apply
                                         )
    parser.add_argument('script',default='',help="Command-line script")
    parser.add_argument('in_diagnostic_netcdf_file',
                                 help='NETCDF retrieved files (input).')
    parser.add_argument('in_extra_netcdf_files',nargs='*',
                                 help='NETCDF extra retrieved files (input).')
    parser.add_argument('out_netcdf_file',
                                 help='NETCDF file (output)')

    parser.add_argument('--applying_to_soft_links',default=False,action='store_true',
                                 help='When applying an operator to soft links use this options for siginificant speed up.')

    select_group = parser.add_argument_group('These arguments specify the structure of the output')
    select_group.add_argument('--add_fixed',default=False, action='store_true',help='include fixed variables')
    select_group.add_argument('-k','--keep_field',action='append', type=str, choices=project_drs.official_drs_no_version,
                                       help='Keep these fields in the applied file.' )
    proc_group = parser.add_argument_group('These arguments set threading options')
    proc_group.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processes to perform the computation.')

    inc_group = parser.add_argument_group('Inclusions')
    #slicing_arguments(inc_group,project_drs)
    #exc_group = parser.add_argument_group('Exclusions')
    #excluded_slicing_arguments(exc_group,project_drs)
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    comp_group = parser.add_argument_group('Complex Query')
    comp_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.official_drs_no_version,
                                       help='Complex query fields.' )
    complex_slicing(comp_group,project_drs,action_type='append')
    return

def certificates(subparsers,epilog,project_drs):
    epilog_certificates=textwrap.dedent(epilog)
    parser=subparsers.add_parser('certificates',
                                           description=textwrap.dedent('Recovers ESGF certificates'),
                                           epilog=epilog_certificates
                                         )
    parser.add_argument('username',help="Username")
    parser.add_argument('--password_from_pipe',default=False,action='store_true',
                        help='If activated it is expected that the user is passing a password through piping\n\
                              Example: echo $PASS | cdb_query_'+project_drs.project+' ...')
    #parser.add_argument('password',help="Password")
    parser.add_argument('service',help="Registering service",choices=['badc'])
    return

def int_list(input):
    return [ int(item) for item in input.split(',')]
    
