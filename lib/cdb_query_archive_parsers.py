#External:
import argparse 
import textwrap
import datetime
import copy

#External but related:
import netcdf4_soft_links.manage_soft_links_parsers as manage_soft_links_parsers

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

def processing_arguments(parser,project_drs):
    proc_group = parser.add_argument_group('These arguments set threading options')
    proc_group.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processes to perform the computation.')
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
    validate(subparsers,epilog,project_drs)

    apply(subparsers,epilog,project_drs)
    convert(subparsers,epilog,project_drs)

    download(subparsers,epilog,project_drs)
    download_raw(subparsers,epilog,project_drs)
    certificates(subparsers,epilog,project_drs)
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

    manage_soft_links_parsers.certificates_arguments(parser,project_drs)
    processing_arguments(parser,project_drs)
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

    input_arguments(parser)
    output_arguments(parser)
    parser.add_argument('--in_diagnostic_headers_file',
                                 help='Alternative diagnostic headers file (to modify target validate)',\
                                 type=str,default=None)

    manage_soft_links_parsers.certificates_arguments(parser,project_drs)
    processing_arguments(parser,project_drs)

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

#nc_Database PARSERS
def convert(subparsers,epilog,project_drs):
    epilog_convert=textwrap.dedent(epilog)
    parser=subparsers.add_parser('convert',
                                           description=textwrap.dedent('Take as an input the results from \'download\' and converts the data.'),
                                           epilog=epilog_convert,
                                         )
    input_arguments(parser)
    parser.add_argument('out_destination',
                             help='Destination directory for retrieval.')
    processing_arguments(parser,project_drs)

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
    processing_arguments(parser,project_drs)

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

#SOFT-LINKS PARSERS
def certificates(subparsers,epilog,project_drs):
    manage_soft_links.certificates(subparsers,epilog,project_drs)
    return

def download(subparsers,epilog,project_drs):
    parser=manage_soft_links_parsers.download(subparsers,epilod,project_drs)

    inc_group = parser.add_argument_group('Inclusions')
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    return

def download_raw(subparsers,epilog,project_drs):
    parser=manage_soft_links_parsers.download_raw(subparsers,epilod,project_drs)

    inc_group = parser.add_argument_group('Inclusions')
    slicing_arguments(inc_group,project_drs,action_type='append')
    exc_group = parser.add_argument_group('Exclusions')
    excluded_slicing_arguments(exc_group,project_drs,action_type='append')
    return

def int_list(input):
    return [ int(item) for item in input.split(',')]
    
