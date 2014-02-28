import argparse 
import textwrap
import datetime
import copy

def input_arguments_json(parser):
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file (input)')
    return
def output_arguments_json(parser):
    parser.add_argument('out_diagnostic_headers_file',
                                 help='Diagnostic headers file (output)')
    return

def input_arguments(parser):
    parser.add_argument('in_diagnostic_netcdf_file',
                                 help='NETCDF Diagnostic paths file (input)')
    return

def output_arguments(parser):
    parser.add_argument('out_diagnostic_netcdf_file',
                                 help='NETCDF Diagnostic paths file (output)')
    return

def slicing_arguments(parser,project_drs,exclude_args=[]):
    #Define the data slicing arguments in a dictionnary:
    for arg in project_drs.slicing_args.keys():
        if not arg in exclude_args:
            parser.add_argument('--'+arg,
                                type=project_drs.slicing_args[arg][0],
                                help=project_drs.slicing_args[arg][1]
                                )
    return
def excluded_slicing_arguments(parser,project_drs,exclude_args=[]):
    #Define the data slicing arguments in a dictionnary:
    for arg in project_drs.slicing_args.keys():
        if not arg in exclude_args:
            parser.add_argument('--'+'X'+arg,
                                type=project_drs.slicing_args[arg][0],
                                help='Exclude '+project_drs.slicing_args[arg][1]
                                )
    return


def generate_subparsers(parser,epilog,project_drs):
    #Discover tree
    subparsers = parser.add_subparsers(help='Commands to discover available data on the archive',dest='command')
    discover(subparsers,epilog,project_drs)
    list_fields(subparsers,epilog,project_drs)

    #Optimset tree
    optimset(subparsers,epilog,project_drs)
    remote_retrieve(subparsers,epilog,project_drs)
    download(subparsers,epilog,project_drs)
    apply(subparsers,epilog,project_drs)
    convert(subparsers,epilog,project_drs)
    return

def discover(subparsers,epilog,project_drs):
    #Find data
    epilog_discover=epilog
    parser=subparsers.add_parser('discover',
                                           description=textwrap.dedent(
                                                '''Returns pointers to models that have as a subset the requested experiments and variables.\n\
                                                 It is good practice to check the results with \'list_fields\' before
                                                 proceeding with \'optimset\'.
                                                 The output of \'optimset\' might depend on the order of the header attribute
                                                 \'data_node_list\' in the output file of \'discover\'. It is good practice to
                                                 reorder this attribute before proceeding with \'optimset\'.
                                                 
                                                 Unlike \'optimset\' this function should NOT require appropriate certificates
                                                 to function properly. If it fails it is possible the servers are down.'''),
                                           epilog=epilog_discover
                                         )
    input_arguments_json(parser)
    output_arguments(parser)
    slicing_arguments(parser,project_drs,exclude_args=project_drs.discover_exclude_args)
    parser.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processors to query the archive.')
    parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')
    parser.add_argument('--list_only_field',default=None, choices=project_drs.remote_fields,
                                  help='When this option is used, the discovery function prints only the specified field \n\
                                        for which published data COULD match the query. Does nothing to the output file.\n\
                                        Listing separate fields is usually much quicker than the discovery step.')
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
    slicing_arguments(parser,project_drs)
    input_arguments(parser)
    parser.add_argument('-f','--field',action='append', type=str, choices=project_drs.base_drs,
                                       help='List the field (or fields if repeated) found in the file' )
    return

def optimset(subparsers,epilog,project_drs):
    #Find Optimset Months
    epilog_optimset=textwrap.dedent(epilog)
    parser=subparsers.add_parser('optimset',
                                   description=textwrap.dedent('Take as an input the results from \'discover\'.\n\
                                         Returns pointers to models that have ALL the\n\
                                         requested experiments and variables for ALL requested years.\n\
                                         \n\
                                         Can be SLOW.\n\
                                         \n\
                                         Note that if this function fails it is likely that approriate\n\
                                         certificates have not been installed on this machine.'),
                                   epilog=epilog_optimset,
                                 )
    slicing_arguments(parser,project_drs)
    parser.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processors to query the archive.')
    input_arguments(parser)
    output_arguments(parser)
    #parser.add_argument('--data_nodes',default=None,type=(lambda x: x.split(',')),
    #                             help='Ordered list of data nodes to be used.')
    return

#def netcdf_paths(subparsers,epilog):
#    #Find Optimset Months
#    epilog_netcdf=textwrap.dedent(epilog)
#    parser=subparsers.add_parser('netcdf_paths',
#                                           description=textwrap.dedent('Take as an input the results from \'optimset\'.\n\
#                                                 Returns pointers to models that have all the\n\
#                                                 requested experiments and variables for all requested years',
#                                           epilog=epilog_netcdf,
#                                         )
#    input_arguments(parser)
#    output_arguments(parser)
#    #parser.add_argument('out_diagnostic_netcdf_file',
#    #                             help='Diagnostic paths file structured as a netcdf file (output)')
#    slicing_arguments(parser)
#    return

def remote_retrieve(subparsers,epilog,project_drs):
    epilog_optimset=textwrap.dedent(epilog)
    parser=subparsers.add_parser('remote_retrieve',
                                           description=textwrap.dedent('Take as an input the results from \'optimset\' and returns a\n\
                                                 netcdf file with the data retrieved.'),
                                           epilog=epilog_optimset,
                                         )
    input_arguments(parser)
    #parser.add_argument('out_destination',
    #                         help='Destination directory for retrieval.')
    output_arguments(parser)
    #parser.add_argument('--num_procs',
    #                             default=1, type=int,
    #                             help='Use num_procs processors to set up the retrieval.')
    parser.add_argument('--source_dir',default=None,help='local cache of data retrieved using \'download\'')
    parser.add_argument('--year',
                                 default=None, type=int,
                                 help='Retrieve only this year.')
    parser.add_argument('--month',
                                 default=None, type=int,
                                 help='Retrieve only this month (1 to 12).')
    slicing_arguments(parser,project_drs)
    excluded_slicing_arguments(parser,project_drs)
    return

def download(subparsers,epilog,project_drs):
    epilog_download=textwrap.dedent(epilog)
    parser=subparsers.add_parser('download',
                                           description=textwrap.dedent('Take as an input the results from \'optimset\' and downloads the data.'),
                                           epilog=epilog_download,
                                         )
    input_arguments(parser)
    parser.add_argument('out_destination',
                             help='Destination directory for retrieval.')
    slicing_arguments(parser,project_drs)
    excluded_slicing_arguments(parser,project_drs)
    return

def convert(subparsers,epilog,project_drs):
    epilog_convert=textwrap.dedent(epilog)
    parser=subparsers.add_parser('convert',
                                           description=textwrap.dedent('Take as an input the results from \'remote_retrieve\' and converts the data.'),
                                           epilog=epilog_convert,
                                         )
    input_arguments(parser)
    parser.add_argument('out_destination',
                             help='Destination directory for retrieval.')
    slicing_arguments(parser,project_drs)
    excluded_slicing_arguments(parser,project_drs)
    parser.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processors to perform the computation.')
    return


def apply(subparsers,epilog,project_drs):
    epilog_apply=textwrap.dedent(epilog)
    parser=subparsers.add_parser('apply',
                                           description=textwrap.dedent('Take as an input retrieved data and apply bash script'),
                                           epilog=epilog_apply
                                         )
    parser.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processors to perform the computation.')
    slicing_arguments(parser,project_drs)
    excluded_slicing_arguments(parser,project_drs)
    parser.add_argument('-s','--script',default='',help="Command-line script")
    parser.add_argument('in_diagnostic_netcdf_file',
                                 help='NETCDF retrieved files (input).')
    parser.add_argument('in_extra_netcdf_files',nargs='*',
                                 help='NETCDF extra retrieved files (input).')
    parser.add_argument('out_netcdf_file',
                                 help='NETCDF file (output)')
    return
    
