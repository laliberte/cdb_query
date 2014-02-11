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


def generate_subparsers(parser,epilog,project_drs):
    #Discover tree
    subparsers = parser.add_subparsers(help='Commands to discover available data on the archive',dest='command')
    discover(subparsers,epilog,project_drs)
    list_fields(subparsers,epilog,project_drs)
    #slice(subparsers,epilog,project_drs)
    #simplify(subparsers,epilog,project_drs)
    #simulations(subparsers,epilog,project_drs)

    #Optimset tree
    optimset(subparsers,epilog,project_drs)
    remote_retrieve(subparsers,epilog)
    return

def discover(subparsers,epilog,project_drs):
    #Find data
    epilog_discover=epilog
    parser=subparsers.add_parser('discover',
                                           description=textwrap.dedent(
                                                '''Returns pointers to models that have as a subset the requested experiments and variables.\n\
                                                 It is good practice to check the results with \'simulations\' before
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
    #parser.add_argument('--num_procs',
    #                             default=1, type=int,
    #                             help='Use num_procs processors to query the archive. NOT WORKING YET.')
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

def slice(subparsers,epilog,project_drs):
    #Slice data
    parser=subparsers.add_parser('slice',
                                   description=textwrap.dedent('Slice the data according the passed keywords.'),
                                   argument_default=argparse.SUPPRESS
                                   )
    #input_arguments_json(parser)
    input_arguments(parser)
    output_arguments(parser)
    slicing_arguments(parser,project_drs)
    return

def simplify(subparsers,epilog,project_drs):
    #Slice data
    parser=subparsers.add_parser('simplify',
                                   description=textwrap.dedent('Simplify the data by removing data_node names that are not requested'),
                                   argument_default=argparse.SUPPRESS
                                   )
    input_arguments(parser)
    output_arguments(parser)
    slicing_arguments(parser,project_drs)
    return

#def find_local(subparsers,epilog):
#    #find_local
#    parser=subparsers.add_parser('find_local',
#                                           help='Find the local files that were downloaded'
#                                           )
#    input_arguments(parser)
#    output_arguments(parser)
#    slicing_arguments(parser)
#    return

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
    input_arguments(parser)
    output_arguments(parser)
    slicing_arguments(parser,project_drs)
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

def remote_retrieve(subparsers,epilog):
    epilog_optimset=textwrap.dedent(epilog)
    parser=subparsers.add_parser('remote_retrieve',
                                           description=textwrap.dedent('Take as an input the results from \'optimset\' and returns a\n\
                                                 netcdf file with the data retrieved.'),
                                           epilog=epilog_optimset,
                                         )
    input_arguments(parser)
    parser.add_argument('out_destination',
                             help='Destination directory for retrieval.')
    parser.add_argument('--netcdf',default=False,action='store_true',
                             help='If activated, the data is retrieved to a hierachical netCDF4 file.\n\
                                   in this case, out_destination should point to a file.')
    return

#def remote_retrieve(subparsers,epilog):
#    #Find Optimset Months
#    epilog_optimset=textwrap.dedent(epilog)
#    parser=subparsers.add_parser('remote_retrieve',
#                                           description=textwrap.dedent('Take as an input the results from \'netcdf_pathst\' and returns a\n\
#                                                 netcdf file.'),
#                                           epilog=epilog_optimset,
#                                         )
#    slicing_list=['institute','model','experiment','time_frequency','realm','mip','rip','var']
#    for arg in slicing_list:
#        parser.add_argument(arg,type=project_drs.slicing_args[arg][0],help=project_drs.slicing_args[arg][1])
#
#    parser.add_argument('--cdo',default=False,action='store_true',help='Output cdo command for retrieval')
#    parser.add_argument('--nco',default=False,action='store_true',help='Output nco command for retrieval')
#    parser.add_argument('--list_data_nodes',default=False,action='store_true',help='List all the data_nodes that house the remote source')
#    parser.add_argument('--data_node',type=str,help='The requested data_node')
#
#    parser.add_argument('timestamp', type=timestamps,
#                                 help='Comma-separated lis of time stamps in ISO format YYYYmmDDTHH:MM:SS')
#    parser.add_argument('in_diagnostic_netcdf_file',
#                                 help='Diagnostic paths file structured as a netcdf file (input)')
#    #parser.add_argument('out_netcdf_file',
#    #                             help='Retrieved data as a netcdf file (output)')
#    return
#
#def timestamps(ts):
#    try:
#        timestamp_list=map(lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S" ), ts.split(','))
#        return timestamp_list
#    except:
#        raise argparse.ArgumentTypeError("Timestamps must be ISO YYYY-mm-DDTHH:MM:SS")
