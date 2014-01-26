import argparse 
import textwrap
import datetime

slicing_args={
              'center': [str,'Modelling center name'],
              'model': [str,'Model name'],
              'rip': [str,'RIP identifier, e.g. r1i1p1'],
              'experiment': [str,'Experiment name'],
              'var': [str,'Variable name, e.g. tas'],
              'frequency': [str,'Frequency, e.g. day'],
              'realm': [str,'Realm, e.g. atmos'],
              'mip': [str,'MIP table name, e.g. day'],
              'year': [int,'Year'],
              'month': [int,'Month as an integer ranging from 1 to 12'],
              'file_type': [str,'File type: OPEnDAP, local_file, HTTPServer, GridFTP']
              }

def input_arguments(parser):
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file (input)')
    return

def output_arguments(parser):
    parser.add_argument('out_diagnostic_headers_file',
                                 help='Diagnostic paths file (output)')
    parser.add_argument('-z','--gzip',
                                 default=False, action='store_true',
                                 help='Compress the output using gzip. Because the output is mostly repeated text, this leads to large compression.')
    return

#def discover_slicing_arguments(parser):
#    #Define the data slicing arguments in a dictionnary:
#    slicing_args={
#                  'center': [str,'Modelling center name'],
#                  'model': [str,'Model name'],
#                  'rip': [str,'RIP identifier, e.g. r1i1p1']
#                  }
#    for arg in slicing_args.keys():
#        parser.add_argument('--'+arg,type=slicing_args[arg][0],help=slicing_args[arg][1])
#    return

def slicing_arguments(parser,exclude_args=[]):
    #Define the data slicing arguments in a dictionnary:
    for arg in slicing_args.keys():
        if not arg in exclude_args:
            parser.add_argument('--'+arg,type=slicing_args[arg][0],help=slicing_args[arg][1])
    return

def base_drs():
    return ['center','model','experiment','frequency','realm','mip','rip','version','var','search','file_type','time','path']
    #return ['time','search','file_type','center','model','experiment','frequency','realm','mip','rip','version','var','path']

def generate_subparsers(parser,epilog):
    #Discover tree
    #discover_group=subparsers.add_argument_group('Discovering available data on the archive')
    discover_group = parser.add_subparsers(help='Commands to discover available data on the archive',dest='command')
    discover(discover_group,epilog)
    discover_centers(discover_group,epilog)
    list_paths(discover_group,epilog)
    list_domains(discover_group,epilog)
    slice(discover_group,epilog)
    simplify(discover_group,epilog)
    find_local(discover_group,epilog)
    simulations(discover_group,epilog)

    #Optimset tree
    optimset(discover_group,epilog)
    netcdf_paths(discover_group,epilog)
    remote_retrieve(discover_group,epilog)
    return

def discover(subparsers,epilog):
    #Find data
    epilog_discover=textwrap.dedent(epilog+'\n\nThe output can be pretty printed by using:\n\
                          cat out_diagnostic_headers_file | python -mjson.tool')
    parser=subparsers.add_parser('discover',
                                           help='Returns pointers to models that have as a subset the requested experiments and variables.\n\
                                                 It is good practice to check the results with \'simulations\' before\n\
                                                 proceeding with \'optimset\'.\n\
                                                 The output of \'optimset\' might depend on the order of the header field\n\
                                                 \'domain_list\' in the output file of \'discover\'',
                                           epilog=epilog_discover,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    input_arguments(parser)
    output_arguments(parser)
    #discover_slicing_arguments(parser)
    slicing_arguments(parser,exclude_args=['experiment','var','frequency','realm','mip','year','month','file_type'])
    parser.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processors to query the archive. NOT WORKING YET.')
    parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')
    parser.set_defaults(drs=base_drs())
    return parser


def list_domains(subparsers,epilog):
    #List_domains
    parser=subparsers.add_parser('list_domains',
                                           help='List domains (on file system or url) to files containing:\n\
                                                 ',
                                           )
    slicing_arguments(parser)
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')
    parser.set_defaults(drs=None)
    return

def list_paths(subparsers,epilog):
    #List_paths
    parser=subparsers.add_parser('list_paths',
                                           help='List paths (on file system or url) to files containing:\n\
                                                 ',
                                           )
    slicing_arguments(parser)
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')

    parser.add_argument('-w','--wget',
                                 default=False,
                                 action='store_true',
                                 help='Prints the paths with a structure that can be passed to a wget script, with MD5 checksum.')
    parser.set_defaults(drs=None)
    return

def slice(subparsers,epilog):
    #Slice data
    parser=subparsers.add_parser('slice',
                                           help='Slice the data according the passed keywords.',
                                           argument_default=argparse.SUPPRESS
                                           )
    parser.set_defaults(drs=None)
    input_arguments(parser)
    output_arguments(parser)
    slicing_arguments(parser)
    return

def simplify(subparsers,epilog):
    #Slice data
    parser=subparsers.add_parser('simplify',
                                           help='Simplify the data by removing not requested domain names.',
                                           argument_default=argparse.SUPPRESS
                                           )
    parser.set_defaults(drs=None)
    input_arguments(parser)
    output_arguments(parser)
    slicing_arguments(parser)
    return

def find_local(subparsers,epilog):
    #find_local
    parser=subparsers.add_parser('find_local',
                                           help='Find the local files that were downloaded'
                                           )
    parser.set_defaults(drs=None)
    input_arguments(parser)
    output_arguments(parser)
    slicing_arguments(parser)
    return

def discover_centers(subparsers,epilog):
    #discover_centers
    parser=subparsers.add_parser('discover_centers',
                                           help='Prints the centers available in the search paths.'
                                           )
    slicing_arguments(parser)
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')
    parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')
    parser.set_defaults(drs=base_drs())
    return

def simulations(subparsers,epilog):
    #Simulations
    parser=subparsers.add_parser('simulations',
                                           help='Prints the (center_model_rip) triples available in the pointers file.'
                                           )
    slicing_arguments(parser)
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')
    parser.set_defaults(drs=None)
    return

def optimset(subparsers,epilog):
    #Find Optimset Months
    epilog_optimset=textwrap.dedent(epilog)
    parser=subparsers.add_parser('optimset',
                                           help='Take as an input the results from \'discover\'.\n\
                                                 Returns pointers to models that have all the\n\
                                                 requested experiments and variables for all requested years.\n\
                                                 It can be slow, particularly if \'OPeNDAP\' files are\n\
                                                 requested.',
                                           epilog=epilog_optimset,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    input_arguments(parser)
    output_arguments(parser)
    slicing_arguments(parser)
    parser.set_defaults(drs=None)
    return

def netcdf_paths(subparsers,epilog):
    #Find Optimset Months
    epilog_netcdf=textwrap.dedent(epilog)
    parser=subparsers.add_parser('netcdf_paths',
                                           help='Take as an input the results from \'optimset\'.\n\
                                                 Returns pointers to models that have all the\n\
                                                 requested experiments and variables for all requested years',
                                           epilog=epilog_netcdf,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    input_arguments(parser)
    parser.add_argument('out_diagnostic_netcdf_file',
                                 help='Diagnostic paths file structured as a netcdf file (output)')
    slicing_arguments(parser)
    parser.set_defaults(drs=None)
    return

def remote_retrieve(subparsers,epilog):
    #Find Optimset Months
    epilog_optimset=textwrap.dedent(epilog)
    parser=subparsers.add_parser('remote_retrieve',
                                           help='Take as an input the results from \'netcdf_pathst\' and returns a\n\
                                                 netcdf file.',
                                           epilog=epilog_optimset,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    slicing_list=['center','model','experiment','frequency','realm','mip','rip','var']
    for arg in slicing_list:
        parser.add_argument(arg,type=slicing_args[arg][0],help=slicing_args[arg][1])

    parser.add_argument('--cdo',default=False,action='store_true',help='Output cdo command for retrieval')
    parser.add_argument('--nco',default=False,action='store_true',help='Output nco command for retrieval')
    parser.add_argument('--list_domains',default=False,action='store_true',help='List all the domains that house the remote source')
    parser.add_argument('--domain',type=str,help='The requested domain')

    parser.add_argument('timestamp', type=timestamps,
                                 help='Comma-separated lis of time stamps in ISO format YYYYmmDDTHH:MM:SS')
    parser.add_argument('in_diagnostic_netcdf_file',
                                 help='Diagnostic paths file structured as a netcdf file (input)')
    #parser.add_argument('out_netcdf_file',
    #                             help='Retrieved data as a netcdf file (output)')
    parser.set_defaults(drs=slicing_list)
    return

def timestamps(ts):
    try:
        timestamp_list=map(lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S" ), ts.split(','))
        return timestamp_list
    except:
        raise argparse.ArgumentTypeError("Timestamps must be ISO YYYY-mm-DDTHH:MM:SS")
