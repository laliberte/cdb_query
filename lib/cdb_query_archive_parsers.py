import argparse 
import textwrap

def input_arguments(parser):
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file (input)')
    parser.add_argument('out_diagnostic_headers_file',
                                 help='Diagnostic paths file (output)')
    parser.add_argument('-z','--gzip',
                                 default=False, action='store_true',
                                 help='Compress the output using gzip. Because the output is mostly repeated text, this leads to large compression.')
    return

def discover_slicing_arguments(parser):
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
    discover_slicing_arguments(parser)
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

def base_drs():
    return ['center','model','experiment','frequency','realm','mip','rip','version','var','search','file_type','time','path']
    #return ['time','search','file_type','center','model','experiment','frequency','realm','mip','rip','version','var','path']

def generate_subparsers(subparsers,epilog):
    discover(subparsers,epilog)
    optimset(subparsers,epilog)
    #optimset_time(subparsers,epilog)
    list_paths(subparsers,epilog)
    slice(subparsers,epilog)
    find_local(subparsers,epilog)
    netcdf_paths(subparsers,epilog)
    simulations(subparsers,epilog)
    return

def discover(subparsers,epilog):
    #Find data
    epilog_discover=textwrap.dedent(epilog+'\n\nThe output can be pretty printed by using:\n\
                          cat out_diagnostic_headers_file | python -mjson.tool')
    parser=subparsers.add_parser('discover',
                                           help='Returns pointers to models that have as a subset the requested experiments and variables.\n\
                                                 It is good practice to check the results with \'simulations\' before\n\
                                                 proceeding with \'optimset\'.',
                                           epilog=epilog_discover,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    input_arguments(parser)
    discover_slicing_arguments(parser)
    parser.add_argument('--num_procs',
                                 default=1, type=int,
                                 help='Use num_procs processors to query the archive. NOT WORKING YET.')
    parser.add_argument('--distrib',
                                 default=False, action='store_true',
                                 help='Distribute the search. Will likely result in a pointers originating from one node.')
    parser.set_defaults(drs=base_drs())
    return
def optimset(subparsers,epilog):
    #Find Optimset Months
    epilog_optimset=textwrap.dedent(epilog+'\n\nThe output can be pretty printed by using:\n\
                          cat out_diagnostic_headers_file | python -mjson.tool')
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
    slicing_arguments(parser)
    parser.set_defaults(drs=None)
    return

def optimset_time(subparsers,epilog):
    #Find Optimset Months
    epilog_optimset_time=textwrap.dedent(epilog+'\n\nThe output can be pretty printed by using:\n\
                          cat out_diagnostic_headers_file | python -mjson.tool')
    parser=subparsers.add_parser('optimset_time',
                                           help='Take as an input the results from \'optimset\'.\n\
                                                 Returns pointers to models that have all the\n\
                                                 requested experiments and variables for all requested years.\n\
                                                 It can be slow, particularly if \'OPeNDAP\' files are\n\
                                                 requested.',
                                           epilog=epilog_optimset_time,
                                           formatter_class=argparse.RawTextHelpFormatter
                                         )
    input_arguments(parser)
    slicing_arguments(parser)
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
    slicing_arguments(parser)
    return

def find_local(subparsers,epilog):
    #find_local
    parser=subparsers.add_parser('find_local',
                                           help='Find the local files that were downloaded'
                                           )
    parser.set_defaults(drs=None)
    input_arguments(parser)
    slicing_arguments(parser)
    return

def netcdf_paths(subparsers,epilog):
    #netcdf_paths
    parser=subparsers.add_parser('netcdf_paths',
                                           help='Organize paths in netcdf files. Files should be local or OPeNDAP.'
                                           )
    parser.set_defaults(drs=None)
    parser.add_argument('--checksum',
                                 default=False,
                                 action='store_true',
                                 help='Record the MD5 checksum for each time slice.')
    parser.add_argument('in_diagnostic_headers_file',
                                 help='Diagnostic headers file with data pointers (input)')
    slicing_arguments(parser)
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
