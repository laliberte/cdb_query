#External:
import argparse 
import textwrap
import datetime
import copy
import os
import numpy as np
import netCDF4
from pkg_resources import parse_version
import importlib
import shutil

#External but related:
import netcdf4_soft_links.parsers as nc4sl_parsers

#Internal:
import remote_archive

file_type_list=['local_file','OPENDAP','HTTPServer']

def full_parser(args_list):
    """
    Takes an argument list that can be easily generated by converting a command line string \'cmd\' using \'[arg for arg in cmd.split(' ') if arg]\'
    """

    version_num='1.9.9.6'
    #Option parser
    description=textwrap.dedent('''\
    This script queries an ESGF project. It can query:
    1. a local archive that follows the project DRS
    filename convention and directory structure.
    2. the ESGF project archive.

    ''')
    epilog='Version {0}: Frederic Laliberte (09/2016),\n\
Previous versions: Frederic Laliberte, Paul Kushner (2011-2016).\n\
\n\
If using this code to retrieve and process data from the ESGF please cite:\n\n\
Efficient, robust and timely analysis of Earth System Models: a database-query approach (2017):\n\
F. Laliberte, Juckes, M., Denvil, S., Kushner, P. J., TBD'.format(version_num)

    if parse_version(netCDF4.__netcdf4libversion__) >= parse_version('4.4'):
        raise ImportError('At the moment, cdb_query is only compatible with netcdf versions less than 4.4')

    prog=args_list[0].split('/')[-1]
    #Be careful with the -h option:
    if len(args_list)>2:
        help_flag=False
    else:
        help_flag=True

    #Start with some Monkey patching:
    argparse.ArgumentParser.set_default_subparser=set_default_subparser

    #First setup the project commands:
    project_parser = argparse.ArgumentParser(
                            formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            version=prog+version_num,
                            add_help=help_flag,
                            epilog=epilog)

    subparsers=project_parser.add_subparsers(help='Project selection',dest='project')
    for sub_project in remote_archive.available_projects:
        subparser=subparsers.add_parser(sub_project,help='Utilities for project '+sub_project,add_help=help_flag)
        if not help_flag:
            #Carry the option around silently:
            subparser.add_argument('-h',action='store_true',help=argparse.SUPPRESS)

    options, commands_args = project_parser.parse_known_args(args=args_list[1:])
    #This is an ad-hoc patch to allow chained subcommands:
    cli=['certificates','list_fields','merge','ask','validate','download_files',
         'reduce_soft_links','download_opendap','reduce', 'reduce_server','record']
    try:
        id_cli_used, cli_used = zip(*[arg for arg in enumerate(commands_args) if arg[1] in cli])

        if ( len(cli_used) < 2 or 
             cli_used[-1] != 'record' or
             cli_used[-2] != 'record'):
            commands_args.insert(np.max(id_cli_used)+1,'record')
            if ( len(cli_used) == 0 or
                 cli_used[-1] != 'record' ):
                commands_args.insert(np.max(id_cli_used)+2,'record')
    except ValueError:
        pass

    if 'h' in dir(options) and options.h:
        #Restore the option:
        commands_args.append('-h')

    #Then parser project-specific arguments:
    project_drs = importlib.import_module('.remote_archive.'+options.project, package=prog).DRS
    command_parser = argparse.ArgumentParser(
                            prog=prog+' '+options.project,
                            formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            version=prog+' '+version_num,
                            epilog=epilog)

    #Generate subparsers
    generate_subparsers(command_parser,epilog,project_drs)

    options = command_parser.parse_args(commands_args,namespace=options)

    #These lines are necessary for the failsafe implementation:
    for field in ['in_netcdf_file', 'out_netcdf_file', 'trial']:
        if ( set([field,'original_'+field]).issubset(dir(options))
             and getattr(options, 'original_'+field) is None ):
            setattr(options, 'original_'+field, getattr(options, field))
            
    return options, project_drs

#http://stackoverflow.com/questions/6365601/default-sub-command-or-handling-no-sub-command-with-argparse
def set_default_subparser(self, name, args=None):
    """default subparser selection. Call after setup, just before parse_args()
    name: is the name of the subparser to call by default
    args: if set is the argument list handed to parse_args()

    , tested with 2.7, 3.2, 3.3, 3.4
    it works with 2.6 assuming argparse is installed
    """
    subparser_found = False
    for arg in sys.argv[1:]:
        if arg in ['-h', '--help']:  # global help if no subparser
            break
    else:
        for x in self._subparsers._actions:
            if not isinstance(x, argparse._SubParsersAction):
                continue
            for sp_name in x._name_parser_map.keys():
                if sp_name in sys.argv[1:]:
                    subparser_found = True
        if not subparser_found:
            # insert default in first position, this implies no
            # global options without a sub_parsers specified
            if args is None:
                sys.argv.insert(1, name)
            else:
                args.insert(0, name)

def _absolute_path(path):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(path)))

def _isfile(options,field):
    return os.path.isfile(getattr(options,field))

def _copyfile(options_source, field_source, options_dest, field_dest):
    shutil.copyfile(getattr(options_source,field_source),
                     getattr(options_dest,field_dest))
    return

def _remove(options,field):
    os.remove(getattr(options,field))
    return


def input_arguments(parser,project_drs):
    parser.add_argument('in_netcdf_file',type=_absolute_path,
                                 help='NETCDF Diagnostic paths file (input)')
    #Options only for failsafe implementation:
    parser.add_argument('--original_in_netcdf_file', default=None, help=argparse.SUPPRESS)
    return

def output_arguments(parser,project_drs):
    parser.add_argument('out_netcdf_file',type=_absolute_path,
                                 help='NETCDF Diagnostic paths file (output)')
    #Options only for failsafe implementation:
    parser.add_argument('--original_out_netcdf_file', default=None, help=argparse.SUPPRESS)

    #group = parser.add_mutually_exclusive_group()
    parser.add_argument('-O',action='store_true',
                            help='Overwrite output file. Default: False')
    #group.add_argument('-S',action='store_false',
    #                        help='Skip if output file exists. Default: True')
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
    parser.add_argument('-s', '--silent', default=False, action='store_true', help='Make not verbose.')
    parser.add_argument('--trial', type=int, default=2, help='Try a function that number of time '
                                                             'before re-attempting the whole branch. Default: 3.')
    #Options only for failsafe implementation:
    parser.add_argument('--original_trial', default=None, help=argparse.SUPPRESS)

    parser.add_argument('--failsafe_attempt', type=int, default=1, help='Try a function sequence function that number of time '
                                                                        'before dropping the offending simulation. Default: 2.')
    parser.add_argument('--debug', default=False, action='store_true', help='Disable --trial and --failsafe_attempt '
                                                                            'and raise exceptions. Necessary to obtain '
                                                                            'debugging informations.')

    parser.add_argument('--log_files', default=False, action='store_true', help='Create one log file per process.')
    parser.add_argument('--swap_dir', type=writeable_dir, default='.',
                                      help='Use this directory as a swap directory.')
    parser.add_argument('--priority', type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument('--command_number', type=int, default=0, help=argparse.SUPPRESS)
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

#def record_validate(parser,project_drs):
#    parser.add_argument('--record_validate',default=False,action='store_true',help='Record validate results in out_netcdf_file.validate')
#    return

def ask_shared_arguments(parser,project_drs):
    query_group = parser.add_argument_group('Scientific query setup')
    default_experiment={'CMIP5':tuple(['historical:1950-2005',]),
                        'CORDEX':tuple(['historical:1979-2005',]),
                        'CREATEIP':tuple(['CFSR:1979-2015','ERA-Interim:1979-2015',
                                          'JRA-25:1979-2013',
                                          'JRA-55:1958-2015',
                                          'MERRA-2:1980-2015',
                                          'MERRA-reanalysis:1979-2015']),
                        'CanSISE':tuple(['historical-r1:1979-2005',])}
    query_group.add_argument('--ask_experiment',
                             default=list(default_experiment[project_drs.project]),
                             type=csv_list,
                             help='Comma-separated list of \'experiment:start_year-end_year\' triples.\n\
                                   Note that specifiying 1<=start_year<10 means that\n\
                                   the years are relative to the first year in the simulation.\n\
                                   For example, \'piControl:1-101\' will find the first hundred\n\
                                   years of the piControl experiment. Can be repeated for multiple experiments.\n\
                                   Default {0}'.format(' '.join(default_experiment[project_drs.project])))
    query_group.add_argument('--ask_month',
                             default=range(1,13),
                             type=int_list,
                             help='Comma-separated list of months to be considered. Default: All months.')
    ESGF_nodes=[
    'https://esgf-index1.ceda.ac.uk/esg-search/',
    'https://esgf-node.ipsl.upmc.fr/esg-search/',
    'https://esgf-data.dkrz.de/esg-search/',
    'https://pcmdi.llnl.gov/esg-search/',
    'https://esgf-node.jpl.nasa.gov/esg-search/',
    'https://esg-dn1.nsc.liu.se/esg-search/'
    ]
    default_search_path_list={'CMIP5':ESGF_nodes,
                              'CORDEX':ESGF_nodes,
                              'CREATEIP':['https://esgf.nccs.nasa.gov/esg-search/'],
                              'CanSISE':['http://collaboration.cmc.ec.gc.ca/cmc/cccma/CanSISE/output']}
    query_group.add_argument('--search_path',
                             default=default_search_path_list[project_drs.project],
                             type=csv_list,
                             help='Comma-separated list of search paths. Can be a local directory, an ESGF index node, a FTP server.\n\
                                   Default: {0}'.format(','.join(default_search_path_list[project_drs.project])))
    query_group.add_argument('--Xsearch_path',
                             default=[],
                             help='Comma-separated list of search paths to exclude.')
    default_var={'CMIP5':tuple(['tas:mon-atmos-Amon',]),
                 'CORDEX':tuple(['tas:mon',]),
                 'CREATEIP':tuple(['tas:mon-atmos',]),
                 'CanSISE':tuple(['tas:mon-atmos',])}
    query_group.add_argument('--ask_var',
                             default=list(default_var[project_drs.project]),
                             type=csv_list,
                             help='Comma-separated list of \'variable:{1}\' tuples.\n\
                                   Default: {0}'.format(','.join(default_var[project_drs.project]),'-'.join(project_drs.var_specs)))
    query_group.add_argument('--ask_file_type',
                             default=[],
                             choices=project_drs.file_types,
                             action='append',
                             help='A file type. Can be repeated.\n\
                                   This list is ordered. The first specified file type will be selected first in the validate step.\n\
                                   Default: {0}'.format(','.join(project_drs.file_types)))

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

def reduce_soft_links_script_process_arguments(parser,project_drs):
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
    parser.add_argument('--sample',action='store_true',help='Save samples in out_destination')
    parser.add_argument('in_extra_netcdf_files',nargs='*',
                                 help='NETCDF extra retrieved files (input).')
    return 

def reduce_script_shared_arguments(parser,project_drs):
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
    parser.add_argument('--restrictive_loop',action='store_true',default=False,
                        help=('When activated, -l defines loops in a very restrictive manner.'
                              'This can be useful when using --missing_years'))
    return parser

def add_dummy_record_parser(parser,description,epilog,number=1):
    subparsers = parser.add_subparsers(help='',dest='command_{0}'.format(number))
    new_parser=subparsers.add_parser('record',description=description,
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
    parser=add_dummy_record_parser(parser,description,epilog)
    parser=add_dummy_record_parser(parser,description,epilog,number=2)
    nc4sl_parsers.certificates_arguments(parser,project_drs)
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
    parser=add_dummy_record_parser(parser,description,epilog)
    parser=add_dummy_record_parser(parser,description,epilog,number=2)
    input_arguments(parser,project_drs)
    select_group = parser.add_argument_group('These arguments specify the structure of the output')
    select_group.add_argument('-f','--field',action='append', type=str, choices=project_drs.base_drs,
                                       help='List the field (or fields if repeated) found in the file' )

    nc4sl_parsers.data_node_restriction(parser,project_drs)
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
    parser=add_dummy_record_parser(parser,description,epilog)
    parser=add_dummy_record_parser(parser,description,epilog,number=2)
    input_arguments(parser,project_drs)
    parser.add_argument('in_extra_netcdf_files',nargs='*',
                                 help='NETCDF extra files (input).')
    output_arguments(parser,project_drs)
    basic_slicing(parser,project_drs)
    complex_slicing_with_fields(parser,project_drs)
    return

#Functions

#def reduce(subparsers,epilog,project_drs):
#    epilog_reduce=textwrap.dedent(epilog)
#    parser=subparsers.add_parser('reduce',
#                                       description=textwrap.dedent('Take as an input retrieved data and reduce bash script'),
#                                       epilog=epilog_reduce
#                                         )
#    functions_arguments(parser,['reduce'])
#    return 

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
                       nc4sl_parsers.certificates_arguments,
                       nc4sl_parsers.serial_arguments,
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
                                   nc4sl_parsers.certificates_arguments,
                                   nc4sl_parsers.serial_arguments,
                                   basic_slicing,
                                   complex_slicing_with_fields,
                                   nc4sl_parsers.data_node_restriction,
                                   nc4sl_parsers.time_selection_arguments,
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
                            nc4sl_parsers.certificates_arguments,
                            nc4sl_parsers.serial_arguments,
                            basic_slicing,
                            complex_slicing_with_fields,
                            fields_selection,
                            nc4sl_parsers.data_node_restriction,
                            nc4sl_parsers.time_selection_arguments,
                            nc4sl_parsers.download_files_arguments_no_io,
                            nc4sl_parsers.download_arguments_no_io,
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
                                            nc4sl_parsers.serial_arguments,
                                            nc4sl_parsers.data_node_restriction,
                                            nc4sl_parsers.time_selection_arguments,
                                            ]
    start_arguments_handles['reduce_soft_links']=[input_arguments]
    process_arguments_handles['reduce_soft_links']=[reduce_soft_links_script_process_arguments,output_arguments]
    chained_arguments_handles['reduce_soft_links']=[reduce_soft_links_chained_arguments]

    description['download_opendap']=textwrap.dedent('''Take as an input the results from \'validate\',
                                \'download_files\' or \'reduce_soft_links\'\n\
                                and returns a soft links file with the opendap data filling the database.\n\
                                Must be called after \'download_files\' in order to prevent missing data.''')
    arguments_handles['download_opendap']=[
                            basic_control_arguments,
                            nc4sl_parsers.certificates_arguments,
                            basic_slicing,
                            complex_slicing_with_fields,
                            nc4sl_parsers.time_selection_arguments,
                            nc4sl_parsers.data_node_restriction,
                            nc4sl_parsers.serial_arguments,
                            fields_selection,
                            nc4sl_parsers.download_opendap_arguments_no_io,
                            nc4sl_parsers.download_arguments_no_io,
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
                                 nc4sl_parsers.time_selection_arguments,
                                 nc4sl_parsers.data_node_restriction,
                                 nc4sl_parsers.serial_arguments,
                                 loop_control,
                                 reduce_script_shared_arguments,
                                 ]
    start_arguments_handles['reduce']=[input_arguments]
    process_arguments_handles['reduce']=[reduce_process_arguments,output_arguments]
    chained_arguments_handles['reduce']=[]

    description['record']=textwrap.dedent('Record results from previous function')
    arguments_handles['record']=[
                                 ]
    start_arguments_handles['record']=[]
    process_arguments_handles['record']=[]
    chained_arguments_handles['record']=[]

    description['reduce_server']=textwrap.dedent('Spawns reducer processes')
    arguments_handles['reduce_server']=[
                                 basic_control_arguments,
                                 processing_arguments,
                                 ]
    start_arguments_handles['reduce_server']=[]
    process_arguments_handles['reduce_server']=[]
    chained_arguments_handles['reduce_server']=[]

    childs={'ask':['validate'],
            'validate':['download_files','reduce_soft_links','download_opendap','reduce'],
            'download_files':['reduce_soft_links','download_opendap','reduce'],
            'reduce_soft_links':['download_opendap','reduce'],
            'download_opendap':['reduce'],
            'reduce':[],
            'record':[],
            'reduce_server':[]
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
                                                  previous_arguments_handles,
                                                  record=False):
    function_name=functions_list[-1]

    subparsers = parser.add_subparsers(help='Commands to discover available data on the archive',
                                       dest='command_{0}'.format(len(functions_list)))
    if len(functions_list)==1:
        current_arguments_handles=(
                                  start_arguments_handles[function_name]+
                                  arguments_handles[function_name])
                                                    
    else:
        current_arguments_handles=arguments_handles[function_name]

    for sub_function_name in childs[function_name]:
        new_parser = subparsers.add_parser(sub_function_name,description=description[sub_function_name],epilog=epilog)
        create_subparsers_recursive(new_parser,epilog,project_drs,
                                    functions_list+[sub_function_name,],childs,description,
                                            start_arguments_handles,
                                              arguments_handles,
                                              process_arguments_handles,
                                              chained_arguments_handles,
                                              previous_arguments_handles+current_arguments_handles+
                                              chained_arguments_handles[function_name])
    if record:
        if len(functions_list)==1:
            create_record_subparser(subparsers,project_drs,functions_list,
                                                        arguments_handles[function_name]+
                                                        start_arguments_handles[function_name]+
                                                        process_arguments_handles[function_name])
        else:
            create_record_subparser(subparsers,project_drs,functions_list,
                                                        previous_arguments_handles+
                                                        arguments_handles[function_name]+
                                                        process_arguments_handles[function_name])
    else:
        record_parser = subparsers.add_parser('record')
        create_subparsers_recursive(record_parser,epilog,project_drs,list(functions_list[:-1])+
                                                                     ['record',function_name],childs,description,
                                                        start_arguments_handles,
                                                          arguments_handles,
                                                          process_arguments_handles,
                                                          chained_arguments_handles,
                                                          previous_arguments_handles,
                                                          record=True)
    return 

def create_record_subparser(subparsers,project_drs,functions_list,previous_arguments_handles):
    parser=subparsers.add_parser('record')
    #functions_arguments(parser,functions_list)
    parser.prog=' '.join(parser.prog.split(' ')[:-2])

    arguments_attributed=[]
    for argument in previous_arguments_handles:
        if (not argument in arguments_attributed and
            'script' in argument.func_name.split('_')):
            argument(parser,project_drs)
            arguments_attributed.append(argument)
    for argument in previous_arguments_handles:
        if not argument in arguments_attributed:
            argument(parser,project_drs)
            arguments_attributed.append(argument)
    #if ('validate' in functions_list and
    #   'validate' != functions_list[-1]):
    #   if not record_validate in arguments_attributed:
    #       record_validate(parser,project_drs)
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

def csv_list(input):
    return [ item for item in input.split(',')]
