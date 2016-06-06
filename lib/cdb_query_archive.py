#External:
import sys
import select
import getpass
import multiprocessing
import copy
import argparse 
import textwrap
import numpy as np

#External but related:
import netcdf4_soft_links.certificates as certificates
import netcdf4_soft_links.retrieval_manager as retrieval_manager

#Internal:
import remote_archive
import cdb_query_archive_parsers
import cdb_query_archive_class
import queues_manager


def main():
    cdb_query_from_list(sys.argv)
    return

def cdb_query_from_list(args_list):
    """
    Takes an argument list that can be easily generated by converting a command line string \'cmd\' using \'[arg for arg in cmd.split(' ') if arg]\'
    """

    version_num='1.9.5'
    #Option parser
    description=textwrap.dedent('''\
    This script queries an ESGF project. It can query:
    1. a local archive that follows the project DRS
    filename convention and directory structure.
    2. the ESGF project archive.

    ''')
    epilog='Version {0}: Frederic Laliberte (06/2016),\n\
Previous versions: Frederic Laliberte, Paul Kushner (2011-2015).\n\
\n\
If using this code to retrieve and process data from the ESGF please cite:\n\n\
Efficient, robust and timely analysis of Earth System Models: a database-query approach (2016):\n\
F. Laliberte, Juckes, M., Denvil, S., Kushner, P. J., TBD'.format(version_num)

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

    options,commands_args=project_parser.parse_known_args(args=args_list[1:])
    #This is an ad-hoc patch to allow chained subcommands:
    cli=['certificates','list_fields','merge','ask','validate','download_files',
                            'reduce_soft_links','download_opendap','reduce']
    if (not 'process' in commands_args and
        len(np.intersect1d(cli,commands_args))>0):
        index_process=np.max(
                            np.argsort(commands_args)[
                            np.searchsorted(commands_args,
                                      np.intersect1d(cli,commands_args),
                                      sorter=np.argsort(commands_args))])
        commands_args.insert(index_process+1,'process')

    if 'h' in dir(options) and options.h:
        #Restore the option:
        commands_args.append('-h')

    #Then parser project-specific arguments:
    project_drs=remote_archive.DRS(options.project)
    command_parser = argparse.ArgumentParser(
                            prog=prog+' '+options.project,
                            formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            version=prog+' '+version_num,
                            epilog=epilog)

    #Generate subparsers
    cdb_query_archive_parsers.generate_subparsers(command_parser,epilog,project_drs)


    options=command_parser.parse_args(commands_args,namespace=options)

    if 'related_experiments' in dir(options) and not options.related_experiments:
        project_drs.simulations_desc.append('experiment')

    #Slicing time is peculiar
    for time_opt in ['year','month']:
        if time_opt in dir(options) and getattr(options,time_opt):
            options.time=True
    
    #Set two defaults:
    options.trial=0
    options.priority=0

    if options.command!='certificates':
        if options.command in ['list_fields','merge']:
            database=cdb_query_archive_class.Database_Manager(project_drs)
            #Run the command:
            getattr(cdb_query_archive_class,options.command)(database,options)
        elif (options.command == 'reduce' and
             'start_server' in options and
             options.start_server):
            #Use a server:
            queues_manager.ReduceManager.register('get_manager')
            reduce_manager=queues_manager.ReduceManager(address=('',50000),authkey='abracadabra')
            reduce_manager.connect()
            q_manager=reduce_manager.get_manager()
            queues_manager.reducer(q_manager,project_drs,options)
        else:
            #Ask for username and password:
            options=certificates.prompt_for_username_and_password(options)

            #Create the queue manager:
            q_manager=queues_manager.CDB_queues_manager(options)
            processes=queues_manager.start_consumer_processes(q_manager,project_drs,options)
            try:
                #Start the queue consumer processes:
                options_copy=copy.copy(options)
                #Increment first queue and put:
                q_manager.increment_expected_and_put((q_manager.queues_names[0],options_copy))
                if ('start_server' in dir(options) and options.start_server):
                    #Start a dedicated recorder process:
                    processes['recorder']=multiprocessing.Process(target=queues_manager.recorder,
                                                                   name='recorder',
                                                                   args=(q_manager,project_drs,options))
                    processes['recorder'].start()
                    #Create server and serve:
                    queues_manager.ReduceManager.register('get_manager',lambda:q_manager,['increment_expected_and_put','put_to_next','remove','get_reduce_no_record'])
                    reduce_manager=queues_manager.ReduceManager(address=('',50000),authkey='abracadabra')
                    reduce_server=reduce_manager.get_server()
                    print('Serving data on :',reduce_server.address)
                    reduce_server.serve_forever()
                else:
                    #Start record process:
                    queues_manager.recorder(q_manager,project_drs,options)
            finally:
                if ('start_server' in dir(options) and options.start_server):
                    reduce_server.shutdown()
                q_manager.stop_download_processes()
                for process_name in processes.keys():
                    if process_name!=multiprocessing.current_process().name:
                        if processes[process_name].is_alive():
                            processes[process_name].terminate()
    else:
        options=certificates.prompt_for_username_and_password(options)
        
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

if __name__ == "__main__":
    sys.settrace
    main()

