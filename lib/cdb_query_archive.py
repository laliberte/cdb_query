#External:
import sys
import select
import getpass
import multiprocessing
import copy

#External but related:
import netcdf4_soft_links.certificates as certificates
import netcdf4_soft_links.retrieval_manager as retrieval_manager

#Internal:
import remote_archive
import cdb_query_archive_parsers
import cdb_query_archive_class
import queues_manager


def main_CMIP5():
    main('CMIP5')
    return

def main_CORDEX():
    main('CORDEX')
    return

def main_NMME():
    main('NMME')
    return

def main_LRFTIP():
    main('LRFTIP')
    return

def main(project):
    import argparse 
    import textwrap

    #Option parser
    version_num='1.7rc1'
    description=textwrap.dedent('''\
    This script queries a {0} archive. It can query:
    1. a local POSIX-based archive that follows the {0} DRS
    filename convention and directory structure.
    2. the ESGF {0} archive.

    '''.format(project))
    epilog='Version {0}: Frederic Laliberte (03/2016),\n\
Previous versions: Frederic Laliberte, Paul Kushner (2011-2015).\n\
\n\
If using this code to retrieve and process data from the ESGF please cite:\n\n\
Efficient, robust and timely analysis of Earth System Models: a database-query approach (2016):\n\
F. Laliberte, Juckes, M., Denvil, S., Kushner, P. J., TBD, Submitted.'.format(version_num)
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            version='%(prog)s '+version_num,
                            epilog=epilog)

    project_drs=remote_archive.DRS(project)

    #Generate subparsers
    cdb_query_archive_parsers.generate_subparsers(parser,epilog,project_drs)

    options=parser.parse_args()

    if 'related_experiments' in dir(options) and not options.related_experiments:
        project_drs.simulations_desc.append('experiment')

    #Slicing time is peculiar
    for time_opt in ['year','month']:
        if time_opt in dir(options) and getattr(options,time_opt):
            options.time=True
    
    #Ask for username and password:
    options=certificates.prompt_for_username_and_password(options)

    if options.command!='certificates':
        if options.command in ['list_fields','merge']:
            database=cdb_query_archive_class.Database_Manager(project_drs)
            #Run the command:
            getattr(cdb_query_archive_class,options.command)(database,options)
        else:
            #Create the queue manager:
            q_manager=queues_manager.CDB_queues_manager(options)
            processes=queues_manager.start_consumer_processes(q_manager,project_drs,options)
            try:
                #Start the queue consumer processes:

                #Put the first command in the queue in spin_up mode:
                #options_copy.spin_up=True

                options_copy=copy.copy(options)
                #Increment first queue and put:
                getattr(q_manager,q_manager.queues_names[0]+'_expected').increment()
                q_manager.put((q_manager.queues_names[0],options_copy))
                #Start record process:
                queues_manager.recorder(q_manager,project_drs,options)
            finally:
                q_manager.stop_download_processes()
                for process_name in processes.keys():
                    if process_name!=multiprocessing.current_process().name:
                        if processes[process_name].is_alive():
                            processes[process_name].terminate()
        
if __name__ == "__main__":
    main('CMIP5')
