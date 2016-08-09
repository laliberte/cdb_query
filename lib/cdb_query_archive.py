#External:
import sys
import select
import getpass
import multiprocessing
import copy
import argparse 
import textwrap
import numpy as np
import shutil
import tempfile

#External but related:
import netcdf4_soft_links.certificates as certificates
import netcdf4_soft_links.retrieval_manager as retrieval_manager

#Internal:
from . import cdb_query_archive_parsers, cdb_query_archive_class, queues_manager

def main():
    cdb_query_from_list(sys.argv)
    return

def cdb_query_from_list(args_list):
    #Parser arguments:
    options, project_drs = cdb_query_archive_parsers.full_parser(args_list)

    if 'related_experiments' in dir(options) and not options.related_experiments:
        project_drs.simulations_desc.append('experiment')
    
    if options.command!='certificates':
        if options.command in ['list_fields','merge']:
            database=cdb_query_archive_class.Database_Manager(project_drs)
            #Run the command:
            getattr(cdb_query_archive_class,options.command)(database,options)
        elif (options.command == 'ask' and
             'list_only_field' in options and
             options.list_only_field):
            database=cdb_query_archive_class.Database_Manager(project_drs)
            cdb_query_archive_class.ask(database,options)
        elif (options.command == 'reduce' and
             'start_server' in options and
             options.start_server):

            #Make tempdir:
            if 'swap_dir' in dir(options):
               options.swap_dir = tempfile.mkdtemp(dir=options.swap_dir) 

            #Use a server:
            queues_manager.ReduceManager.register('get_manager')
            reduce_manager=queues_manager.ReduceManager(address=('',50000),authkey='abracadabra')
            reduce_manager.connect()
            q_manager=reduce_manager.get_manager()
            try:
                queues_manager.reducer(q_manager,project_drs,options)
            finally:
                #remove tempdir:
                shutil.rmtree(options.swap_dir)
        else:
            #Ask for username and password:
            options=certificates.prompt_for_username_and_password(options)

            #Make tempdir:
            if 'swap_dir' in dir(options):
               options.swap_dir = tempfile.mkdtemp(dir=options.swap_dir) 

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

                #remove tempdir:
                shutil.rmtree(options.swap_dir)

                q_manager.stop_download_processes()
                for process_name in processes.keys():
                    if process_name!=multiprocessing.current_process().name:
                        if processes[process_name].is_alive():
                            processes[process_name].terminate()
    else:
        options=certificates.prompt_for_username_and_password(options)
        

if __name__ == "__main__":
    sys.settrace
    main()

