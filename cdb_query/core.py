# External:
import sys
import os
import logging
import logging.handlers
import requests.packages.urllib3.exceptions

# Internal:
from . import parsers


def main():
    cdb_query_from_list(sys.argv)
    return


def cdb_query_from_list(args_list):
    options, project_drs = options_from_list(args_list)
    cdb_query_from_options(options, project_drs)
    return


def options_from_list(args_list):
    # Parser arguments:
    options, project_drs = parsers.full_parser(args_list)
    return options, project_drs


def check_clobber_mode_from_options(options):
    if (hasattr(options, 'out_netcdf_file') and
        parsers._isfile(options, 'out_netcdf_file') and
        not options.O and
       not options.A):
        # File exists and neither overwrite nor append were requested. Skip.
        print(('File {0} exists, skipping processing. '
               'To enable overwrite, use -O option. '
               'To enable appending, use -A option.')
              .format(options.out_netcdf_file))
        quit()


def logging_from_options(options):
    # https://docs.python.org/2/howto/logging-cookbook.html
    level = logging.WARNING
    if hasattr(options, 'debug') and options.debug:
        level = (logging.WARNING if (hasattr(options, 's') and options.s)
                 else logging.DEBUG)
    if (hasattr(options, 'log_files') and options.log_files and
       hasattr(options, 'out_netcdf_file')):
        logging.basicConfig(
                level=level,
                format=('%(processName)-10s %(asctime)s.%(msecs)03d '
                        '%(name)-12s:%(lineno)d %(levelname)-8s %(message)s'),
                datefmt='%m-%d %H:%M:%S',
                filename=options.out_netcdf_file+'.log',
                filemode='w')
    else:
        logging.basicConfig(level=level,
                            format=('%(processName)-20s %(asctime)s '
                                    '%(name)-12s:%(lineno)d %(levelname)-8s '
                                    '%(message)s'),
                            datefmt='%m-%d %H:%M')
    return


def cdb_query_from_options(options, project_drs):
    check_clobber_mode_from_options(options)
    logging_from_options(options)

    if (hasattr(options, 'debug') and options.debug):
        import warnings
        with warnings.catch_warnings():
            # netCDF4 implementation problems:
            warnings.filterwarnings('ignore', 'in the future, boolean '
                                              'array-likes will be handled as '
                                              'a boolean array index')
            warnings.filterwarnings('ignore', 'Unicode equal comparison '
                                              'failed to convert both '
                                              'arguments to Unicode - '
                                              'interpreting them as '
                                              'being unequal',
                                    append=True)
            warnings.filterwarnings('once', category=(requests
                                                      .packages
                                                      .urllib3
                                                      .exceptions
                                                      .InsecureRequestWarning),
                                    append=True)
            # raise all other warnings:
            warnings.filterwarnings('error', append=True)
            setup_queues_or_run_command(options, project_drs)
    else:
        setup_queues_or_run_command(options, project_drs)


def setup_queues_or_run_command(options, project_drs):
    # External:
    import multiprocessing
    import copy
    import shutil
    import tempfile

    # External but related:
    from .netcdf4_soft_links import certificates

    # Internal:
    from . import commands, queues_manager

    if (hasattr(options, 'related_experiments') and
       not options.related_experiments):
        project_drs.simulations_desc.append('experiment')

    if options.command in ['recipes']:
        this_dir, this_filename = os.path.split(__file__)
        DATA_PATH = os.path.join(
                this_dir, "recipes", project_drs.project,
                'recipe{0}.sh'.format(str(options.recipe_number).zfill(2)))
        shutil.copyfile(DATA_PATH, options.recipe_script)
    elif options.command in ['list_fields', 'merge']:
        database = commands.Database_Manager(project_drs)
        # Run the command:
        getattr(commands, options.command)(database, options)
    elif (options.command == 'ask' and
          hasattr(options, 'list_only_field') and
          options.list_only_field):
        database = commands.Database_Manager(project_drs)
        commands.ask(database, options)
    elif (options.command == 'reduce_server'):

        # Make tempdir:
        if hasattr(options, 'swap_dir'):
            options.swap_dir = tempfile.mkdtemp(dir=options.swap_dir)

        # Use a server:
        queues_manager.ReduceManager.register('get_manager')
        reduce_manager = queues_manager.ReduceManager(
                                        address=('', 50000),
                                        authkey='abracadabra')
        reduce_manager.connect()
        q_manager = reduce_manager.get_manager()
        try:
            queues_manager.reducer(q_manager, project_drs, options)
        finally:
            # Remove tempdir:
            shutil.rmtree(options.swap_dir)
    else:
        # Ask for username and password:
        options = certificates.prompt_for_username_and_password(options)

        # Make tempdir:
        if hasattr(options, 'swap_dir'):
            options.swap_dir = tempfile.mkdtemp(dir=options.swap_dir)

        # Create the queue manager:
        logging.debug('Starting queues manager')
        q_manager = queues_manager.CDB_queues_manager(options)
        processes = queues_manager.start_consumer_processes(
                                        q_manager, project_drs, options)
        try:
            # Start the queue consumer processes:
            options_copy = copy.copy(options)
            # Increment first queue and put:
            q_manager.increment_expected_and_put(options_copy)
            if (hasattr(options, 'start_server') and options.start_server):
                # Start a dedicated recorder process:
                processes['recorder'] = multiprocessing.Process(
                                            target=queues_manager.recorder,
                                            name='recorder',
                                            args=(q_manager, project_drs,
                                                  options))
                processes['recorder'].start()
                # Create server and serve:
                queues_manager.ReduceManager.register('get_manager',
                                                      lambda: q_manager)
                reduce_manager = queues_manager.ReduceManager(
                                                address=('', 50000),
                                                authkey='abracadabra')
                reduce_server = reduce_manager.get_server()
                print('Serving data on :', reduce_server.address)
                reduce_server.serve_forever()
            else:
                # Start record process:
                queues_manager.recorder(q_manager, project_drs, options)
        finally:
            # if (hasattr(options, 'start_server') and
            #    options.start_server):
            #     reduce_server.shutdown()
            pass

            # remove tempdir:
            shutil.rmtree(options.swap_dir)

            q_manager.stop_download_processes()
            for process_name in processes:
                if (process_name != multiprocessing.current_process().name and
                   processes[process_name].is_alive()):
                    processes[process_name].terminate()
    return


if __name__ == "__main__":
    sys.settrace
    main()
