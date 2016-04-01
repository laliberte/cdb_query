#External:
import sys
import select
import getpass

#External but related:
import netcdf4_soft_links.certificates as certificates

#Internal:
import remote_archive
import cdb_query_archive_parsers
import cdb_query_archive_class
import nc_Database_apply
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

    if (options.command=='certificates' and 
        'username' in dir(options) and options.username==None):
        options.username=raw_input('Enter Username: ')
        
    if 'username' in dir(options) and options.username!=None:
        if not options.password_from_pipe:
            options.password=getpass.getpass('Enter Credential phrase: ')
        else:
            timeout=1
            i,o,e=select.select([sys.stdin],[],[],timeout)
            if i:
                options.password=sys.stdin.readline()
            else:
                print '--password_from_pipe selected but no password was piped. Exiting.'
                return
        certificates.retrieve_certificates(options.username,options.service,user_pass=options.password,trustroots=options.no_trustroots)
    else:
        options.password=None

    if options.command!='certificates':
        if options.command=='list_fields':
            apps_class=cdb_query_archive_class.SimpleTree(project_drs)
            #Run the command:
            getattr(apps_class,options.command)(options)
        else:
            manager=queues_manager.CDB_queue_manager(options)
            apps_class=cdb_query_archive_class.SimpleTree(project_drs,queues_manager=manager)
            #Run the command:
            getattr(apps_class,options.command)(options)
            queues_manager.recorder(manager,project_drs,options)
        
if __name__ == "__main__":
    main('CMIP5')
