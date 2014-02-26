import os
import hashlib

import filesystem_query
import esgf_query

import copy

import remote_archive

#import database_utils
import io_tools

import cdb_query_archive_parsers
import cdb_query_archive_class

import netcdf_soft_links
import netcdf_utils

def main_CMIP5():
    main('CMIP5')
    return

def main_CORDEX():
    main('CORDEX')
    return

def main(project):
    import argparse 
    import textwrap

    #Option parser
    description=textwrap.dedent('''\
    This script queries a {0} archive. It can query:
    1. a local POSIX-based archive that follows the {0} DRS
    filename convention and directory structure.
    2. the ESGF {0} archive.

    '''.format(project))
    epilog='Frederic Laliberte, Paul Kushner 02/2014'
    version_num='0.9'
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            version='%(prog)s '+version_num,
                            epilog=epilog)

    project_drs=remote_archive.DRS(project)

    #Generate subparsers
    cdb_query_archive_parsers.generate_subparsers(parser,epilog,project_drs)

    options=parser.parse_args()

    #Slicing time is peculiar
    for time_opt in ['year','month']:
        if time_opt in dir(options) and getattr(options,time_opt):
            options.time=True

    #Load pointer file:
    if options.command=='remote_retrieve':
        options.netcdf=True
        netcdf_soft_links.retrieve_data(options,project_drs)
    elif options.command=='download':
        options.netcdf=False
        netcdf_soft_links.retrieve_data(options,project_drs)
    elif options.command=='apply':
        netcdf_utils.apply(options,project_drs)
    elif 'in_diagnostic_headers_file' in dir(options):
        paths_dict=cdb_query_archive_class.SimpleTree(options,project_drs)
        #Run the command:
        getattr(paths_dict,options.command)(options)
    elif 'in_diagnostic_netcdf_file' in dir(options):
        paths_dict=cdb_query_archive_class.SimpleTree(options,project_drs)
        #Run the command:
        getattr(paths_dict,options.command)(options)
        
if __name__ == "__main__":
    main('CMIP5')
