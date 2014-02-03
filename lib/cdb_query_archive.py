import os
import hashlib

import filesystem_query
import esgf_query

import valid_experiments_path
import valid_experiments_time

import copy

import remote_archive

#import database_utils
import io_tools

import tree_utils
from tree_utils import File_Expt

import cdb_query_archive_parsers
import cdb_query_archive_class

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
        netcdf_utils.retrieve_data(options,project_drs)
    elif 'in_diagnostic_headers_file' in dir(options):
        paths_dict=cdb_query_archive_class.SimpleTree(io_tools.open_json(options),options,project_drs)
        #Run the command:
        getattr(paths_dict,options.command)(options)
    elif 'in_diagnostic_netcdf_file' in dir(options):
        paths_dict=cdb_query_archive_class.SimpleTree(io_tools.open_netcdf(options,project_drs),options,project_drs)
        #Run the command:
        getattr(paths_dict,options.command)(options)
        
    #print paths_dict.pointers.tree
    #Close the file:
    if 'out_diagnostic_headers_file' in dir(options):
        io_tools.close_json(paths_dict,options)

if __name__ == "__main__":
    main('CMIP5')
