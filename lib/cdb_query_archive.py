import os
import hashlib

import filesystem_query
import esgf_query

import valid_experiments_path
import valid_experiments_time

import copy

#import database_utils
import json_tools

import tree_utils
from tree_utils import File_Expt

import cdb_query_archive_parsers
import cdb_query_archive_class

def main():
    import argparse 
    import textwrap

    #Option parser
    description=textwrap.dedent('''\
    This script queries an ESGF archive. It can query a
    local POSIX-based archive following the CMIP5 DRS
    filename convention and directory structure.

    In the future it should become able to query the THREDDS
    catalog of the ESGF and provide a simple interface to
    the OPEnDAP services.
    ''')
    epilog='Frederic Laliberte, Paul Kushner 10/2012'
    version_num='0.2'
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            version='%(prog)s '+version_num,
                            epilog=epilog)

    subparsers = parser.add_subparsers(help='commands',dest='command')

    #Generate subparsers
    cdb_query_archive_parsers.generate_subparsers(subparsers,epilog)

    options=parser.parse_args()

    #Slicing time is perculiar
    for time_opt in ['year','month']:
        if time_opt in dir(options) and getattr(options,time_opt):
            options.time=True

    #Load pointer file:
    paths_dict=cdb_query_archive_class.SimpleTree(json_tools.open_json(options))
    #Run the command:
    getattr(paths_dict,options.command)(options)
    #print paths_dict.pointers.tree
    #Close the file:
    if 'out_diagnostic_headers_file' in dir(options):
        json_tools.close_json(paths_dict,options)

if __name__ == "__main__":
    main()
