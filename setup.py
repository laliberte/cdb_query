# This Python file uses the following encoding: utf-8


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    import os
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

from setuptools import setup, find_packages

package_name='cdb_query'
setup(
       name = package_name,
       version = "1.9.9.6",
       packages = find_packages(),
       #package_dir = {package_name: 'lib'},
#
#        # metadata for upload to PyPI
        author = "F. B. Laliberte, P. J. Kushner",
        author_email = "frederic.laliberte@utoronto.ca",
        description = "Simple tools to query and retrieve data from the ESGF's CMIP5 and CORDEX projects.",
        license = "BSD",
        keywords = "atmosphere climate",
        url = "http://proj.badc.rl.ac.uk/exarch",   # project home page, if any
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Science/Research",
            "Natural Language :: English",
            "License :: OSI Approved :: BSD License",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Fortran",
            "Topic :: Scientific/Engineering :: Atmospheric Science",
            "Topic :: Scientific/Engineering :: Mathematics"
        ],
        long_description=read('README'),
        install_requires = ['numpy',
                            'h5py',
                            'h5netcdf>=0.3',
                            'netCDF4',
                            'sqlalchemy>=1.0',
                            'esgf-pyclient',
                            'netcdf4_soft_links>=0.7.7',
                            'bs4'],
        zip_safe=False,
        # other arguments here...
        #package_data = {package_name : ['lib/*.sh']},
        entry_points = {
                  'console_scripts': [
                           package_name+'= '+package_name+'.core:main',
                                     ],
                       }
    )
