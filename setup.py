# This Python file uses the following encoding: utf-8
from setuptools import setup, find_packages


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    import os
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


package_name = 'cdb_query'
setup(name=package_name,
      version="1.9.9.9.6",
      packages=find_packages(exclude=['test']),
      # metadata for upload to PyPI
      author="F. B. Laliberte, P. J. Kushner",
      author_email="frederic.laliberte@utoronto.ca",
      description=("Simple tools to query and retrieve data from the "
                   "ESGF's CMIP5 and CORDEX projects."),
      license="BSD",
      keywords="atmosphere climate",
      classifiers=["Development Status :: 4 - Beta",
                   "Operating System :: POSIX :: Linux",
                   "Operating System :: MacOS :: MacOS X",
                   "Intended Audience :: Science/Research",
                   "Natural Language :: English",
                   "License :: OSI Approved :: BSD License",
                   "Programming Language :: Python :: 2.7",
                   "Programming Language :: Python :: 2 :: Only",
                   "Topic :: Scientific/Engineering :: Atmospheric Science",
                   "Topic :: Scientific/Engineering :: Mathematics",
                   "Topic :: Scientific/Engineering :: Physics"],
      long_description=read('README.rst'),
      install_requires=['numpy',
                        'h5py',
                        'h5netcdf>=0.3',
                        'netCDF4',
                        'sqlalchemy>=1.0',
                        'beautifulsoup4',
                        'requests>=1.1.0',
                        'requests_cache',
                        'pydap==3.1.1',
                        'singledispatch',
                        'Webob',
                        'Jinja2',
                        'docopt',
                        'gunicorn',
                        'six >= 1.4.0',
                        'mechanicalsoup'],
      zip_safe=False,
      extras_require={'testing': ['flake8', 'pytest-cov', 'pytest-xdist']},
      entry_points={'console_scripts': [package_name + '= ' +
                                        package_name + '.core:main']})
