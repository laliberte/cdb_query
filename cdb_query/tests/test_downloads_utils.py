from cdb_query.utils import downloads_utils
from argparse import Namespace


def test_min_year_from_header_old(capsys):
    """ Test that downloads_utils is compatible with old headers """
    options = Namespace(silent=False)
    header = {"experiment_list":{"historical": "1980,2005",
                                 "rcp85": "2006,2099"}}
    min_year = downloads_utils.min_year_from_header(header, options)
    assert min_year is None

    header = {"experiment_list":{"historical": "1980,2005",
                                 "piControl": "1,200"}}
    min_year = downloads_utils.min_year_from_header(header, options)
    assert min_year == 1

    out, err = capsys.readouterr()
    assert out == 'Using min year 1 for experiment piControl\n'


def test_min_year_from_header_new(capsys):
    """ Test that downloads_utils is compatible with new headers """
    options = Namespace(silent=False)
    header = {"experiment_list":{"historical": ["1980,2005"],
                                 "rcp85": ["2006,2099"]}}
    min_year = downloads_utils.min_year_from_header(header, options)
    assert min_year is None

    header = {"experiment_list":{"historical": ["1980,2005"],
                                 "piControl": ["1,200", "300,400"]}}
    min_year = downloads_utils.min_year_from_header(header, options)
    assert min_year == 1

    out, err = capsys.readouterr()
    assert out == 'Using min year 1 for experiment piControl\n'
