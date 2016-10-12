import copy

class DRS:
    project = __name__.split('.')[-1]

    product = 'reanalysis'
    file_types=['local_file', 'HTTPServer','OPENDAP']
    remote_file_types=['HTTPServer','OPENDAP','FTPServer']
    required_file_types=['HTTPServer']

    var_specs=['time_frequency','realm']
    simulations_desc=['institute','model']

    header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']

    filename_drs=['var','time_frequency','product','experiment']
    official_drs=[
          'product',
          'institute',
          'model',
          'experiment',
          'time_frequency',
          'realm',
          'var',
          'version'
          ]
    official_drs_no_version=copy.copy(official_drs)
    official_drs_no_version.remove('version')

    base_drs=official_drs+[
              'file_type',
              'time',
              'data_node',
              'path'
          ]

    slicing_args={
          'institute': [str,'Modelling institute name'],
          'model': [str,'Model name'],
          'product': [str,'Product name'],
          'experiment': [str,'Experiment name'],
          'realm': [str,'Realm, e.g. atmos'],
          'time_frequency': [str,'Frequency, e.g. day'],
          'var': [str,'Variable name, e.g. tas']
          }
    discover_exclude_args=['experiment','var','time_frequency','year','month','file_type']

    remote_fields=['product','institute','model','version','file_type']
    known_fields=['experiment','var'] + var_specs

    #aliases={'rcm_model':['rcm_name','rcm_model','model']}

    alt_base_drs=[
          'product',
          'institute',
          'model',
          'experiment',
          'ensemble',
          'time_frequency',
          'var',
          'version',
          'file_type',
          'time',
          'data_node',
          'path'
          ]
    need_instant_time = [('tas', '3hr', 'atmos'), ('psl', '3hr', 'atmos'), ('ps', '3hr', 'atmos'), ('huss', '3hr', 'atmos'), ('sfcWind', '3hr', 'atmos'), ('mrfso', '6hr', 'atmos'), ('mrso', '6hr', 'atmos'), ('snw', '6hr', 'atmos'), ('uas', '6hr', 'atmos'), ('vas', '6hr', 'atmos'), ('ts', '6hr', 'atmos'), ('zmla', '6hr', 'atmos'), ('prw', '6hr', 'atmos'), ('clwvi', '6hr', 'atmos'), ('clivi', '6hr', 'atmos'), ('ua850', '6hr', 'atmos'), ('va850', '6hr', 'atmos'), ('ta850', '6hr', 'atmos'), ('hus850', '6hr', 'atmos'), ('ua500', '6hr', 'atmos'), ('va500', '6hr', 'atmos'), ('zg500', '6hr', 'atmos'), ('ta500', '6hr', 'atmos'), ('ua200', '6hr', 'atmos'), ('va200', '6hr', 'atmos'), ('ta200', '6hr', 'atmos'), ('zg200', '6hr', 'atmos'), ('snc', '6hr', 'atmos'), ('snd', '6hr', 'atmos')]
