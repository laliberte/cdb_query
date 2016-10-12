import copy

class DRS:
    project = __name__.split('.')[-1]

    file_types=['local_file', 'HTTPServer', 'GridFTP','OPENDAP']
    remote_file_types=['HTTPServer','GridFTP','OPENDAP','FTPServer']
    required_file_types=['HTTPServer']

    var_specs=['time_frequency']
    simulations_desc=['domain','institute','driving_model','rcm_model','rcm_version','ensemble']

    header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']

    filename_drs=['var','domain','driving_model','experiment','ensemble','rcm_model','version','time_frequency']
    official_drs=[
          'domain',
          'institute',
          'driving_model',
          'experiment',
          'ensemble',
          'rcm_model',
          'rcm_version',
          'time_frequency',
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
          'domain': [str,'Modelling domain'],
          'institute': [str,'Modelling institute name'],
          'driving_model': [str,'Drinving model name'],
          'experiment': [str,'Experiment name'],
          'ensemble': [str,'RIP identifier, e.g. r1i1p1'],
          'rcm_model': [str,'RCM model name'],
          'rcm_version': [str,'RCM model version'],
          'time_frequency': [str,'Frequency, e.g. day'],
          'var': [str,'Variable name, e.g. tas']
          }
          #'file_type': [str,'File type: '+','.join(file_types)]
    discover_exclude_args=['experiment','var','time_frequency','year','month','file_type']

    remote_fields=['institute','rcm_model','rcm_version','ensemble','driving_model','domain','file_type','version']
    known_fields=['experiment','var'] + var_specs

    aliases={'rcm_model':['rcm_name','rcm_model','model']}

    alt_base_drs=[
          'domain',
          'institute',
          'driving_model',
          'experiment',
          'ensemble',
          'rcm_model',
          'rcm_version',
          'time_frequency',
          'var',
          'version',
          'file_type',
          'time',
          'data_node',
          'path'
          ]
    need_instant_time = [('tas', '3hr', 'atmos'), ('psl', '3hr', 'atmos'), ('ps', '3hr', 'atmos'), ('huss', '3hr', 'atmos'), ('sfcWind', '3hr', 'atmos'), ('mrfso', '6hr', 'atmos'), ('mrso', '6hr', 'atmos'), ('snw', '6hr', 'atmos'), ('uas', '6hr', 'atmos'), ('vas', '6hr', 'atmos'), ('ts', '6hr', 'atmos'), ('zmla', '6hr', 'atmos'), ('prw', '6hr', 'atmos'), ('clwvi', '6hr', 'atmos'), ('clivi', '6hr', 'atmos'), ('ua850', '6hr', 'atmos'), ('va850', '6hr', 'atmos'), ('ta850', '6hr', 'atmos'), ('hus850', '6hr', 'atmos'), ('ua500', '6hr', 'atmos'), ('va500', '6hr', 'atmos'), ('zg500', '6hr', 'atmos'), ('ta500', '6hr', 'atmos'), ('ua200', '6hr', 'atmos'), ('va200', '6hr', 'atmos'), ('ta200', '6hr', 'atmos'), ('zg200', '6hr', 'atmos'), ('snc', '6hr', 'atmos'), ('snd', '6hr', 'atmos')]
