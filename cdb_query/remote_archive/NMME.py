import copy

class DRS:
    project = __name__.split('.')[-1]

    file_types=['local_file', 'HTTPServer', 'GridFTP']
    remote_file_types=['HTTPServer','GridFTP','FTPServer','OPENDAP']
    required_file_types=['HTTPServer','OPENDAP']

    var_specs=['time_frequency','realm']

    official_drs=['institute',
                        'model',
                        'experiment',
                        'time_frequency',
                        'realm',
                        'ensemble',
                        'version',
                        'var']
    official_drs_no_version=copy.copy(official_drs)
    official_drs_no_version.remove('version')

    filename_drs=['var','time_frequency','model','experiment','ensemble']

    header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']
    base_drs=official_drs+[
              'file_type',
              'time',
              'data_node',
              'path'
              ]
    simulations_desc=['institute','model','ensemble']
    catalogue_missing_simulations_desc=True

    slicing_args={
                  'institute': [str,'Modelling institute name'],
                  'model': [str,'Model name'],
                  'experiment': [str,'Experiment name'],
                  'var': [str,'Variable name, e.g. tas'],
                  'time_frequency': [str,'Frequency, e.g. day'],
                  'realm': [str,'Realm, e.g. atmos'],
                  'ensemble': [str,'RIP identifier, e.g. r1i1p1']
                  }
                  #'file_type': [str,'File type: '+','.join(file_types)]
    discover_exclude_args=['experiment','var','time_frequency','realm','year','month','file_type']

    remote_fields=['institute','model','ensemble','file_type','version']

    known_fields=['experiment','var'] + var_specs
