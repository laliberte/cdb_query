import copy

class DRS:
    project = __name__.split('.')[-1]

    file_types=['local_file', 'FTPServer']
    remote_file_types=['FTPServer']

    var_specs=['time_frequency','realm']

    official_drs=[
                        'model',
                        'experiment',
                        'time_frequency',
                        'realm',
                        'var',
                        'ensemble'
                        ]
                        #'version'
    #official_drs_no_version=copy.copy(official_drs)
    #official_drs_no_version.remove('version')

    filename_drs=['var','cmor_table','model','experiment','ensemble']

    header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']
    base_drs=official_drs+[
              'file_type',
              'time',
              'data_node',
              'path'
              ]
    simulations_desc=['model','ensemble']
    catalogue_missing_simulations_desc=True

    slicing_args={
                  'model': [str,'Model name'],
                  'experiment': [str,'Experiment name'],
                  'var': [str,'Variable name, e.g. tas'],
                  'time_frequency': [str,'Frequency, e.g. day'],
                  'realm': [str,'Realm, e.g. atmos'],
                  'ensemble': [str,'RIP identifier, e.g. r1i1p1']
                  }
                  #'file_type': [str,'File type: '+','.join(file_types)]
    discover_exclude_args=['experiment','var','time_frequency','realm','year','month','file_type']

    remote_fields=['model','ensemble','file_type']

    known_fields=['experiment','var'] + var_specs
