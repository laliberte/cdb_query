import copy

class DRS:
    def __init__(self,project):
        self.project=project
        getattr(self,project)()
        return

    def CORDEX(self):
        self.file_types=['local_file', 'HTTPServer', 'GridFTP']
        self.remote_file_types=['HTTPServer','GridFTP']
        self.required_file_types=['HTTPServer']

        self.var_specs=['time_frequency']
        #self.simulations_desc=['domain','institute','driving_model','model_version','model','ensemble']
        self.simulations_desc=['domain','institute','driving_model','model','ensemble']

        self.header_desc=['search_list','file_type_list','months_list','data_node_list','experiment_list','variable_list']

        self.filename_drs=['var','domain','driving_model','experiment','ensemble','model','version','time_frequency']
        self.official_drs=[
              'domain',
              'institute',
              'driving_model',
              'experiment',
              'ensemble',
              'model',
              'model_version',
              'time_frequency',
              'var',
              'version'
              ]
        self.official_drs_no_version=copy.copy(self.official_drs)
        self.official_drs_no_version.remove('version')
        self.base_drs=self.official_drs+[
                  'file_type',
                  'time',
                  'data_node',
                  'path'
              ]
        
        self.slicing_args={
              'domain': [str,'Modelling domain'],
              'institute': [str,'Modelling institute name'],
              'driving_model': [str,'Drinving model name'],
              'experiment': [str,'Experiment name'],
              'ensemble': [str,'RIP identifier, e.g. r1i1p1'],
              'model': [str,'Model name'],
              'time_frequency': [str,'Frequency, e.g. day'],
              'var': [str,'Variable name, e.g. tas']
              }
              #'file_type': [str,'File type: '+','.join(self.file_types)]
        self.discover_exclude_args=['experiment','var','time_frequency','year','month','file_type']

        self.remote_fields=['institute','model','model_version','ensemble','driving_model','domain','file_type','version']
        self.known_fields=['experiment','var'] + self.var_specs

        self.alt_base_drs=[
              'domain',
              'institute',
              'driving_model',
              'experiment',
              'ensemble',
              'model',
              'time_frequency',
              'var',
              'version',
              'file_type',
              'time',
              'data_node',
              'path'
              ]
        return

    def CMIP5(self):
        self.file_types=['local_file', 'HTTPServer', 'GridFTP']
        self.remote_file_types=['HTTPServer','GridFTP']
        self.required_file_types=['HTTPServer','OPENDAP']

        self.var_specs=['time_frequency','realm','cmor_table']

        self.official_drs=['institute',
                            'model',
                            'experiment',
                            'time_frequency',
                            'realm',
                            'cmor_table',
                            'ensemble',
                            'version',
                            'var']
        self.official_drs_no_version=copy.copy(self.official_drs)
        self.official_drs_no_version.remove('version')

        self.filename_drs=['var','cmor_table','model','experiment','ensemble']

        self.header_desc=['search_list','file_type_list','months_list','data_node_list','experiment_list','variable_list']
        self.base_drs=self.official_drs+[
                  'file_type',
                  'time',
                  'data_node',
                  'path'
                  ]
        self.simulations_desc=['institute','model','ensemble']

        self.slicing_args={
                      'institute': [str,'Modelling institute name'],
                      'model': [str,'Model name'],
                      'experiment': [str,'Experiment name'],
                      'var': [str,'Variable name, e.g. tas'],
                      'time_frequency': [str,'Frequency, e.g. day'],
                      'realm': [str,'Realm, e.g. atmos'],
                      'cmor_table': [str,'CMOR table name, e.g. day'],
                      'ensemble': [str,'RIP identifier, e.g. r1i1p1']
                      }
                      #'file_type': [str,'File type: '+','.join(self.file_types)]
        self.discover_exclude_args=['experiment','var','time_frequency','realm','cmor_table','year','month','file_type']

        self.remote_fields=['institute','model','ensemble','file_type','version']

        self.known_fields=['experiment','var'] + self.var_specs

        return

