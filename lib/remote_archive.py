import copy

#available_projects=['CMIP5','CORDEX','NMME','LFRTIP']
available_projects=['CMIP5','CORDEX','CanSISE','CREATEIP']

class DRS:
    def __init__(self,project):
        self.project=project
        getattr(self,project)()
        return

    def CREATEIP(self):
        self.product = 'reanalysis'
        self.file_types=['local_file', 'HTTPServer','OPENDAP']
        self.remote_file_types=['HTTPServer','OPENDAP','FTPServer']
        self.required_file_types=['HTTPServer']

        self.var_specs=['time_frequency','realm']
        self.simulations_desc=['institute','model']

        self.header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']

        self.filename_drs=['var','time_frequency','product','experiment']
        self.official_drs=[
              'product',
              'institute',
              'model',
              'experiment',
              'time_frequency',
              'realm',
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
              'institute': [str,'Modelling institute name'],
              'model': [str,'Model name'],
              'product': [str,'Product name'],
              'experiment': [str,'Experiment name'],
              'realm': [str,'Realm, e.g. atmos'],
              'time_frequency': [str,'Frequency, e.g. day'],
              'var': [str,'Variable name, e.g. tas']
              }
        self.discover_exclude_args=['experiment','var','time_frequency','year','month','file_type']

        self.remote_fields=['product','institute','model','version','file_type']
        self.known_fields=['experiment','var'] + self.var_specs

        #self.aliases={'rcm_model':['rcm_name','rcm_model','model']}

        self.alt_base_drs=[
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
        self.need_instant_time = [('tas', '3hr', 'atmos'), ('psl', '3hr', 'atmos'), ('ps', '3hr', 'atmos'), ('huss', '3hr', 'atmos'), ('sfcWind', '3hr', 'atmos'), ('mrfso', '6hr', 'atmos'), ('mrso', '6hr', 'atmos'), ('snw', '6hr', 'atmos'), ('uas', '6hr', 'atmos'), ('vas', '6hr', 'atmos'), ('ts', '6hr', 'atmos'), ('zmla', '6hr', 'atmos'), ('prw', '6hr', 'atmos'), ('clwvi', '6hr', 'atmos'), ('clivi', '6hr', 'atmos'), ('ua850', '6hr', 'atmos'), ('va850', '6hr', 'atmos'), ('ta850', '6hr', 'atmos'), ('hus850', '6hr', 'atmos'), ('ua500', '6hr', 'atmos'), ('va500', '6hr', 'atmos'), ('zg500', '6hr', 'atmos'), ('ta500', '6hr', 'atmos'), ('ua200', '6hr', 'atmos'), ('va200', '6hr', 'atmos'), ('ta200', '6hr', 'atmos'), ('zg200', '6hr', 'atmos'), ('snc', '6hr', 'atmos'), ('snd', '6hr', 'atmos')]
        return

    def CORDEX(self):
        self.file_types=['local_file', 'HTTPServer', 'GridFTP','OPENDAP']
        self.remote_file_types=['HTTPServer','GridFTP','OPENDAP','FTPServer']
        self.required_file_types=['HTTPServer']

        self.var_specs=['time_frequency']
        #self.simulations_desc=['domain','institute','driving_model','model_version','model','ensemble']
        self.simulations_desc=['domain','institute','driving_model','rcm_model','rcm_version','ensemble']

        self.header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']

        self.filename_drs=['var','domain','driving_model','experiment','ensemble','rcm_model','version','time_frequency']
        self.official_drs=[
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
              'rcm_model': [str,'RCM model name'],
              'rcm_version': [str,'RCM model version'],
              'time_frequency': [str,'Frequency, e.g. day'],
              'var': [str,'Variable name, e.g. tas']
              }
              #'file_type': [str,'File type: '+','.join(self.file_types)]
        self.discover_exclude_args=['experiment','var','time_frequency','year','month','file_type']

        self.remote_fields=['institute','rcm_model','rcm_version','ensemble','driving_model','domain','file_type','version']
        self.known_fields=['experiment','var'] + self.var_specs

        self.aliases={'rcm_model':['rcm_name','rcm_model','model']}

        self.alt_base_drs=[
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
        self.need_instant_time = [('tas', '3hr', 'atmos'), ('psl', '3hr', 'atmos'), ('ps', '3hr', 'atmos'), ('huss', '3hr', 'atmos'), ('sfcWind', '3hr', 'atmos'), ('mrfso', '6hr', 'atmos'), ('mrso', '6hr', 'atmos'), ('snw', '6hr', 'atmos'), ('uas', '6hr', 'atmos'), ('vas', '6hr', 'atmos'), ('ts', '6hr', 'atmos'), ('zmla', '6hr', 'atmos'), ('prw', '6hr', 'atmos'), ('clwvi', '6hr', 'atmos'), ('clivi', '6hr', 'atmos'), ('ua850', '6hr', 'atmos'), ('va850', '6hr', 'atmos'), ('ta850', '6hr', 'atmos'), ('hus850', '6hr', 'atmos'), ('ua500', '6hr', 'atmos'), ('va500', '6hr', 'atmos'), ('zg500', '6hr', 'atmos'), ('ta500', '6hr', 'atmos'), ('ua200', '6hr', 'atmos'), ('va200', '6hr', 'atmos'), ('ta200', '6hr', 'atmos'), ('zg200', '6hr', 'atmos'), ('snc', '6hr', 'atmos'), ('snd', '6hr', 'atmos')]
        return

    def CMIP5(self):
        self.file_types=['local_file', 'HTTPServer', 'GridFTP','FTPServer','OPENDAP']
        self.remote_file_types=['HTTPServer','GridFTP','OPENDAP','FTPServer']
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

        self.header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']
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

        self.need_instant_time = [('tas', '3hr', 'atmos'), ('uas', '3hr', 'atmos'), ('vas', '3hr', 'atmos'), ('huss', '3hr', 'atmos'), ('mrsos', '3hr', 'land'), ('tslsi', '3hr', 'land'), ('tso', '3hr', 'ocean'), ('ps', '3hr', 'atmos'), ('ua', '6hrPlev', 'atmos'), ('va', '6hrPlev', 'atmos'), ('ta', '6hrPlev', 'atmos'), ('psl', '6hrPlev', 'atmos'), ('ta', '6hrLev', 'atmos'), ('ua', '6hrLev', 'atmos'), ('va', '6hrLev', 'atmos'), ('hus', '6hrLev', 'atmos'), ('ps', '6hrLev', 'atmos'), ('clcalipso', 'cf3hr', 'atmos'), ('clcalipso2', 'cf3hr', 'atmos'), ('cfadDbze94', 'cf3hr', 'atmos'), ('cfadLidarsr532', 'cf3hr', 'atmos'), ('parasolRefl', 'cf3hr', 'atmos'), ('cltcalipso', 'cf3hr', 'atmos'), ('cllcalipso', 'cf3hr', 'atmos'), ('clmcalipso', 'cf3hr', 'atmos'), ('clhcalipso', 'cf3hr', 'atmos'), ('cltc', 'cf3hr', 'atmos'), ('zfull', 'cf3hr', 'atmos'), ('zhalf', 'cf3hr', 'atmos'), ('pfull', 'cf3hr', 'atmos'), ('phalf', 'cf3hr', 'atmos'), ('ta', 'cf3hr', 'atmos'), ('h2o', 'cf3hr', 'atmos'), ('clws', 'cf3hr', 'atmos'), ('clis', 'cf3hr', 'atmos'), ('clwc', 'cf3hr', 'atmos'), ('clic', 'cf3hr', 'atmos'), ('reffclws', 'cf3hr', 'atmos'), ('reffclis', 'cf3hr', 'atmos'), ('reffclwc', 'cf3hr', 'atmos'), ('reffclic', 'cf3hr', 'atmos'), ('grpllsprof', 'cf3hr', 'atmos'), ('prcprof', 'cf3hr', 'atmos'), ('prlsprof', 'cf3hr', 'atmos'), ('prsnc', 'cf3hr', 'atmos'), ('prlsns', 'cf3hr', 'atmos'), ('reffgrpls', 'cf3hr', 'atmos'), ('reffrainc', 'cf3hr', 'atmos'), ('reffrains', 'cf3hr', 'atmos'), ('reffsnowc', 'cf3hr', 'atmos'), ('reffsnows', 'cf3hr', 'atmos'), ('dtaus', 'cf3hr', 'atmos'), ('dtauc', 'cf3hr', 'atmos'), ('dems', 'cf3hr', 'atmos'), ('demc', 'cf3hr', 'atmos'), ('clc', 'cf3hr', 'atmos'), ('cls', 'cf3hr', 'atmos'), ('tas', 'cf3hr', 'atmos'), ('ts', 'cf3hr', 'atmos'), ('tasmin', 'cf3hr', 'atmos'), ('tasmax', 'cf3hr', 'atmos'), ('psl', 'cf3hr', 'atmos'), ('ps', 'cf3hr', 'atmos'), ('uas', 'cf3hr', 'atmos'), ('vas', 'cf3hr', 'atmos'), ('sfcWind', 'cf3hr', 'atmos'), ('hurs', 'cf3hr', 'atmos'), ('huss', 'cf3hr', 'atmos'), ('pr', 'cf3hr', 'atmos'), ('prsn', 'cf3hr', 'atmos'), ('prc', 'cf3hr', 'atmos'), ('evspsbl', 'cf3hr', 'atmos'), ('sbl', 'cf3hr', 'atmos'), ('tauu', 'cf3hr', 'atmos'), ('tauv', 'cf3hr', 'atmos'), ('hfls', 'cf3hr', 'atmos'), ('hfss', 'cf3hr', 'atmos'), ('rlds', 'cf3hr', 'atmos'), ('rlus', 'cf3hr', 'atmos'), ('rsds', 'cf3hr', 'atmos'), ('rsus', 'cf3hr', 'atmos'), ('rsdscs', 'cf3hr', 'atmos'), ('rsuscs', 'cf3hr', 'atmos'), ('rldscs', 'cf3hr', 'atmos'), ('rsdt', 'cf3hr', 'atmos'), ('rsut', 'cf3hr', 'atmos'), ('rlut', 'cf3hr', 'atmos'), ('rlutcs', 'cf3hr', 'atmos'), ('rsutcs', 'cf3hr', 'atmos'), ('prw', 'cf3hr', 'atmos'), ('clt', 'cf3hr', 'atmos'), ('clwvi', 'cf3hr', 'atmos'), ('clivi', 'cf3hr', 'atmos'), ('rtmt', 'cf3hr', 'atmos'), ('ccb', 'cf3hr', 'atmos'), ('cct', 'cf3hr', 'atmos'), ('ci', 'cf3hr', 'atmos'), ('sci', 'cf3hr', 'atmos'), ('fco2antt', 'cf3hr', 'atmos'), ('fco2fos', 'cf3hr', 'atmos'), ('fco2nat', 'cf3hr', 'atmos'), ('cl', 'cfSites', 'atmos'), ('clw', 'cfSites', 'atmos'), ('cli', 'cfSites', 'atmos'), ('mc', 'cfSites', 'atmos'), ('ta', 'cfSites', 'atmos'), ('ua', 'cfSites', 'atmos'), ('va', 'cfSites', 'atmos'), ('hus', 'cfSites', 'atmos'), ('hur', 'cfSites', 'atmos'), ('wap', 'cfSites', 'atmos'), ('zg', 'cfSites', 'atmos'), ('rlu', 'cfSites', 'atmos'), ('rsu', 'cfSites', 'atmos'), ('rld', 'cfSites', 'atmos'), ('rsd', 'cfSites', 'atmos'), ('rlucs', 'cfSites', 'atmos'), ('rsucs', 'cfSites', 'atmos'), ('rldcs', 'cfSites', 'atmos'), ('rsdcs', 'cfSites', 'atmos'), ('tnt', 'cfSites', 'atmos'), ('tnta', 'cfSites', 'atmos'), ('tntmp', 'cfSites', 'atmos'), ('tntscpbl', 'cfSites', 'atmos'), ('tntr', 'cfSites', 'atmos'), ('tntc', 'cfSites', 'atmos'), ('tnhus', 'cfSites', 'atmos'), ('tnhusa', 'cfSites', 'atmos'), ('tnhusc', 'cfSites', 'atmos'), ('tnhusd', 'cfSites', 'atmos'), ('tnhusscpbl', 'cfSites', 'atmos'), ('tnhusmp', 'cfSites', 'atmos'), ('evu', 'cfSites', 'atmos'), ('edt', 'cfSites', 'atmos'), ('pfull', 'cfSites', 'atmos'), ('phalf', 'cfSites', 'atmos'), ('tas', 'cfSites', 'atmos'), ('ts', 'cfSites', 'atmos'), ('psl', 'cfSites', 'atmos'), ('ps', 'cfSites', 'atmos'), ('uas', 'cfSites', 'atmos'), ('vas', 'cfSites', 'atmos'), ('sfcWind', 'cfSites', 'atmos'), ('hurs', 'cfSites', 'atmos'), ('huss', 'cfSites', 'atmos'), ('pr', 'cfSites', 'atmos'), ('prsn', 'cfSites', 'atmos'), ('prc', 'cfSites', 'atmos'), ('evspsbl', 'cfSites', 'atmos'), ('sbl', 'cfSites', 'atmos'), ('tauu', 'cfSites', 'atmos'), ('tauv', 'cfSites', 'atmos'), ('hfls', 'cfSites', 'atmos'), ('hfss', 'cfSites', 'atmos'), ('rlds', 'cfSites', 'atmos'), ('rlus', 'cfSites', 'atmos'), ('rsds', 'cfSites', 'atmos'), ('rsus', 'cfSites', 'atmos'), ('rsdscs', 'cfSites', 'atmos'), ('rsuscs', 'cfSites', 'atmos'), ('rldscs', 'cfSites', 'atmos'), ('rsdt', 'cfSites', 'atmos'), ('rsut', 'cfSites', 'atmos'), ('rlut', 'cfSites', 'atmos'), ('rlutcs', 'cfSites', 'atmos'), ('rsutcs', 'cfSites', 'atmos'), ('prw', 'cfSites', 'atmos'), ('clt', 'cfSites', 'atmos'), ('clwvi', 'cfSites', 'atmos'), ('clivi', 'cfSites', 'atmos'), ('rtmt', 'cfSites', 'atmos'), ('ccb', 'cfSites', 'atmos'), ('cct', 'cfSites', 'atmos'), ('ci', 'cfSites', 'atmos'), ('sci', 'cfSites', 'atmos'), ('fco2antt', 'cfSites', 'atmos'), ('fco2fos', 'cfSites', 'atmos'), ('fco2nat', 'cfSites', 'atmos')]
        return

    def NMME(self):
        self.file_types=['local_file', 'HTTPServer', 'GridFTP']
        self.remote_file_types=['HTTPServer','GridFTP','FTPServer','OPENDAP']
        self.required_file_types=['HTTPServer','OPENDAP']

        self.var_specs=['time_frequency','realm']

        self.official_drs=['institute',
                            'model',
                            'experiment',
                            'time_frequency',
                            'realm',
                            'ensemble',
                            'version',
                            'var']
        self.official_drs_no_version=copy.copy(self.official_drs)
        self.official_drs_no_version.remove('version')

        self.filename_drs=['var','time_frequency','model','experiment','ensemble']

        self.header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']
        self.base_drs=self.official_drs+[
                  'file_type',
                  'time',
                  'data_node',
                  'path'
                  ]
        self.simulations_desc=['institute','model','ensemble']
        self.catalogue_missing_simulations_desc=True

        self.slicing_args={
                      'institute': [str,'Modelling institute name'],
                      'model': [str,'Model name'],
                      'experiment': [str,'Experiment name'],
                      'var': [str,'Variable name, e.g. tas'],
                      'time_frequency': [str,'Frequency, e.g. day'],
                      'realm': [str,'Realm, e.g. atmos'],
                      'ensemble': [str,'RIP identifier, e.g. r1i1p1']
                      }
                      #'file_type': [str,'File type: '+','.join(self.file_types)]
        self.discover_exclude_args=['experiment','var','time_frequency','realm','year','month','file_type']

        self.remote_fields=['institute','model','ensemble','file_type','version']

        self.known_fields=['experiment','var'] + self.var_specs

        return

    def LRFTIP(self):
        self.file_types=['local_file', 'FTPServer']
        self.remote_file_types=['FTPServer']

        self.var_specs=['time_frequency','realm']

        self.official_drs=[
                            'model',
                            'experiment',
                            'time_frequency',
                            'realm',
                            'var',
                            'ensemble'
                            ]
                            #'version'
        #self.official_drs_no_version=copy.copy(self.official_drs)
        #self.official_drs_no_version.remove('version')

        self.filename_drs=['var','cmor_table','model','experiment','ensemble']

        self.header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']
        self.base_drs=self.official_drs+[
                  'file_type',
                  'time',
                  'data_node',
                  'path'
                  ]
        self.simulations_desc=['model','ensemble']
        self.catalogue_missing_simulations_desc=True

        self.slicing_args={
                      'model': [str,'Model name'],
                      'experiment': [str,'Experiment name'],
                      'var': [str,'Variable name, e.g. tas'],
                      'time_frequency': [str,'Frequency, e.g. day'],
                      'realm': [str,'Realm, e.g. atmos'],
                      'ensemble': [str,'RIP identifier, e.g. r1i1p1']
                      }
                      #'file_type': [str,'File type: '+','.join(self.file_types)]
        self.discover_exclude_args=['experiment','var','time_frequency','realm','year','month','file_type']

        self.remote_fields=['model','ensemble','file_type']

        self.known_fields=['experiment','var'] + self.var_specs

        return

    def CanSISE(self):
        self.file_types=['HTTPServer','local_file']
        self.remote_file_types=['HTTPServer']
        self.required_file_types=['HTTPServer']

        self.var_specs=['time_frequency','realm']

        self.official_drs=['institute',
                            'model',
                            'experiment',
                            'time_frequency',
                            'realm',
                            'var',
                            'ensemble'
                            ]
        self.official_drs_no_version=copy.copy(self.official_drs)

        self.filename_drs=['var','cmor_table','model','experiment','ensemble']

        self.header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']
        self.base_drs=self.official_drs+[
                  'file_type',
                  'time',
                  'data_node',
                  'version',
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
                      #'cmor_table': [str,'CMOR table name, e.g. day'],
                      'ensemble': [str,'RIP identifier, e.g. r1i1p1']
                      }
                      #'file_type': [str,'File type: '+','.join(self.file_types)]
        #self.discover_exclude_args=['experiment','var','time_frequency','realm','cmor_table','year','month','file_type']

        self.remote_fields=['institute','model','ensemble','file_type']

        self.known_fields=['experiment','var'] + self.var_specs

        self.need_instant_time = [('tas', '3hr', 'atmos'), ('uas', '3hr', 'atmos'), ('vas', '3hr', 'atmos'), ('huss', '3hr', 'atmos'), ('mrsos', '3hr', 'land'), ('tslsi', '3hr', 'land'), ('tso', '3hr', 'ocean'), ('ps', '3hr', 'atmos'), ('ua', '6hrPlev', 'atmos'), ('va', '6hrPlev', 'atmos'), ('ta', '6hrPlev', 'atmos'), ('psl', '6hrPlev', 'atmos'), ('ta', '6hrLev', 'atmos'), ('ua', '6hrLev', 'atmos'), ('va', '6hrLev', 'atmos'), ('hus', '6hrLev', 'atmos'), ('ps', '6hrLev', 'atmos'), ('clcalipso', 'cf3hr', 'atmos'), ('clcalipso2', 'cf3hr', 'atmos'), ('cfadDbze94', 'cf3hr', 'atmos'), ('cfadLidarsr532', 'cf3hr', 'atmos'), ('parasolRefl', 'cf3hr', 'atmos'), ('cltcalipso', 'cf3hr', 'atmos'), ('cllcalipso', 'cf3hr', 'atmos'), ('clmcalipso', 'cf3hr', 'atmos'), ('clhcalipso', 'cf3hr', 'atmos'), ('cltc', 'cf3hr', 'atmos'), ('zfull', 'cf3hr', 'atmos'), ('zhalf', 'cf3hr', 'atmos'), ('pfull', 'cf3hr', 'atmos'), ('phalf', 'cf3hr', 'atmos'), ('ta', 'cf3hr', 'atmos'), ('h2o', 'cf3hr', 'atmos'), ('clws', 'cf3hr', 'atmos'), ('clis', 'cf3hr', 'atmos'), ('clwc', 'cf3hr', 'atmos'), ('clic', 'cf3hr', 'atmos'), ('reffclws', 'cf3hr', 'atmos'), ('reffclis', 'cf3hr', 'atmos'), ('reffclwc', 'cf3hr', 'atmos'), ('reffclic', 'cf3hr', 'atmos'), ('grpllsprof', 'cf3hr', 'atmos'), ('prcprof', 'cf3hr', 'atmos'), ('prlsprof', 'cf3hr', 'atmos'), ('prsnc', 'cf3hr', 'atmos'), ('prlsns', 'cf3hr', 'atmos'), ('reffgrpls', 'cf3hr', 'atmos'), ('reffrainc', 'cf3hr', 'atmos'), ('reffrains', 'cf3hr', 'atmos'), ('reffsnowc', 'cf3hr', 'atmos'), ('reffsnows', 'cf3hr', 'atmos'), ('dtaus', 'cf3hr', 'atmos'), ('dtauc', 'cf3hr', 'atmos'), ('dems', 'cf3hr', 'atmos'), ('demc', 'cf3hr', 'atmos'), ('clc', 'cf3hr', 'atmos'), ('cls', 'cf3hr', 'atmos'), ('tas', 'cf3hr', 'atmos'), ('ts', 'cf3hr', 'atmos'), ('tasmin', 'cf3hr', 'atmos'), ('tasmax', 'cf3hr', 'atmos'), ('psl', 'cf3hr', 'atmos'), ('ps', 'cf3hr', 'atmos'), ('uas', 'cf3hr', 'atmos'), ('vas', 'cf3hr', 'atmos'), ('sfcWind', 'cf3hr', 'atmos'), ('hurs', 'cf3hr', 'atmos'), ('huss', 'cf3hr', 'atmos'), ('pr', 'cf3hr', 'atmos'), ('prsn', 'cf3hr', 'atmos'), ('prc', 'cf3hr', 'atmos'), ('evspsbl', 'cf3hr', 'atmos'), ('sbl', 'cf3hr', 'atmos'), ('tauu', 'cf3hr', 'atmos'), ('tauv', 'cf3hr', 'atmos'), ('hfls', 'cf3hr', 'atmos'), ('hfss', 'cf3hr', 'atmos'), ('rlds', 'cf3hr', 'atmos'), ('rlus', 'cf3hr', 'atmos'), ('rsds', 'cf3hr', 'atmos'), ('rsus', 'cf3hr', 'atmos'), ('rsdscs', 'cf3hr', 'atmos'), ('rsuscs', 'cf3hr', 'atmos'), ('rldscs', 'cf3hr', 'atmos'), ('rsdt', 'cf3hr', 'atmos'), ('rsut', 'cf3hr', 'atmos'), ('rlut', 'cf3hr', 'atmos'), ('rlutcs', 'cf3hr', 'atmos'), ('rsutcs', 'cf3hr', 'atmos'), ('prw', 'cf3hr', 'atmos'), ('clt', 'cf3hr', 'atmos'), ('clwvi', 'cf3hr', 'atmos'), ('clivi', 'cf3hr', 'atmos'), ('rtmt', 'cf3hr', 'atmos'), ('ccb', 'cf3hr', 'atmos'), ('cct', 'cf3hr', 'atmos'), ('ci', 'cf3hr', 'atmos'), ('sci', 'cf3hr', 'atmos'), ('fco2antt', 'cf3hr', 'atmos'), ('fco2fos', 'cf3hr', 'atmos'), ('fco2nat', 'cf3hr', 'atmos'), ('cl', 'cfSites', 'atmos'), ('clw', 'cfSites', 'atmos'), ('cli', 'cfSites', 'atmos'), ('mc', 'cfSites', 'atmos'), ('ta', 'cfSites', 'atmos'), ('ua', 'cfSites', 'atmos'), ('va', 'cfSites', 'atmos'), ('hus', 'cfSites', 'atmos'), ('hur', 'cfSites', 'atmos'), ('wap', 'cfSites', 'atmos'), ('zg', 'cfSites', 'atmos'), ('rlu', 'cfSites', 'atmos'), ('rsu', 'cfSites', 'atmos'), ('rld', 'cfSites', 'atmos'), ('rsd', 'cfSites', 'atmos'), ('rlucs', 'cfSites', 'atmos'), ('rsucs', 'cfSites', 'atmos'), ('rldcs', 'cfSites', 'atmos'), ('rsdcs', 'cfSites', 'atmos'), ('tnt', 'cfSites', 'atmos'), ('tnta', 'cfSites', 'atmos'), ('tntmp', 'cfSites', 'atmos'), ('tntscpbl', 'cfSites', 'atmos'), ('tntr', 'cfSites', 'atmos'), ('tntc', 'cfSites', 'atmos'), ('tnhus', 'cfSites', 'atmos'), ('tnhusa', 'cfSites', 'atmos'), ('tnhusc', 'cfSites', 'atmos'), ('tnhusd', 'cfSites', 'atmos'), ('tnhusscpbl', 'cfSites', 'atmos'), ('tnhusmp', 'cfSites', 'atmos'), ('evu', 'cfSites', 'atmos'), ('edt', 'cfSites', 'atmos'), ('pfull', 'cfSites', 'atmos'), ('phalf', 'cfSites', 'atmos'), ('tas', 'cfSites', 'atmos'), ('ts', 'cfSites', 'atmos'), ('psl', 'cfSites', 'atmos'), ('ps', 'cfSites', 'atmos'), ('uas', 'cfSites', 'atmos'), ('vas', 'cfSites', 'atmos'), ('sfcWind', 'cfSites', 'atmos'), ('hurs', 'cfSites', 'atmos'), ('huss', 'cfSites', 'atmos'), ('pr', 'cfSites', 'atmos'), ('prsn', 'cfSites', 'atmos'), ('prc', 'cfSites', 'atmos'), ('evspsbl', 'cfSites', 'atmos'), ('sbl', 'cfSites', 'atmos'), ('tauu', 'cfSites', 'atmos'), ('tauv', 'cfSites', 'atmos'), ('hfls', 'cfSites', 'atmos'), ('hfss', 'cfSites', 'atmos'), ('rlds', 'cfSites', 'atmos'), ('rlus', 'cfSites', 'atmos'), ('rsds', 'cfSites', 'atmos'), ('rsus', 'cfSites', 'atmos'), ('rsdscs', 'cfSites', 'atmos'), ('rsuscs', 'cfSites', 'atmos'), ('rldscs', 'cfSites', 'atmos'), ('rsdt', 'cfSites', 'atmos'), ('rsut', 'cfSites', 'atmos'), ('rlut', 'cfSites', 'atmos'), ('rlutcs', 'cfSites', 'atmos'), ('rsutcs', 'cfSites', 'atmos'), ('prw', 'cfSites', 'atmos'), ('clt', 'cfSites', 'atmos'), ('clwvi', 'cfSites', 'atmos'), ('clivi', 'cfSites', 'atmos'), ('rtmt', 'cfSites', 'atmos'), ('ccb', 'cfSites', 'atmos'), ('cct', 'cfSites', 'atmos'), ('ci', 'cfSites', 'atmos'), ('sci', 'cfSites', 'atmos'), ('fco2antt', 'cfSites', 'atmos'), ('fco2fos', 'cfSites', 'atmos'), ('fco2nat', 'cfSites', 'atmos')]
        return

