import copy

class DRS:
    project = __name__.split('.')[-1]

    file_types=['HTTPServer','local_file']
    remote_file_types=['HTTPServer']
    required_file_types=['HTTPServer']

    var_specs=['time_frequency','realm']

    official_drs=['institute',
                        'model',
                        'experiment',
                        'time_frequency',
                        'realm',
                        'var',
                        'ensemble'
                        ]
    official_drs_no_version=copy.copy(official_drs)

    filename_drs=['var','cmor_table','model','experiment','ensemble']

    header_desc=['search_list','file_type_list','month_list','data_node_list','experiment_list','variable_list']
    base_drs=official_drs+[
              'file_type',
              'time',
              'data_node',
              'version',
              'path'
              ]
    simulations_desc=['institute','model','ensemble']

    slicing_args={
                  'institute': [str,'Modelling institute name'],
                  'model': [str,'Model name'],
                  'experiment': [str,'Experiment name'],
                  'var': [str,'Variable name, e.g. tas'],
                  'time_frequency': [str,'Frequency, e.g. day'],
                  'realm': [str,'Realm, e.g. atmos'],
                  #'cmor_table': [str,'CMOR table name, e.g. day'],
                  'ensemble': [str,'RIP identifier, e.g. r1i1p1']
                  }
                  #'file_type': [str,'File type: '+','.join(file_types)]
    #discover_exclude_args=['experiment','var','time_frequency','realm','cmor_table','year','month','file_type']

    remote_fields=['institute','model','ensemble','file_type']

    known_fields=['experiment','var'] + var_specs

    need_instant_time = [('tas', '3hr', 'atmos'), ('uas', '3hr', 'atmos'), ('vas', '3hr', 'atmos'), ('huss', '3hr', 'atmos'), ('mrsos', '3hr', 'land'), ('tslsi', '3hr', 'land'), ('tso', '3hr', 'ocean'), ('ps', '3hr', 'atmos'), ('ua', '6hrPlev', 'atmos'), ('va', '6hrPlev', 'atmos'), ('ta', '6hrPlev', 'atmos'), ('psl', '6hrPlev', 'atmos'), ('ta', '6hrLev', 'atmos'), ('ua', '6hrLev', 'atmos'), ('va', '6hrLev', 'atmos'), ('hus', '6hrLev', 'atmos'), ('ps', '6hrLev', 'atmos'), ('clcalipso', 'cf3hr', 'atmos'), ('clcalipso2', 'cf3hr', 'atmos'), ('cfadDbze94', 'cf3hr', 'atmos'), ('cfadLidarsr532', 'cf3hr', 'atmos'), ('parasolRefl', 'cf3hr', 'atmos'), ('cltcalipso', 'cf3hr', 'atmos'), ('cllcalipso', 'cf3hr', 'atmos'), ('clmcalipso', 'cf3hr', 'atmos'), ('clhcalipso', 'cf3hr', 'atmos'), ('cltc', 'cf3hr', 'atmos'), ('zfull', 'cf3hr', 'atmos'), ('zhalf', 'cf3hr', 'atmos'), ('pfull', 'cf3hr', 'atmos'), ('phalf', 'cf3hr', 'atmos'), ('ta', 'cf3hr', 'atmos'), ('h2o', 'cf3hr', 'atmos'), ('clws', 'cf3hr', 'atmos'), ('clis', 'cf3hr', 'atmos'), ('clwc', 'cf3hr', 'atmos'), ('clic', 'cf3hr', 'atmos'), ('reffclws', 'cf3hr', 'atmos'), ('reffclis', 'cf3hr', 'atmos'), ('reffclwc', 'cf3hr', 'atmos'), ('reffclic', 'cf3hr', 'atmos'), ('grpllsprof', 'cf3hr', 'atmos'), ('prcprof', 'cf3hr', 'atmos'), ('prlsprof', 'cf3hr', 'atmos'), ('prsnc', 'cf3hr', 'atmos'), ('prlsns', 'cf3hr', 'atmos'), ('reffgrpls', 'cf3hr', 'atmos'), ('reffrainc', 'cf3hr', 'atmos'), ('reffrains', 'cf3hr', 'atmos'), ('reffsnowc', 'cf3hr', 'atmos'), ('reffsnows', 'cf3hr', 'atmos'), ('dtaus', 'cf3hr', 'atmos'), ('dtauc', 'cf3hr', 'atmos'), ('dems', 'cf3hr', 'atmos'), ('demc', 'cf3hr', 'atmos'), ('clc', 'cf3hr', 'atmos'), ('cls', 'cf3hr', 'atmos'), ('tas', 'cf3hr', 'atmos'), ('ts', 'cf3hr', 'atmos'), ('tasmin', 'cf3hr', 'atmos'), ('tasmax', 'cf3hr', 'atmos'), ('psl', 'cf3hr', 'atmos'), ('ps', 'cf3hr', 'atmos'), ('uas', 'cf3hr', 'atmos'), ('vas', 'cf3hr', 'atmos'), ('sfcWind', 'cf3hr', 'atmos'), ('hurs', 'cf3hr', 'atmos'), ('huss', 'cf3hr', 'atmos'), ('pr', 'cf3hr', 'atmos'), ('prsn', 'cf3hr', 'atmos'), ('prc', 'cf3hr', 'atmos'), ('evspsbl', 'cf3hr', 'atmos'), ('sbl', 'cf3hr', 'atmos'), ('tauu', 'cf3hr', 'atmos'), ('tauv', 'cf3hr', 'atmos'), ('hfls', 'cf3hr', 'atmos'), ('hfss', 'cf3hr', 'atmos'), ('rlds', 'cf3hr', 'atmos'), ('rlus', 'cf3hr', 'atmos'), ('rsds', 'cf3hr', 'atmos'), ('rsus', 'cf3hr', 'atmos'), ('rsdscs', 'cf3hr', 'atmos'), ('rsuscs', 'cf3hr', 'atmos'), ('rldscs', 'cf3hr', 'atmos'), ('rsdt', 'cf3hr', 'atmos'), ('rsut', 'cf3hr', 'atmos'), ('rlut', 'cf3hr', 'atmos'), ('rlutcs', 'cf3hr', 'atmos'), ('rsutcs', 'cf3hr', 'atmos'), ('prw', 'cf3hr', 'atmos'), ('clt', 'cf3hr', 'atmos'), ('clwvi', 'cf3hr', 'atmos'), ('clivi', 'cf3hr', 'atmos'), ('rtmt', 'cf3hr', 'atmos'), ('ccb', 'cf3hr', 'atmos'), ('cct', 'cf3hr', 'atmos'), ('ci', 'cf3hr', 'atmos'), ('sci', 'cf3hr', 'atmos'), ('fco2antt', 'cf3hr', 'atmos'), ('fco2fos', 'cf3hr', 'atmos'), ('fco2nat', 'cf3hr', 'atmos'), ('cl', 'cfSites', 'atmos'), ('clw', 'cfSites', 'atmos'), ('cli', 'cfSites', 'atmos'), ('mc', 'cfSites', 'atmos'), ('ta', 'cfSites', 'atmos'), ('ua', 'cfSites', 'atmos'), ('va', 'cfSites', 'atmos'), ('hus', 'cfSites', 'atmos'), ('hur', 'cfSites', 'atmos'), ('wap', 'cfSites', 'atmos'), ('zg', 'cfSites', 'atmos'), ('rlu', 'cfSites', 'atmos'), ('rsu', 'cfSites', 'atmos'), ('rld', 'cfSites', 'atmos'), ('rsd', 'cfSites', 'atmos'), ('rlucs', 'cfSites', 'atmos'), ('rsucs', 'cfSites', 'atmos'), ('rldcs', 'cfSites', 'atmos'), ('rsdcs', 'cfSites', 'atmos'), ('tnt', 'cfSites', 'atmos'), ('tnta', 'cfSites', 'atmos'), ('tntmp', 'cfSites', 'atmos'), ('tntscpbl', 'cfSites', 'atmos'), ('tntr', 'cfSites', 'atmos'), ('tntc', 'cfSites', 'atmos'), ('tnhus', 'cfSites', 'atmos'), ('tnhusa', 'cfSites', 'atmos'), ('tnhusc', 'cfSites', 'atmos'), ('tnhusd', 'cfSites', 'atmos'), ('tnhusscpbl', 'cfSites', 'atmos'), ('tnhusmp', 'cfSites', 'atmos'), ('evu', 'cfSites', 'atmos'), ('edt', 'cfSites', 'atmos'), ('pfull', 'cfSites', 'atmos'), ('phalf', 'cfSites', 'atmos'), ('tas', 'cfSites', 'atmos'), ('ts', 'cfSites', 'atmos'), ('psl', 'cfSites', 'atmos'), ('ps', 'cfSites', 'atmos'), ('uas', 'cfSites', 'atmos'), ('vas', 'cfSites', 'atmos'), ('sfcWind', 'cfSites', 'atmos'), ('hurs', 'cfSites', 'atmos'), ('huss', 'cfSites', 'atmos'), ('pr', 'cfSites', 'atmos'), ('prsn', 'cfSites', 'atmos'), ('prc', 'cfSites', 'atmos'), ('evspsbl', 'cfSites', 'atmos'), ('sbl', 'cfSites', 'atmos'), ('tauu', 'cfSites', 'atmos'), ('tauv', 'cfSites', 'atmos'), ('hfls', 'cfSites', 'atmos'), ('hfss', 'cfSites', 'atmos'), ('rlds', 'cfSites', 'atmos'), ('rlus', 'cfSites', 'atmos'), ('rsds', 'cfSites', 'atmos'), ('rsus', 'cfSites', 'atmos'), ('rsdscs', 'cfSites', 'atmos'), ('rsuscs', 'cfSites', 'atmos'), ('rldscs', 'cfSites', 'atmos'), ('rsdt', 'cfSites', 'atmos'), ('rsut', 'cfSites', 'atmos'), ('rlut', 'cfSites', 'atmos'), ('rlutcs', 'cfSites', 'atmos'), ('rsutcs', 'cfSites', 'atmos'), ('prw', 'cfSites', 'atmos'), ('clt', 'cfSites', 'atmos'), ('clwvi', 'cfSites', 'atmos'), ('clivi', 'cfSites', 'atmos'), ('rtmt', 'cfSites', 'atmos'), ('ccb', 'cfSites', 'atmos'), ('cct', 'cfSites', 'atmos'), ('ci', 'cfSites', 'atmos'), ('sci', 'cfSites', 'atmos'), ('fco2antt', 'cfSites', 'atmos'), ('fco2fos', 'cfSites', 'atmos'), ('fco2nat', 'cfSites', 'atmos')]
