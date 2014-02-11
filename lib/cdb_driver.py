import sys
import os
import shutil
import argparse 
import textwrap
import json
import json_tools
import cdb_query_archive
import cdb_query_archive_class

class Open_With_Indent:
    """This class creates an open file were indentation is tracked"""

    def __init__(self,file_name,read_method):
        self.open = open(file_name,read_method)
        self._current_indent=0
    
    def writei(self,write_string):
        self.open.write(write_string.rjust(len(write_string)+self._current_indent))

    def inc_indent(self):
        self._current_indent+=3

    def dec_indent(self):
        if self._current_indent > 3:
            self._current_indent-=3
        else:
            self._current_indent=0

class Experiment_Setup:
    """ Sets an experiment for the CMIP5 archive """

    def __init__(self,options):
        for opt in dir(options):
            if opt[0] != '_' and opt not in ['ensure_value','read_file','read_module']:
                setattr(self,opt,getattr(options,opt))
        if 'years' in dir(self):
            self.years=self.years.split(',')

        desc_list=[self.diagnostic,self.center,self.model,self.rip,self.experiment,'-'.join(self.years)]
        if self.month: desc_list.append(str(self.month))
        self.description='_'.join(desc_list)
        self.runscript_file=self.runscripts_dir+'/'+self.description

    def prepare_scripts(self):
        """ Prepares the scripts for bash launch """
    
        out=Open_With_Indent(self.runscript_file,'w')

        out.writei('#!/bin/bash\n')
        if self.pbs_expt:
            #If the script is expected to be submitted to a PBS queue, output the required headers:
	    if self.queue != None: out.writei('#PBS -q {0}\n'.format(self.queue))
            out.writei('#PBS -l nodes=1:ppn={0},walltime={1}\n'.format(max(self.m_async,self.ppn),self.walltime))
            #out.writei('#PBS -l nodes=1:ppn={0}\n'.format(self.m_async))
            out.writei('#PBS -N {0}\n'.format(self.description))
            out.writei('#PBS -o {1}/pbs_out/{0}\n'.format(self.description,self.output_dir))
            out.writei('#PBS -e {1}/pbs_err/{0}\n'.format(self.description,self.output_dir))

        #Put the header to the diagnostic:
        out.writei('\n')
        out.writei('#The next variable should be empty. If your script completed and some\n')
        out.writei('#years did not process properly (usually due to a timeout on large\n')
        out.writei('#storage systems), you can list the years (space separated)\n')
        out.writei('#to recompute these years. Simply rerun or resubmit the scripts\n')
        out.writei('CDB_YEARS_FIX_LIST=""\n'.format(self.years[0],self.years[1]))
        out.writei('\n')
        out.writei('CDB_YEARS="{0},{1}"\n'.format(self.years[0],self.years[1]))
        out.writei('CDB_YEAR_START=$(echo $CDB_YEARS | awk -F\',\' \'{print $1}\')\n')
        out.writei('CDB_YEAR_END=$(echo $CDB_YEARS | awk -F\',\' \'{print $2}\')\n')
        out.writei('export CDB_MODEL="{0}"\n'.format(self.model))
        out.writei('export CDB_CENTER="{0}"\n'.format(self.center))
        out.writei('export CDB_RUN_ID="{0}"\n'.format(self.rip))
        out.writei('export CDB_EXPT="{0}"\n'.format(self.experiment))
        out.writei('export CDB_DIAG_NAME="{0}"\n'.format(self.diagnostic))
        out.writei('export CDB_DIAG_HEADER="{0}"\n'.format(os.path.abspath(self.in_diagnostic_headers_file)))
        out.writei('export CDB_OUT_FILE=${CDB_DIAG_NAME}_${CDB_MODEL}_${CDB_EXPT}_$(printf "%04d" $CDB_YEAR_START)_$(printf "%04d" $CDB_YEAR_END)_${CDB_RUN_ID}\n')
        out.writei('\n')
        out.writei('#SET THE OUTPUT DIRECTORY\n')
        out.writei('export CDB_OUT_DIR="{0}"\n'.format(self.output_dir))
        out.writei('mkdir -p ${CDB_OUT_DIR}\n')
        out.writei('\n')
        out.writei('#SET THE TEMP DIRECTORY\n')
        out.writei('CDB_ROOT_TEMP_DIR="{0}"\n'.format(self.temp_dir))
        out.writei('if [ -z "${PBS_JOBID}" ]; then\n')
        out.inc_indent()
        out.writei('export CDB_TEMP_DIR="${CDB_ROOT_TEMP_DIR}/${CDB_DIAG_NAME}.job_pid${$}"\n')
        out.dec_indent()
        out.writei('else\n')
        out.inc_indent()
        out.writei('export CDB_TEMP_DIR="${CDB_ROOT_TEMP_DIR}/${CDB_DIAG_NAME}.pbs_job_${PBS_JOBID}"\n')
        out.dec_indent()
        out.writei('fi\n')
        out.writei('mkdir -p ${CDB_TEMP_DIR}\n')
        out.writei('\n')
        out.writei('#Subset the data pointers file and put it in temporary directory for optimal retrieval:\n')
        out.writei('CDB_DIAG_HEADER_BASE=$(basename ${CDB_DIAG_HEADER%.gz})\n')
        out.writei('cp ${CDB_DIAG_HEADER%.gz}.nc ${CDB_TEMP_DIR}/${CDB_DIAG_HEADER_BASE}.nc\n')
        out.writei('cdb_query_archive slice --experiment=${CDB_EXPT} --model=${CDB_MODEL} --center=${CDB_CENTER} ${CDB_DIAG_HEADER} ${CDB_TEMP_DIR}/${CDB_DIAG_HEADER_BASE}\n')
        out.writei('CDB_DIAG_HEADER="${CDB_TEMP_DIR}/${CDB_DIAG_HEADER_BASE}"\n')
        out.writei('\n')
        out.writei('#This variable sets the number of timesteps to add after and before a month:\n')
        out.writei('export CDB_OFFSET=0\n')
        out.writei('\n')
        out.writei('#Get the time axis for all the variables:\n')
        for var_name in self.variable_list.keys():
            description_list=['${CDB_CENTER}','${CDB_MODEL}','${CDB_EXPT}']
            description_list.extend(self.variable_list[var_name])
            if self.variable_list[var_name][0]!='fx':
                description_list.append('${CDB_RUN_ID}')
                description_list.append(var_name)
                out_time_file='${CDB_DIAG_HEADER}.'+var_name+'_'+'_'.join(self.variable_list[var_name])+'.time.nc'
                out.writei('ncks -3 -G : -g '+'/'.join(description_list)+' -v time ${CDB_DIAG_HEADER}.nc '+out_time_file+'\n')
                out.writei('ncap2 -O -s \'empty[$time]=0.0;\' '+' '.join([out_time_file,out_time_file])+'\n')
        out.writei('if [ "$CDB_YEAR_START" -lt "10" ]; then\n')
        out.inc_indent()
        out_time_file_list=[ '${CDB_DIAG_HEADER}.'+var_name+'_'+'_'.join(self.variable_list[var_name])+'.time.nc' for 
                                var_name in self.variable_list.keys() if self.variable_list[var_name][0]!='fx' ]
        out.writei('CDB_FILE_TIME_LIST="'+' '.join(out_time_file_list)+'"\n')
        out.writei('NTIME_MIN=$(echo $(for FILE in ${CDB_FILE_TIME_LIST}; do cdo -s showyear $FILE; done| sort)| awk -F\' \' \'{print $1}\')\n') 
        out.writei('CDB_YEAR_START=$((CDB_YEAR_START+NTIME_MIN))\n')
        out.writei('CDB_YEAR_END=$((CDB_YEAR_END+NTIME_MIN))\n')
        out.dec_indent()
        out.writei('fi\n')
        out.writei('#Select the requested years:\n')
        for var_name in self.variable_list.keys():
            if self.variable_list[var_name][0]!='fx':
                out_time_file='${CDB_DIAG_HEADER}.'+var_name+'_'+'_'.join(self.variable_list[var_name])+'.time.nc'
                out.writei('cdo -selyear,$(seq -s \',\' ${CDB_YEAR_START} ${CDB_YEAR_END}) '+' '.join([out_time_file,out_time_file+'.tmp'])+'\n')
                out.writei('mv '+' '.join([out_time_file+'.tmp',out_time_file])+'\n')

        #Load script file:
        script_file=open(self.diagnostic_dir+'/'+self.diagnostic+'.sh','r')

        #Define instructions:
        instructions={
                      '#!START MONTH LOOP' : start_monthly_loop,
                      '#!END MONTH LOOP' : end_monthly_loop
                    }

        #Loop through lines:
        for line in script_file:
                out.writei(line)
                for inst in instructions.keys():
                    if line.lstrip().upper()[:len(inst)]==inst:
                        self=instructions[inst](self,out,line)

        out.writei('\n')
        out.writei('rm -rf ${CDB_TEMP_DIR}\n')
        out.writei('\n')

        #Finally add BASH code at the end of the script to structure the output like the CMIP5 DRS:
        structure_out_with_cmip5_drs(self,out)
        out.open.close()

def ramdisk_protection(self,out):
    out.writei('function cleanup_ramdisk {\n')
    out.inc_indent()
    out.writei('echo -n "Cleaning up ramdisk directory {0} on "\n'.format(self.temp_dir))
    out.writei('date\n')
    out.writei('rm -rf {0}\n'.format(self.temp_dir))
    out.writei('echo -n "done at "\n')
    out.writei('date\n')
    out.dec_indent()
    out.writei('}')

    out.writei('function trap_term {\n')
    out.inc_indent()
    out.writei('echo -n "Trapped term (soft kill) signal on "\n')
    out.writei('date\n')
    out.writei('cleanup_ramdisk\n')
    out.writei('exit\n')
    out.dec_indent()
    out.writei('}\n')

    out.writei('#trap the termination signal, and call the function trap_term when\n')
    out.writei('# that happens, so results may be saved.\n')
    out.writei('trap "trap_term" TERM\n')
    return self

def structure_out_with_cmip5_drs(self,out):
    #code to structure the output with the CMIP5 DRS:
    out.writei('#THE LAST PART OF THIS SCRIPT REORGANIZES THE OUTPUT TO CONFORM WITH THE CMIP5 DRS:\n')
    out.writei('if [ ! -z "$CDB_CMIP5_COMP_LIST" ]; then\n')
    out.inc_indent()
    out.writei('for FILE_DESC in $CDB_CMIP5_COMP_LIST; do\n')
    out.inc_indent()
    out.writei('FILE=$(echo $FILE_DESC | awk -F\';\' \'{print $1}\')\n')
    out.writei('REALM_FREQ_INFO=$(echo $FILE_DESC | awk -F\';\' \'{print $2}\')\n')
    out.writei('CDB_INFO="This output was created using the Climate Diagnostics Benchmark toolbox,\n\
                developed by F. Laliberte and P. J. Kushner, 2012, at the University of Toronto.\n\
                This code was developed as part of the ExArch project."\n')
    out.writei('\n')
    out.writei('CDB_VAR_LIST=$(cdo showname ${FILE})\n')
    out.writei('\n')
    out.writei('CDB_FREQ=`echo ${REALM_FREQ_INFO} | tr \',\' \' \' | awk -F\' \' \'{ print $1}\'`\n')
    out.writei('CDB_REALM=`echo ${REALM_FREQ_INFO} | tr \',\' \' \' | awk -F\' \' \'{ print $2}\'`\n')
    out.writei('CDB_MIP=`echo ${REALM_FREQ_INFO} | tr \',\' \' \' | awk -F\' \' \'{ print $3}\'`\n')
    out.writei('\n')
    out.writei('for CDB_VAR_NAME in $CDB_VAR_LIST; do\n')
    out.inc_indent()
    out.writei('CDB_LOCAL_RUN_ID={0}\n'.format(self.rip))
    out.writei('if [ "$CDB_FREQ" == "fx" ]; then\n')
    out.inc_indent()
    out.writei('if [ "$CDB_MIP" == "fx" ]; then\n')
    out.inc_indent()
    out.writei('CDB_LOCAL_RUN_ID="r0i0p0"\n')
    out.dec_indent()
    out.writei('fi\n')
    out.dec_indent()
    out.writei('fi\n')
    out.writei('BASE_PATH="$CDB_OUT_DIR/in/{0}/{1}/{2}/$CDB_FREQ/$CDB_REALM/$CDB_MIP/$CDB_LOCAL_RUN_ID/v$(date +%F | tr -d \'-\')/$CDB_VAR_NAME"\n'.format(self.center,self.model,self.experiment))
    out.writei('mkdir -p $BASE_PATH\n')
    out.writei('FILE_NAME="$BASE_PATH/${CDB_VAR_NAME}_${CDB_MIP}_'+self.model+'_'+self.experiment+'_${CDB_LOCAL_RUN_ID}"\n')
    out.writei('case $CDB_FREQ in\n')
    out.inc_indent()
    out.writei('fx) FILE_NAME="${FILE_NAME}";;\n')
    out.writei('clim) FILE_NAME="${FILE_NAME}";;\n')
    out.writei('3hr) FILE_NAME="${FILE_NAME}_${CDB_YEAR_START}01010000-${CDB_YEAR_END}12312100";;\n')
    out.writei('6hr) FILE_NAME="${FILE_NAME}_${CDB_YEAR_START}01010000-${CDB_YEAR_END}12311800";;\n')
    out.writei('day) FILE_NAME="${FILE_NAME}_${CDB_YEAR_START}0101-${CDB_YEAR_END}1231";;\n')
    out.writei('mon) FILE_NAME="${FILE_NAME}_${CDB_YEAR_START}01-${CDB_YEAR_END}12";;\n')
    out.writei('yr) FILE_NAME="${FILE_NAME}_${CDB_YEAR_START}-${CDB_YEAR_END}";;\n')
    out.writei('*) echo "Unrecognized output frequency $CDB_FREQ";;\n')
    out.dec_indent()
    out.writei('esac\n')
    out.writei('cdo -s -f nc4 -z zip -selname,${CDB_VAR_NAME} $FILE ${FILE_NAME}.nc\n')
    #out.writei('echo $CDB_INFO > ${FILE_NAME}.info\n')
    #out.writei('cdo -s -f nc4 -z zip -setgatt,CDB_info,${FILE_NAME}.info -selname,${CDB_VAR_NAME} $FILE ${FILE_NAME}.nc\n')
    #out.writei('rm ${FILE_NAME}.info\n')
    out.dec_indent()
    out.writei('done\n')
    out.writei('rm $FILE\n')
    out.dec_indent()
    out.writei('done\n')
    out.dec_indent()
    out.writei('fi\n')

def start_monthly_loop(self,out,line):
    #This function creates a BASH function within the script. The function processes one month.
    #Because of the monthly processing this can be parallelized.
    out.writei('function monthly_processing()\n')
    out.writei('{\n')
    out.writei('cat > $3 <<EndOfScriptHeader\n')
    out.writei('#!/bin/bash\n')
    out.writei('#This file launches a monthly diagnostic\n')
    out.writei('export CDB_YEAR=$1\n')
    out.writei('export CDB_MONTH=$2\n')
    out.writei('\n')
    out.writei('export CDB_YEAR_START=${CDB_YEAR_START}\n')
    out.writei('export CDB_YEAR_END=${CDB_YEAR_END}\n')
    out.writei('export CDB_CENTER=${CDB_CENTER}\n')
    out.writei('export CDB_MODEL=${CDB_MODEL}\n')
    out.writei('export CDB_RUN_ID=${CDB_RUN_ID}\n')
    out.writei('export CDB_EXPT=${CDB_EXPT}\n')
    out.writei('export CDB_DIAG_NAME=${CDB_DIAG_NAME}\n')
    out.writei('export CDB_DIAG_HEADER=${CDB_DIAG_HEADER}\n')
    out.writei('export CDB_TEMP_DIR=${CDB_TEMP_DIR}\n')
    out.writei('export CDB_OUT_FILE=${CDB_OUT_FILE}_$(printf "%04d\n" $CDB_YEAR)_${CDB_MONTH}\n')
    #for type_var in cdb_environment_variables.keys():
    #    for name_var in cdb_environment_variables[type_var]:
    #        out.writei('export CDB_{0}_{1}=$CDB_{0}_{1}\n'.format(name_var.upper(),type_var.upper()))
    out.writei('\n')
    out.writei('export CDB_VAR_LIST="${CDB_VAR_LIST}"\n')

    out.open.write('EndOfScriptHeader\n')
    out.writei('\n')
    out.writei('cat >> $3 <<\'EndOfScriptMain\'\n')
    out.writei('\n')

    out.writei('#GENERATE A TIME AXIS FOR THE MONTH FOR EACH VARIABLE:\n')
    out.writei('\n')
    for var_name in self.variable_list.keys():
        if self.variable_list[var_name][0]!='fx':
            in_time_file='${CDB_DIAG_HEADER}.'+var_name+'_'+'_'.join(self.variable_list[var_name])+'.time.nc'
            out_time_file='${CDB_TEMP_DIR}/${CDB_OUT_FILE}.'+var_name+'_'+'_'.join(self.variable_list[var_name])+'.time.nc'
            out.writei('if [ "$CDB_OFFSET" -gt "0" ]; then\n')
            out.writei('cdo -selsmon,${CDB_MONTH},$CDB_OFFSET -selyear,${CDB_YEAR} '+' '.join([in_time_file,out_time_file])+'\n')
            out.writei('else\n')
            out.writei('cdo -selmon,${CDB_MONTH} -selyear,${CDB_YEAR} '+' '.join([in_time_file,out_time_file])+'\n')
            out.writei('fi\n')

    return self

def end_monthly_loop(self,out,line):
    #Closing the monthly loop
    for var_name in self.variable_list.keys():
        if self.variable_list[var_name][0]!='fx':
            out_time_file='${CDB_TEMP_DIR}/${CDB_OUT_FILE}.'+var_name+'_'+'_'.join(self.variable_list[var_name])+'.time.nc'
            out.writei('rm '+out_time_file+'\n')
    out.open.write('EndOfScriptMain\n')
    out.writei('}\n')

    #MONTH LOOP --- SHOULD ALLOW A PBS IMPLEMENTATION
    out.writei('\n')
    out.writei('\n')
    out.writei('CDB_YEAR=${CDB_YEAR_START}\n')

    #Create monthly_scripts
    out.writei('if [ -z "$CDB_YEARS_FIX_LIST" ];then\n')
    out.inc_indent()
    out.writei('while [ "$CDB_YEAR" -le "$CDB_YEAR_END" ]; do\n')
    out.inc_indent()
    months_list=' '.join([str(month).zfill(2) for month in self.months_list])
    out.writei('for CDB_MONTH in '+months_list+'; do\n')
    out.inc_indent()
    out.writei('monthly_processing $CDB_YEAR $CDB_MONTH ${CDB_TEMP_DIR}/script_$( printf "%04d" ${CDB_YEAR})_${CDB_MONTH}.sh\n')
    out.dec_indent()
    out.writei('done #CDB_MONTH\n')
    out.writei('let "CDB_YEAR += 1"\n')
    out.dec_indent()
    out.writei('done #CDB_YEAR\n')
    out.dec_indent()
    out.writei('else\n')
    out.inc_indent()
    out.writei('#The user has specified a subset of years to be computed.\n')
    out.writei('for CDB_YEAR in $CDB_YEARS_FIX_LIST; do\n')
    out.inc_indent()
    out.writei('for CDB_MONTH in '+months_list+'; do\n')
    out.inc_indent()
    out.writei('monthly_processing $CDB_YEAR $CDB_MONTH ${CDB_TEMP_DIR}/script_$( printf "%04d" ${CDB_YEAR})_${CDB_MONTH}.sh\n')
    out.dec_indent()
    out.writei('done #CDB_MONTH\n')
    out.dec_indent()
    out.writei('done #CDB_YEAR\n')
    out.dec_indent()
    out.writei('fi\n')

    ramdisk_protection(self,out)
    #Process the monthly_scripts
    if self.debug:
        out.writei('ls ${CDB_TEMP_DIR}/script_????_??.sh | parallel --tmpdir ${CDB_OUT_DIR} --tty "bash {}; rm {}"\n')
    else:
        out.writei('ls ${CDB_TEMP_DIR}/script_????_??.sh | parallel --tmpdir ${CDB_OUT_DIR} -k -j'+str(self.m_async)+' "bash {}; rm {}" > ${CDB_OUT_DIR}/${CDB_OUT_FILE}.out 2>&1 \n')
        #out.writei('pid=${!}\n')
        #out.writei('${CDB_SOURCE_DIR}/parallel_ramdisk.sh $pid ${CDB_TEMP_DIR}\n')
        out.writei('rm ${CDB_TEMP_DIR}/script_????_??.sh\n')
    return self

def main():

    #Option parser
    description=textwrap.dedent('''\
    This script sets up and run a diagnostic on the CMIP5 archive
    
    Created scripts might require:
    NCO 4.0.8 w/ OPeNDAP, netCDF4/HDF5
    CDO 1.5.1 w/ OPeNDAP, netCDF4/HDF5
    gnu-parallel
    ''')
    epilog="Frederic Laliberte, Paul J. Kushner, University of Toronto, 12/2012"
    usage = "%(prog)s [options] diagnostic"
    version_num='0.2'
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            usage=usage,
                            version='%(prog)s '+version_num,
                            epilog=epilog)
    parser.add_argument("in_diagnostic_headers_file")
    parser.set_defaults(drs=None)

    #Setup options
    setup_group=parser.add_argument_group("Setup","These options must be set when using this script")
    setup_group.add_argument("--debug",dest="debug",
                      default=False, action="store_true",
                      help="Debug flag. Disables option --m_async.")
    setup_group.add_argument("--run",dest="run",
                      default=False, action="store_true",
                      help="Launches the scripts.")

    #Processing Options
    proc_group=parser.add_argument_group("Processing",
                            "Use these options to set parallelized options.\n\
                             BEWARE! Asynchronous options are largely untested!")
    proc_group.add_argument("--m_async",dest="m_async",
                      default=1,
                      help="Uses the specified # of processors for the asynchronous processing of months.")
    proc_group.add_argument("--ppn",dest="ppn",
                      default=1,
                      help="Processors per nodes. Set to max(M_ASYNC,PPN).")
    proc_group.add_argument("-P","--pbs_expt",dest="pbs_expt",
                      default=False, action="store_true",
                      help="Prepare a pbs header for each model/rip/experiment")
    proc_group.add_argument("-w","--walltime",dest="walltime",
                      default="1:00:00",
                      help="pbs job walltime.")
    proc_group.add_argument("-q","--queue",dest="queue",
                      default=None,
                      help="PBS queue type")
    proc_group.add_argument("--submit",dest="submit",
                      default=False, action="store_true",
                      help="Submit scripts to the PBS queue")

    slicing_args={
                  'center': [str,'Modelling center name'],
                  'model': [str,'Model name'],
                  'experiment': [str,'Experiment name'],
                  'rip': [str,'RIP identifier, e.g. r1i1p1'],
                  'var': [str,'Variable name, e.g. tas'],
                  'frequency': [str,'Frequency, e.g. day'],
                  'realm': [str,'Realm, e.g. atmos'],
                  'cmor_table': [str,'CMOR table name, e.g. day'],
                  'year': [int,'Year'],
                  'month': [int,'Month as an integer ranging from 1 to 12'],
                  'file_type': [str,'File type: OPEnDAP, local_file, HTTPServer, GridFTP']
                  }

    #Slicing options
    slicing_group=parser.add_argument_group("Slicing",
                            "Use these options to restrict the processing to a subset of models")
    for arg in slicing_args.keys():
        slicing_group.add_argument('--'+arg,type=slicing_args[arg][0],help=slicing_args[arg][1])

    options = parser.parse_args()

    #Load diagnostic description file:
    paths_dict=cdb_query_archive_class.SimpleTree(json_tools.open_json(options),options)
    #paths_dict.pointers.slice(options)
    #paths_dict.pointers.create_database(cdb_query_archive_class.find_simple)

    simulations_list=paths_dict.simulations(options)

    diag_desc=paths_dict.header

    if options.submit: print('Submitting jobs using qsub ')

    for dir_to_set in ['output_dir','diagnostic_dir','runscripts_dir','temp_dir']:
        dir_path=os.path.expanduser(
                    os.path.expandvars(diag_desc[dir_to_set]))
        if os.path.isabs(dir_path):
            setattr(options,dir_to_set,os.path.abspath(dir_path))
        else:
            setattr(options,dir_to_set,
                    os.path.abspath(dir_path))

    options.diagnostic=diag_desc['diagnostic_name']

    options.variable_list=diag_desc['variable_list']

    #Check if only a subset of months were requested:
    if 'months_list' in diag_desc.keys():
        options.months_list=diag_desc['months_list']
    else:
        options.months_list=range(1,13)

    if options.month:
        options.months_list=[options.month]

    for simulation in paths_dict.simulations_list():
        options.center, options.model, options.rip = simulation
        for exp in diag_desc['experiment_list'].keys():
            options.experiment=exp
            period_list=diag_desc['experiment_list'][exp]
            if not isinstance(period_list,list): period_list=[period_list]
            map(lambda x:period_script(x,options),period_list)

    if not options.run and not options.submit:
        print 'Scripts are available in '+options.runscripts_dir

def period_script(period,options):
    options.years=set_period(period,options)

    #PREPARE SCRIPTS
    experiment = Experiment_Setup(options)
    experiment.prepare_scripts()

    #options.file_type='HTTPServer'
    #print cdb_query_archive.list_unique_paths(paths_dict,options)

    if options.run:
        os.system('bash '+experiment.runscript_file)
    elif options.submit:
        os.system('qsub '+experiment.runscript_file)

def set_period(period,options):
    if not options.year:
        return period
    else:
        years_range=[int(item) for item in period.split(',')]
        years_range[1]+=1
        if options.year not in range(*years_range):
            return period
        else:
            return ','.join([str(options.year),str(options.year)])

if __name__ == "__main__":
    main()
