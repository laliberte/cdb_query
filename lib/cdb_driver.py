import sys
import os
import shutil
import argparse 
import textwrap
import json
import parallel_driver

class Open_With_Indent:

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

        #For parallel option:
        self.parallel_dimension=None
        self.parallel_dimension_number=0

    def prepare_scripts(self):
        """ Prepares the scripts for bash launch """
    
        runscript_file=self.runscripts_dir+'/'+'_'.join([self.diagnostic,
                                                            self.model,
                                                            self.center,
                                                            self.run_id,
                                                            self.experiment,
                                                            '-'.join(self.years)])
        out=Open_With_Indent(runscript_file,'w')

        out.writei('#!/bin/bash\n')
        if self.pbs_expt:
	    if self.queue != None: out.writei('#PBS -q {0}\n'.format(self.queue))
            #out.writei('#PBS -l nodes=1:ppn={0},walltime={1}\n'.format(max(self.dim_async,self.m_async),self.walltime))
            out.writei('#PBS -l nodes=1:ppn={0}\n'.format(max(self.dim_async,self.m_async)))
            out.writei('#PBS -N {0}_{1}_{2}_{3}_{4}\n'.format(self.years[0],self.years[1],self.model,self.run_id,self.experiment))
            out.writei('#PBS -o {5}/pbs_out/{0}_{1}_{2}_{3}_{4}\n'.format(self.years[0],self.years[1],self.model,self.run_id,self.experiment,self.output_dir))
            out.writei('#PBS -e {5}/pbs_err/{0}_{1}_{2}_{3}_{4}\n'.format(self.years[0],self.years[1],self.model,self.run_id,self.experiment,self.output_dir))

        out.writei('\n')
        out.writei('#The next variable should be empty. If your script completed and some\n')
        out.writei('#years did not process properly (usually due to a timeout on large\n')
        out.writei('#storage systems), you can list the years (space separated)\n')
        out.writei('#to recompute these years. Simply rerun or resubmit the scripts\n')
        out.writei('CDB_YEARS_FIX_LIST=""\n'.format(self.years[0],self.years[1]))
        out.writei('\n')
        out.writei('CDB_YEARS="{0},{1}"\n'.format(self.years[0],self.years[1]))
        out.writei('export CDB_MODEL="{0}"\n'.format(self.model))
        out.writei('export CDB_CENTER="{0}"\n'.format(self.center))
        out.writei('export CDB_RUN_ID="{0}"\n'.format(self.run_id))
        out.writei('export CDB_EXPT="{0}"\n'.format(self.experiment))
        out.writei('export CDB_DIAG_NAME="{0}"\n'.format(self.diagnostic))
        out.writei('export CDB_DIAG_HEADER="{0}"\n'.format(os.path.abspath(self.diag_header)))
        out.writei('export CDB_OUT_FILE=${CDB_DIAG_NAME}_${CDB_MODEL}_${CDB_EXPT}_$(echo $CDB_YEARS | tr \',\' \'_\')_${CDB_RUN_ID}\n')
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
        out.writei('cdb_query_archive slice --experiment=${CDB_EXPT} --model=${CDB_MODEL} --center=${CDB_CENTER} ${CDB_DIAG_HEADER} ${CDB_TEMP_DIR}/${CDB_DIAG_HEADER_BASE}\n')
        out.writei('CDB_DIAG_HEADER="${CDB_TEMP_DIR}/${CDB_DIAG_HEADER_BASE}"\n')

        
        #LOAD SCRIPT FILE AND CONVERT PARALLEL INSTANCES
        script_file=open(self.diagnostic_dir+'/'+self.diagnostic+'.sh','r')

        parallel_dimension=None
        instructions={
                      '#!START MONTH LOOP' : start_monthly_loop,
                      '#!END MONTH LOOP' : end_monthly_loop,
                      '#!PARALLEL' : parallel_driver.start_parallel_instance,
                      '#!END PARA' : parallel_driver.end_parallel_instance
                    }

        for line in script_file:
                out.writei(line)
                for inst in instructions.keys():
                    if line.lstrip().upper()[:len(inst)]==inst:
                        self=instructions[inst](self,out,line)

        out.writei('\n')
        out.writei('rm -rf ${CDB_TEMP_DIR}\n')
        out.writei('\n')
        structure_out_with_cmip5_drs(self,out)
        out.open.close()

def structure_out_with_cmip5_drs(self,out):
    #Finally, introduce the code to structure the output with the CMIP5 DRS:
    out.writei('#THE LAST PART OF THIS SCRIPT REORGANIZES THE OUTPUT TO CONFORM WITH THE CMIP5 DRS:\n')
    out.writei('if [ ! -z "$CDB_CMIP5_COMP_LIST" ]; then\n')
    out.inc_indent()
    out.writei('for FILE_DESC in $CDB_CMIP5_COMP_LIST; do\n')
    out.inc_indent()
    out.writei('FILE=$(echo $FILE_DESC | awk -F\';\' \'{print $1}\')\n')
    out.writei('REALM_FREQ_INFO=$(echo $FILE_DESC | awk -F\';\' \'{print $2}\')\n')
    out.writei('bash $CDB_SOURCE_DIR/structure_out_as_in.sh -d ${CDB_DIAG_NAME} -m ${CDB_MODEL} -e ${CDB_EXPT} -r ${CDB_RUN_ID} -y ${CDB_YEARS} -f ${REALM_FREQ_INFO} $FILE\n')
    out.writei('CDB_INFO="This output was created using the Climate Diagnostics Benchmark toolbox,\
                developed by F. Laliberte and P. J. Kushner, 2012, at the University of Toronto.\
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
    out.writei('CDB_LOCAL_RUN_ID={0}\n'.format(self.run_id))
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
    out.writei('function monthly_processing()\n')
    out.writei('{\n')
    out.writei('cat > $3 <<EndOfScriptHeader\n')
    out.writei('#!/bin/bash\n')
    out.writei('#This file launches a monthly diagnostic\n')
    out.writei('export CDB_YEAR=$1\n')
    out.writei('export CDB_MONTH=$2\n')
    out.writei('\n')
    #out.writei('CDB_YEAR_START=${CDB_YEAR_START}\n')
    #out.writei('CDB_YEAR_END=${CDB_YEAR_END}\n')
    out.writei('export CDB_CENTER=${CDB_CENTER}\n')
    out.writei('export CDB_MODEL=${CDB_MODEL}\n')
    out.writei('export CDB_RUN_ID=${CDB_RUN_ID}\n')
    out.writei('export CDB_EXPT=${CDB_EXPT}\n')
    out.writei('export CDB_DIAG_NAME=${CDB_DIAG_NAME}\n')
    out.writei('export CDB_DIAG_HEADER=${CDB_DIAG_HEADER}\n')
    out.writei('export CDB_TEMP_DIR=${CDB_TEMP_DIR}\n')
    out.writei('export CDB_OUT_FILE=${CDB_OUT_FILE}_${CDB_YEAR}_${CDB_MONTH}\n')
    #for type_var in cdb_environment_variables.keys():
    #    for name_var in cdb_environment_variables[type_var]:
    #        out.writei('export CDB_{0}_{1}=$CDB_{0}_{1}\n'.format(name_var.upper(),type_var.upper()))
    out.writei('\n')
    out.writei('export CDB_VAR_LIST="${CDB_VAR_LIST}"\n')

    out.open.write('EndOfScriptHeader\n')
    out.writei('\n')
    out.writei('cat >> $3 <<\'EndOfScriptMain\'\n')
    out.writei('\n')
    out.writei('#Retrieve path names to data:\n')
    for var_name in self.variable_list.keys():
        if self.variable_list[var_name][0]!='fx':
            retrieval_string='${CDB_CENTER} ${CDB_MODEL} ${CDB_EXPT} ${CDB_RUN_ID} '
        else:
            retrieval_string='${CDB_CENTER} ${CDB_MODEL} ${CDB_EXPT} r0i0p0 '
        retrieval_string+=var_name+' '
        retrieval_string+=' '.join(self.variable_list[var_name])
        retrieval_string+=' ${CDB_YEAR} ${CDB_MONTH}'
        retrieval_string+=' ${CDB_DIAG_HEADER}'
        out.writei('export CDB_'+var_name+'_'+'_'.join(self.variable_list[var_name])+'=$(cdb_query_archive retrieve '+retrieval_string+')\n')
        out.writei('export CDB_'+var_name+'_'+'_'.join(self.variable_list[var_name])+'_TYPE=$(cdb_query_archive retrieve -f '+retrieval_string+')\n')
    out.writei('\n')
    out.writei('#Create a cdo retrieval script:\n')
    out.writei('CDO_RETRIEVAL_SCRIPT=""\n')
    for var_name in self.variable_list.keys():
        var_id = '$CDB_'+var_name+'_'+'_'.join(self.variable_list[var_name])
        out.writei('#Creating retrieval for {0}\n'.format(var_name))
        out.writei('CDB_VAR=$(echo '+var_id+'| awk -F\'|\' \'{print $1}\')\n')
        out.writei('CDB_VAR_START=$(echo '+var_id+' | awk -F\'|\' \'{print $2}\')\n')
        out.writei('CDB_VAR_END=$(echo '+var_id+' | awk -F\'|\' \'{print $3}\')\n')
        out.writei('CDB_TIME_STEPS=$(echo $(seq $((CDB_VAR_START+1)) $((CDB_VAR_END+1))) | tr \' \' \',\')\n')
        out.writei('CDO_RETRIEVAL_SCRIPT="${CDO_RETRIEVAL_SCRIPT} -selname,'+var_name+' -seltimestep,${CDB_TIME_STEPS} $CDB_VAR"\n')
    return self

def end_monthly_loop(self,out,line):
    out.open.write('EndOfScriptMain\n')
    out.writei('}\n')

    #MONTH LOOP --- SHOULD ALLOW A PBS IMPLEMENTATION
    out.writei('\n')
    out.writei('\n')
    out.writei('CDB_YEAR_START=$(echo $CDB_YEARS | awk -F\',\' \'{print $1}\')\n')
    out.writei('CDB_YEAR_END=$(echo $CDB_YEARS | awk -F\',\' \'{print $2}\')\n')
    out.writei('CDB_YEAR=${CDB_YEAR_START}\n')
    #Create monthly_scripts
    out.writei('if [ -z "$CDB_YEARS_FIX_LIST" ];then\n')
    out.inc_indent()
    out.writei('while [ "$CDB_YEAR" -le "$CDB_YEAR_END" ]; do\n')
    out.inc_indent()
    months_list=' '.join([str(month).zfill(2) for month in self.months_list])
    out.writei('for CDB_MONTH in '+months_list+'; do\n')
    out.inc_indent()
    out.writei('monthly_processing $CDB_YEAR $CDB_MONTH ${CDB_TEMP_DIR}/script_${CDB_YEAR}_${CDB_MONTH}.sh\n')
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
    out.writei('monthly_processing $CDB_YEAR $CDB_MONTH ${CDB_TEMP_DIR}/script_${CDB_YEAR}_${CDB_MONTH}.sh\n')
    out.dec_indent()
    out.writei('done #CDB_MONTH\n')
    out.dec_indent()
    out.writei('done #CDB_YEAR\n')
    out.dec_indent()
    out.writei('fi\n')

    #Process the monthly_scripts
    if self.debug:
        out.writei('for SCRIPT in $(ls ${CDB_TEMP_DIR}/script_????_??.sh); do bash $SCRIPT; rm $SCRIPT; done\n')
    elif self.m_async == 1:
        out.writei('for SCRIPT in $(ls ${CDB_TEMP_DIR}/script_????_??.sh); do bash $SCRIPT > ${CDB_OUT_DIR}/${CDB_OUT_FILE}.out 2>&1; rm $SCRIPT; done\n')
    else:
        out.writei('ls ${CDB_TEMP_DIR}/script_????_??.sh | parallel --tmpdir ${CDB_OUT_DIR} -k -j'+str(self.m_async)+' "bash {}; rm {}" > ${CDB_OUT_DIR}/${CDB_OUT_FILE}.out 2>&1 &\n')
        out.writei('pid=${!}\n')
        out.writei('${CDB_SOURCE_DIR}/parallel_ramdisk.sh $pid ${CDB_TEMP_DIR}\n')
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
    parser.add_argument("diag_header")

    #Setup options
    setup_group=parser.add_argument_group("Setup","These options must be set when using this script")
    setup_group.add_argument("--debug",dest="debug",
                      default=False, action="store_true",
                      help="Debug flag. Disables options --m_async and --dim_async.")
    setup_group.add_argument("--run",dest="run",
                      default=False, action="store_true",
                      help="Launches the scripts.")
    #setup_group.add_argument("--time_diff",dest="time_diff",
    #                  default=False, action="store_true",
    #                  help="Request the month before and the month after: allows to take a time derivative.\n\
    #                        If the months before and after are unavailable, the script terminates.")

    #Processing Options
    proc_group=parser.add_argument_group("Processing",
                            "Use these options to set parallelized options.\n\
                             BEWARE! Asynchronous options are largely untested!")
    proc_group.add_argument("-P","--pbs_expt",dest="pbs_expt",
                      default=False, action="store_true",
                      help="Prepare a pbs header for each model/run_id/experiment")
    proc_group.add_argument("-w","--walltime",dest="walltime",
                      default="1:00:00",
                      help="pbs job walltime. Inactive for now: Determined by queue")
    proc_group.add_argument("-q","--queue",dest="queue",
                      default=None,
                      help="PBS queue type")
    proc_group.add_argument("--submit",dest="submit",
                      default=False, action="store_true",
                      help="Submit scripts to the PBS queue")
    proc_group.add_argument("--m_async",dest="m_async",
                      default=1,
                      help="Uses the specified # of processors for the asynchronous processing of months.\n\
                            Cannot be used with --dim_async. Requires NCO version 4.0.0 and above, gnu-parallel.")
    proc_group.add_argument("--dim_async",dest="dim_async",
                      default=1,
                      help="Uses the specified # of processors in dimension splitting.\n\
                            Cannot be used with --m_async. Requires NCO version 4.0.0 and above, gnu-parallel.")

    options = parser.parse_args()

    #Disable asynchronous computing if debugging
    if options.debug:
        options.m_async=1
        options.dim_async=1

    if options.m_async>1 and options.dim_async>1:
        raise IOError('Can only specify M_ASYNC>1 or --DIM_ASYNC>1, but not both.')

    #Load diagnostic description file:
    if options.diag_header[-3:]=='.gz':
        import gzip
        infile=gzip.open(options.diag_header,'r')
    else:
        infile=open(options.diag_header,'r')
    paths_dict=json.load(infile)

    diag_desc=paths_dict['diagnostic']

    if options.submit: print('Submitting jobs using qsub -q '+options.queue)

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

    for simulation in paths_dict['simulations_list']:
        options.center, options.model, options.run_id = simulation.split('_')
        for experiment in diag_desc['experiment_list'].keys():
            options.experiment=experiment
            period_list=diag_desc['experiment_list'][experiment]
            if not isinstance(period_list,list): period_list=[period_list]
            for period in period_list:
                options.years=period

                #PREPARE SCRIPTS
                experiment = Experiment_Setup(options)
                experiment.prepare_scripts()

                if options.run:
                    print 'Not implemented yet'
                elif options.submit:
                    print 'Not implemented yet'

    if not options.run and not options.submit:
        print 'Scripts are available in '+options.runscripts_dir

if __name__ == "__main__":
    main()
