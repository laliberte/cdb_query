def start_parallel_instance(self,out,line):
    if not self.dim_async > 1:
        return self

    if self.parallel_dimension != None:
        raise IOError('Attempting to parallelize a dimension without having closed the previous parallel instance')
    else:
        self.parallel_dimension = line.lstrip()[10:].rstrip('\n').strip()

    out.writei('#Creating a parallel loop over dimension '+self.parallel_dimension+' with '+str(self.dim_async)+' processors:\n')
    out.writei('#########################################################################\n')
    out.writei('#Identify the files to transfer\n')
    out.writei('LIST_TEMP_FILE=`ls ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}*`\n')
    out.writei('export LIST_FILE_ID=`for FILE in ${LIST_TEMP_FILE}; do echo ${FILE#${CDB_TEMP_DIR}/${CDB_TEMP_FILE}}; done`\n')
    out.writei('export CDB_TEMP_FILE="${CDB_TEMP_FILE}"\n')

    out.writei('\n')
    out.writei('#Define splitting function:\n')
    out.writei('function parallel_dimension_'+self.parallel_dimension+str(self.parallel_dimension_number)+'() {\n')

    out.inc_indent()
    out.writei('cat > $1 <<\'EndOfFunction\'\n')
    out.writei('NUM_DIM=`echo $1 | tr \',\' \' \' | awk -F\' \' \'{ print $1 }\'`\n')
    out.writei('LAST_NUM=`echo $1 | tr \',\' \' \' | awk -F\' \' \'{ print $2 }\'`\n')
    out.writei('NUM_ID=$(printf "%03d" ${NUM_DIM})\n')
    out.writei('for FILE_ID in $LIST_FILE_ID; do\n')

    out.inc_indent()
    out.writei('DIM_LENGTH=`ncks -H -v '+self.parallel_dimension+' ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID} | wc -l`\n')
    out.writei('let "DIM_LENGTH-=1"\n')
    out.writei('if [ "$DIM_LENGTH" -gt "1" ]; then\n')

    out.inc_indent()
    out.writei('ncks -d '+self.parallel_dimension+',${NUM_DIM},${LAST_NUM} ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID} ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}.'+self.parallel_dimension+'${NUM_ID}${FILE_ID}\n')
    out.dec_indent()
    out.writei('else\n')
    out.inc_indent()
    out.writei('cp ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID} ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}.'+self.parallel_dimension+'${NUM_ID}${FILE_ID}\n')
    out.dec_indent()
    out.writei('fi\n')

    out.dec_indent()
    out.writei('done\n')
    out.writei('if [ -e "${CDB_TEMP_DIR}/${CDB_TEMP_FILE}" ]; then\n')
    out.writei('ncks -d '+self.parallel_dimension+',${NUM_DIM},${LAST_NUM} ${CDB_TEMP_DIR}/${CDB_TEMP_FILE} ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}.'+self.parallel_dimension+'${NUM_ID}\n')
    out.writei('fi\n')
    out.writei('CDB_TEMP_FILE="${CDB_TEMP_FILE}.'+self.parallel_dimension+'${NUM_ID}"\n')
    out.writei('#########################################################################\n')
    out.inc_indent()
    out.inc_indent()
    return self

def end_parallel_instance(self,out,line):
    if not self.dim_async > 1:
        return self

    if line.lstrip()[14:].rstrip('\n').strip() != self.parallel_dimension:
        raise IOError('Attempting to close parallel instance {0} when parallel instance {1} is open'.format(line.lstrip()[14:].rstrip('\n').strip(),self.parallel_dimension) )
    else:
        out.dec_indent()
        out.dec_indent()
        out.writei('#########################################################################\n')
        out.writei('for FILE_ID in $LIST_FILE_ID; do rm -f ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID}; done\n')
        out.writei('if [ -e "${CDB_TEMP_DIR}/${CDB_TEMP_FILE}" ]; then\n')
        out.writei('rm ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}\n')
        out.writei('fi\n')
        out.writei('\n')
        if self.parallel_dimension != 'time':
            out.writei('LIST_LOCAL_FILE=`ls ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}*`\n')
            out.writei('export LIST_LOCAL_FILE_ID=`for FILE in ${LIST_LOCAL_FILE}; do echo ${FILE#${CDB_TEMP_DIR}/${CDB_TEMP_FILE}}; done`\n')
            out.writei('for FILE_ID in $LIST_LOCAL_FILE_ID; do\n')
            out.inc_indent()
            out.writei('ncpdq -a '+self.parallel_dimension+',time ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID} ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID}_perm\n')
            out.writei('mv ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID}_perm ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID}\n')
            out.dec_indent()
            out.writei('done\n')
        out.dec_indent()
        out.open.write('EndOfFunction\n')
        out.dec_indent()
        out.writei('}\n')
        out.writei('parallel_dimension_'+self.parallel_dimension+str(self.parallel_dimension_number)+' ${CDB_TEMP_DIR}/'+self.parallel_dimension+'_function'+str(self.parallel_dimension_number)+'.sh\n')
        out.writei('#Performing parallel computation\n')
        out.writei('DIM_LENGTHS=`for FILE in $LIST_TEMP_FILE; do ncks -H -v '+self.parallel_dimension+' ${FILE} | wc -l; done`\n')
        out.writei('DIM_LENGTH=`for DIM in ${DIM_LENGTHS}; do echo $DIM; done | awk \'{if(min==""){min=max=$1}; if($1>max) {max=$1}; if($1<min) {min=$1}} END {print max}\'`\n')
        out.writei('let "DIM_LENGTH -= 2"\n')
        if line.lstrip()[:14].upper() == '#!END PARAFULL':
            out.writei('STRIDE=1\n')
        else:
            out.writei('STRIDE=`expr $DIM_LENGTH / '+str(self.dim_async)+' + 1`\n')
        out.writei('NUM_DIM=0\n')
        out.writei('while [ "$NUM_DIM" -le "$DIM_LENGTH" ]; do if [ "$((${NUM_DIM} + ${STRIDE} - 1))" -ge "$DIM_LENGTH" ]; then \\\n')
        out.writei('echo ${NUM_DIM},${DIM_LENGTH}; else echo ${NUM_DIM},$((${NUM_DIM} + ${STRIDE} - 1)); fi; \\\n')
        out.writei('let "NUM_DIM += ${STRIDE}"; done |\\\n')
        out.writei('parallel -j'+str(self.dim_async)+' -k "bash ${CDB_TEMP_DIR}/'+self.parallel_dimension+'_function'+str(self.parallel_dimension_number)+'.sh {}"\n')
        out.writei('rm ${CDB_TEMP_DIR}/'+self.parallel_dimension+'_function'+str(self.parallel_dimension_number)+'.sh\n')
        out.writei('\n')
        out.writei('#Delete files that were deleted in the parallel process\n')
        out.writei('for FILE_ID in $LIST_FILE_ID; do\n')
        out.inc_indent()
        out.writei('if [ ! -f ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}.'+self.parallel_dimension+'000${FILE_ID} ]; then rm ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID}; fi\n')
        out.dec_indent()
        out.writei('done\n')

        out.writei('#Recombine output files\n')
        out.writei('LIST_PARALLEL_FILE=`ls ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}.'+self.parallel_dimension+'000*`\n')
        out.writei('LIST_PARALLEL_FILE_ID=`for FILE in ${LIST_PARALLEL_FILE}; do echo ${FILE#${CDB_TEMP_DIR}/${CDB_TEMP_FILE}.'+self.parallel_dimension+'000}; done`\n')

        out.writei('for FILE_ID in $LIST_PARALLEL_FILE_ID; do\n')
        out.inc_indent()
        out.writei('if [ ! -f ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID} ]; then\n')
        out.inc_indent()
        out.writei('ncrcat ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}.'+self.parallel_dimension+'???${FILE_ID} ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID}\n')
        out.writei('rm ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}.'+self.parallel_dimension+'???${FILE_ID}\n')
        if self.parallel_dimension != 'time':
            out.writei('ncpdq -a time,'+self.parallel_dimension+' ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID} ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID}_perm\n')
            out.writei('mv ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID}_perm ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}${FILE_ID}\n')
        out.dec_indent()
        out.writei('else\n')
        out.inc_indent()
        out.writei('rm ${CDB_TEMP_DIR}/${CDB_TEMP_FILE}.'+self.parallel_dimension+'???${FILE_ID}\n')
        out.dec_indent()
        out.writei('fi\n')
        out.dec_indent()
        out.writei('done\n')
        out.writei('#########################################################################\n')

        #These parameters are important if many parallel instances are specified:
        self.parallel_dimension=None
        self.parallel_dimension_number+=1
    return self
