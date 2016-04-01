#External:
import netCDF4
import h5py
import copy
import os
import multiprocessing
import Queue

#Internal:
import nc_Database_utils

#http://stackoverflow.com/questions/2080660/python-multiprocessing-and-a-shared-counter
class Counter(object):
    #Shared counter class
    def __init__(self,manager):
        self.val = manager.Value('i', 0)

    def increment(self, n=1):
        with self.val.get_lock():
            value=self.val.value
            self.val.value += n
            return value

    @property
    def value(self):
        return self.val.value

class CDB_queue_manager:
    def __init__(self,options):
        self.manager=multiprocessing.Manager()

        #Create queues:
        self.queues_names=manager.list()
        #for name in ['ask','validate','download_raw','time_split']:
        #for name in ['ask','validate','time_split']:
        for name in ['ask','validate']:
            if (name in dir(options) and getattr(options,name)):
                self.queues_names.append(name)
                     
        if 'download' in dir(options) and getattr(options,'download'):
            self.queues_names.append('download')
        else:
            self.queues_names.append('reduce')
        self.queues_names.append('record')
        
        for queue_name in queues_names:
            setattr(self,queue_name,manager.Queue())
        
        #Create a shared counter to prevent file collisions:
        self.counter=Counter(manager)
        return
                
    def put(self,item):
        #Put the item in the right queue and give it a number:
        getattr(self,item[0]).put((self.counter.increment(),item))
        return

    def get(self):
        #Simple get that goes through the queues sequentially
        timeout_first=0.01
        timeout_subsequent=0.1
        timeout=timeout_first

        while True:
            if len(self.queues_names)==0:
                #If all queues names were removed, let consumer know
                return 'STOP'

            #Get an element from one queue, starting from the last:
            for queue_name in reversed(self.queues_names):
                try:
                    item=getattr(self,queue_name).get(timeout)
                    if item=='STOP':
                        #remove queue name from list if it passed 'STOP'
                        self.queues_names.remove(queue_name)
                    return item
                except Queue.Empty:
                    pass
            #First pass, short timeout. Subsequent pass, longer:
            if timeout==timeout_first: timeout=subsequent
        return 

#def consumer(CDB_queue_manager,project_drs,options):
#    for item in iter(CDB_queue_manager,get,'STOP'):
#        counter=item[0]
#        if 
        

def worker_apply(function_handle,in_queue,out_queue,downloaded_file_list,download_queue):
    for tuple in iter(in_queue.get,'STOP'):
        result=apply_func(function_handle,tuple,downloaded_file_list=downloaded_file_list,download_queue=download_queue)
        out_queue.put(result,out_queue)
    out_queue.put('STOP')
    return

def apply_func(function_handle,tuple,downloaded_file_list=[],download_queue=None):
    return function_handle(*tuple,downloaded_file_list=downloaded_file_list,download_queue=download_queue)

def distributed_apply(function_handle,download_function_handle,project_drs,options,vars_list,manager=None,retrieval_queues=dict()):
    if manager==None:
        manager=multiprocessing.Manager()
    #This is the gathering queue:
    record_queue=manager.Queue()
    #This is the apply queue:
    apply_queue=manager.Queue()
    processes=[]

    #validate_queue, validated_file_list, validate_process = start_download_manager(download_function_handle,retrieval_queues,options)
    #download_queue, downloaded_file_list, download_process = start_download_manager(download_function_handle,retrieval_queues,manager,options)

    #Set up the discovery var per var:
    for var_id,var in enumerate(vars_list):
        options_copy=copy.copy(options)
        for opt_id, opt in enumerate(project_drs.official_drs_no_version):
            if var[opt_id]!=None:
                setattr(options_copy,opt,var[opt_id])
        apply_queue.put((project_drs,options_copy))
    
    #Span up to options.num_procs processes and each child process analyzes only one simulation
    if options.num_procs==1:
        apply_queue.put('STOP')
        if not ('convert' in dir(options) and options.convert):
            output=netCDF4.Dataset(options.out_netcdf_file,'w',format='NETCDF4')
        else:
            output=None

        for tuple in iter(apply_queue.get,'STOP'):
            record_in_output(apply_func(function_handle,tuple,downloaded_file_list=downloaded_file_list,download_queue=download_queue),
                            output,project_drs,options)
            if not ('convert' in dir(options) and options.convert): output.sync()
    else:
        try:
            processes=[multiprocessing.Process(target=worker_apply, 
                                            args=(function_handle,apply_queue,record_queue,downloaded_file_list,download_queue)) for proc in range(options.num_procs)]
            for process in processes:
                process.start()
                apply_queue.put('STOP')
            output=worker_record(record_queue,project_drs,options)
        finally:
            for process in processes: process.terminate()
            if 'terminate' in dir(download_process): download_process.terminate()

    if 'download' in dir(options) and options.download:
        download_queue.put('STOP')
    return output

def worker_record(out_queue,project_drs,options):
    #Open output file:
    if not ('convert' in dir(options) and options.convert):
        output=netCDF4.Dataset(options.out_netcdf_file,'w',format='NETCDF4')
    else:
        output=None
    for process in range(options.num_procs):
        for description in iter(out_queue.get,'STOP'):
            record_in_output(description,output,project_drs,options)
            if not ('convert' in dir(options) and options.convert): output.sync()
    return output

def record_in_output(description,output,project_drs,options):
    if (isinstance(output,netCDF4.Dataset) or
        isinstance(output,netCDF4.Group)):
        return record_in_output_database(description,output,project_drs,options)
    else:
        return record_in_output_directory(description,project_drs,options)

def record_in_output_directory(description,project_drs,options):
    temp_file_name=description[0]
    var=description[1]

    version='v'+datetime.datetime.now().strftime('%Y%m%d')
    var_with_version=[var[project_drs.official_drs_no_version.index(opt)] if opt in project_drs.official_drs_no_version
                     else verions for opt in project_drs.official_drs]

    if ('out_destination' in dir(options)):
        output_file_name=options.out_destination+'/'+'/'.join(var_with_version)+'/'+os.path.basename(temp_file_name)
    else:
        output_file_name=options.out_netcdf_file+'/'+'/'.join(var_with_version)+'/'+os.path.basename(temp_file_name)

    #Create directory:
    try:
        directory=os.path.dirname(output_file_name)
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        pass

    time_frequency=var[project_drs.official_drs_no_version.index('time_frequency')]
    #Get the time:
    output_tmp=netCDF4.Dataset(temp_file_name,'r')
    timestamp=nc_Database_utils.convert_dates_to_timestamps(output_tmp,time_frequency)
    output_tmp.close()

    if timestamp=='':
        os.remove(temp_file_name)
    else:
        os.rename(temp_file_name,'.'.join(output_file_name.split('.')[:-1])+timestamp+'.nc')
    return

def record_in_output_database(description,output,project_drs,options):
    temp_file_name=description[0]
    var=description[1]
    data=netCDF4.Dataset(temp_file_name,'r')
    data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==temp_file_name:
            data_hdf5=h5py.File(item)
    tree=zip(project_drs.official_drs_no_version,var)

    if ('applying_to_soft_links' in dir(options) and
        options.applying_to_soft_links):
        #Do not check empty:
        nc_Database_utils.replace_netcdf_variable_recursive(output,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=False)
    else:
        nc_Database_utils.replace_netcdf_variable_recursive(output,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=True)
        data.close()

    if data_hdf5!=None:
        data_hdf5.close()
    try:
        os.remove(temp_file_name)
    except OSError:
        pass
    return

def start_download_manager(download_function_handle,retrieval_queues,manager,options):
    if 'download' in dir(options) and options.download:
        #This starts a process to handle the download:
        download_queue=manager.Queue()
        downloaded_file_list=manager.list()
        download_process=multiprocessing.Process(target=worker_download, 
                                        name='worker_download',
                                        args=(download_function_handle,download_queue,retrieval_queues,downloaded_file_list))
        download_process.start()
        return download_queue, downloaded_file_list, download_process
    else:
        return None, None, []

def worker_download(download_function_handle, input_queue, retrieval_queues, output_list):
    for tuple in iter(input_queue.get, 'STOP'):
        download_function_handle(*tuple,check_empty=True,queues=retrieval_queues)
        output_list.append(tuple[0])
    return

