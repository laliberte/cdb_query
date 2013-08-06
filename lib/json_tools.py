import json
import gzip

def open_json(options):
    if options.in_diagnostic_headers_file[-3:]=='.gz':
        infile=gzip.open(options.in_diagnostic_headers_file,'r')
    else:
        infile=open(options.in_diagnostic_headers_file,'r')
    paths_dict=json.load(infile)
    if options.drs!=None:
        paths_dict['header']['drs']=options.drs
    return paths_dict

def close_json(paths_dict,options):
    if options.gzip:
        outfile = gzip.open(options.out_diagnostic_headers_file+'.gz','w')
    else:
        outfile = open(options.out_diagnostic_headers_file,'w')

    json.dump({'pointers':paths_dict.pointers.tree,'header':paths_dict.header},outfile)
    outfile.close()
    return
