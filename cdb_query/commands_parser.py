
def _get_command_name(options, next=False, prev=False):
    if options.command_number == 0:
        if ( not next and
             not prev ):
            return options.command
        elif next:
            if ( 'command_1' in dir(options) and
                 not ( getattr(options,'command_1') == 'process' ) ):
                return getattr(options,'command_1')
            else:
                return None
        elif prev:
            return None
    else:
        if ( ( not next and
               not prev ) or
             ( next and prev) ):
            return getattr(options,'command_{0}'.format(options.command_number))
        elif next:
            if ( 'command_{0}'.format(options.command_number+1) in dir(options) and
                 not getattr(options,'command_{0}'.format(options.command_number+1)) == 'process' ):
                return getattr(options,'command_{0}'.format(options.command_number+1))
            else:
                return None
        elif prev:
            if 'command_{0}'.format(options.command_number-1) in dir(options):
                return getattr(options,'command_{0}'.format(options.command_number-1))
            else:
                return None

def _get_command_names(options):
    command_names = [options.command,]
    command_fields = [ field for field in dir(options)
                       if 'command' in field ]
    for id in range(1,len(command_fields)):
        field = 'command_{0}'.format(id)
        if field in command_fields:
            command_names.append(getattr(options,field))
    command_names.pop()
    return command_names

#def _get_record_command_name(options):
#    if ( getattr(options,'command_{0}'.format(options.command_number+1)) == 'record' and
#         getattr(options,'command_{0}'.format(options.command_number)) == 'record' ):
#        return 'record'
#    elif options.command_number == 1 :
#        return ( 'record_'+
#                 getattr(options,'command') )
#    else:
#        return ( 'record_'+
#                 getattr(options,'command_{0}'.format(options.command_number-1)) )
#
#def _get_record_command_names(options):
#    command_names = _get_command_names(options)
#    return [ _record_name_mod(command_id, command, command_names)
#             for command_id, command in enumerate(command_names) 
#             if command == 'record' ]

#def _record_name_mod(command_id, command, command_names):
#    if command_id == len(command_names) - 1:
#        return command
#    else:
#        return command + '_' + command_names[command_id-1]

def _number_of_commands(options):
    return len( [ field for field in dir(options) if 'command' in field ] ) - 1
