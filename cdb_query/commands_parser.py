
def _get_command_name(options, nxt=False, prev=False):
    if options.command_number == 0:
        if (not nxt and
           not prev):
            return options.command
        elif nxt:
            if (hasattr(options, 'command_1') and
               getattr(options, 'command_1') != 'process'):
                return getattr(options, 'command_1')
            else:
                return None
        elif prev:
            return None
    else:
        if ((not nxt and
             not prev) or
           (nxt and prev)):
            return getattr(options,
                           'command_{0}'.format(options.command_number))
        elif nxt:
            nxt_command = 'command_{0}'.format(options.command_number + 1)
            if (hasattr(options, nxt_command) and
               getattr(options, nxt_command) != 'process'):
                return getattr(options, nxt_command)
            else:
                return None
        elif prev:
            prev_command = 'command_{0}'.format(options.command_number - 1)
            if hasattr(options, prev_command):
                return getattr(options, prev_command)
            else:
                return None


def _get_command_names(options):
    command_names = [options.command]
    command_fields = [field for field in dir(options)
                      if 'command' in field]
    for idx in range(1, len(command_fields)):
        field = 'command_{0}'.format(idx)
        if field in command_fields:
            command_names.append(getattr(options, field))
    command_names.pop()
    return command_names


def _number_of_commands(options):
    return len([field for field in dir(options)
                if 'command' in field]) - 1
