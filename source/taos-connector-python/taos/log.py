# info
show_info = True
show_warn = True
show_debug = False
show_debug1 = False
show_debug2 = False
show_debug3 = False


# set show
def setting(info, warn, debug, debug1, debug2, debug3):
    global show_info, show_warn, show_debug, show_debug1, show_debug2, show_debug3
    show_info = info
    show_warn = warn
    show_debug = debug
    show_debug1 = debug1
    show_debug2 = debug2
    show_debug3 = debug3


# info
def info(msg):
    if show_info:
        print(msg)


# warn
def warn(msg):
    if show_warn:
        print(msg)


# debug
def debug(msg):
    if show_debug:
        print(msg)


# debug1
def debug1(msg):
    if show_debug1:
        print(msg)


# debug2
def debug2(msg):
    if show_debug2:
        print(msg)


# debug3
def debug3(msg):
    if show_debug3:
        print(msg)
