#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/util/term.py#1 $
#
# Created 2006/08/02
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

import os

def ioctl_GWINSZ(fd):
    try:
        ### Discover terminal width
        import fcntl, termios, struct, os
        cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
    except:
        return None
    return cr

def dimensions():
    """dimensions() -> (columns, rows)

    Get dimensions of current terminal.  Defaults to (80,25) if it
    cannot be determined from the system environment.
    """
    ### decide on *some* terminal size
    # try open fds
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        # ...then ctty
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        # env vars or defaults
        cr = (os.environ.get('LINES', 25), os.environ.get('COLUMNS', 80))

    # reverse rows, cols
    # return cols,rows
    return int(cr[1]), int(cr[0])


def main():
    print "Terminal size: cols=%d, rows=%d" % dimensions()

if __name__ == '__main__':
    main()
