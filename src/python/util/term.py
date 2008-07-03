#!/usr/bin/env python
#
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-08-02
#
# This file is part of KDI.
# 
# KDI is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or any later
# version.
# 
# KDI is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
# License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.

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
