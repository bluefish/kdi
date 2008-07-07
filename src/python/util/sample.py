#!/usr/bin/env python
#
# Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
# Created 2008-06-15
# 
# This file is part of the util library.
# 
# The util library is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or any later
# version.
# 
# The util library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

def getLine(fp, pos):
    fp.seek(pos)
    if pos:
        fp.readline()
    return fp.readline()

def sampleLines(fn, num):
    from random import randrange
    f = file(fn)
    try:
        f.seek(0, 2)
        sz = f.tell()
        while num > 0:
            pos = randrange(0, sz)
            line = getLine(f, pos)
            if line:
                yield line
                num -= 1
    except:
        f.close()
        raise


def main():
    import optparse
    op = optparse.OptionParser()
    op.add_option('-n','--num',type='int',help='samples per file')
    op.add_option('-o','--output',help='output file')
    opt,args = op.parse_args()

    num = opt.num
    if not num:
        num = 1

    if opt.output:
        out = file(opt.output, 'w')
    else:
        import sys
        out = sys.stdout

    for fn in args:
        for line in sampleLines(fn, num):
            out.write(line)
            if not line.endswith('\n'):
                out.write('\n')

if __name__ == '__main__':
    main()
