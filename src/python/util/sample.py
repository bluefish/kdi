#!/usr/bin/env python
#
# $Id: sample.py $
#
# Created 2008/06/15
#
# Copyright 2008 Kosmix Corporation.  All rights reserved.
# Kosmix PROPRIETARY and CONFIDENTIAL.

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
