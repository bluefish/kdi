#!/usr/bin/env python
#
# Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
# Created 2008-06-02
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

from util.string_interpolate import Alphabet, chr_range
from kdi.splits import iterSplits, loadSplitPointsFromFile
from util.zero import zeroEncode
import pykdi

def iterMetaCells(table, nPartitions, splitPoints, servers, format, alphabet):
    d = {}
    d['table'] = table

    pi = 0
    for split in iterSplits(nPartitions, splitPoints, alphabet):
        d['partition'] = pi
        d['server'] = servers[pi % len(servers)]
        row = zeroEncode(table, '\x01', split)
        value = format % d
        yield (row, 'location', 0, value)
        pi += 1

    d['partition'] = pi
    d['server'] = servers[pi % len(servers)]
    row = zeroEncode(table, '\x02', '')
    value = format % d
    yield (row, 'location', 0, value)

def main():
    import optparse
    op = optparse.OptionParser()
    op.add_option('-m','--meta',help='location of metatable')
    op.add_option('-t','--table',help='name of table to insert')
    op.add_option('-k','--count',type='int',help='number of partitions to make')
    op.add_option('-p','--partitions',help='file of partition information')
    op.add_option('-s','--servers',help='comma-separated tablet servers')
    op.add_option('-f','--format',help='tablet URI format (def=%default)',
                  default='kdi://%(server)s/%(table)s/%(partition)s')
    op.add_option('-n','--dryrun',action='store_true',
                  help='print meta table cells without loading them')
    op.add_option('-b','--binary',action='store_true',
                  help='use binary string interpolation')
    
    opt,arg = op.parse_args()

    if not opt.table:
        op.error('need --table')
    if not opt.count or opt.count < 1:
        op.error('need --count')

    if opt.binary:
        alphabet = Alphabet(chr_range('\x00', '\xff'))
    else:
        alphabet = Alphabet(chr_range(' ', '~'))
    
    if opt.partitions:
        pts = loadSplitPointsFromFile(opt.partitions)
    else:
        pts = []

    if not opt.servers:
        op.error('need --servers')
    servers = opt.servers.split(',')
    if not servers:
        op.error('need --servers')

    if opt.dryrun:
        def emit(r,c,t,v):
            print '(%r,%r,%r,%r)' % (r,c,t,v)
        metaTable = None
    else:
        if not opt.meta:
            op.error('need --meta')
        metaTable = pykdi.Table(opt.meta)
        def emit(r,c,t,v):
            metaTable.set(r,c,t,v)

    #for split in iterSplits(opt.count, pts, alphabet):
    #    print split
    #return

    for r,c,t,v in iterMetaCells(opt.table, opt.count, pts, servers, opt.format, alphabet):
        emit(r,c,t,v)

    if metaTable:
        metaTable.sync()

if __name__ == '__main__':
    main()
