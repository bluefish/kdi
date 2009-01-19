#!/usr/bin/env python
#
# Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
# Created 2009-01-18
#
# This file is part of KDI.
#
# KDI is free software; you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or any later version.
#
# KDI is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import pykdi
from cStringIO import StringIO
from util.zero import zeroDecode
from util.props import Properties

def parseConfig(s):
    cfg = Properties()
    cfg.load(StringIO(s))
    server = cfg.get('server')
    minRow = cfg.get('minRow', None)
    frags = []
    for key in cfg:
        if key in ('server', 'minRow'):
            continue
        if not key.startswith('tables.'):
            raise RuntimeError('unknown key: %s' % key)
        frags.append((key, cfg.get(key)))
    frags.sort()
    frags = [ val for key,val in frags ]
    return server, minRow, frags

def serializeConfig(server, minRow, frags):
    cfg = Properties()
    cfg.set('server', server)
    if minRow is not None:
        cfg.set('minRow', minRow)
    for i,f in enumerate(frags):
        cfg.set('tables.i%d' % i, f)
    o = StringIO()
    cfg.dump(o, noSpace=True)
    return o.getvalue()

def joinTablets(meta, keys, server, minRow, frags):
    cfg = serializeConfig(server, minRow, frags)
    r,c,t = keys[-1]
    for k in keys[:-1]:
        meta.erase(*k)
    meta.set(r,c,t,cfg)

def makeEcho(label):
    def echo(*args):
        print label, args
    return echo

def main():
    import optparse
    op = optparse.OptionParser()
    op.add_option('-m','--meta',help='Location of META table')
    op.add_option('-n','--dryrun',action='store_true',
                  help="Print but don't do")
    opt,args = op.parse_args()

    if not opt.meta:
        op.error('need --meta')

    meta = pykdi.Table(opt.meta)
    scan = meta.scan('column = "config"')

    if opt.dryrun:
        meta.set = makeEcho('meta.set')
        meta.erase = makeEcho('meta.erase')
        meta.sync = makeEcho('meta.sync')
    
    lastMatch = None
    joinMin = None
    sameKeys = []
    for r,c,t,v in scan:
        if opt.dryrun:
            print 'scan:', (r,c,t,v)

        table = zeroDecode(r)[0]
        server,minRow,frags = parseConfig(v)

        match = table,server,frags
        if match != lastMatch:
            if len(sameKeys) > 1:
                joinTablets(meta, sameKeys, lastMatch[1], joinMin,
                            lastMatch[2])
            joinMin = minRow
            sameKeys = []

        sameKeys.append((r,c,t))
        lastMatch = match

    if len(sameKeys) > 1:
        joinTablets(meta, sameKeys, lastMatch[1], joinMin, lastMatch[2])

    meta.sync()


if __name__ == '__main__':
    main()
