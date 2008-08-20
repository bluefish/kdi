#!/usr/bin/env python
#
# Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
# Created 2008-08-19
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

def updateLocations(meta, port):
    import pykdi
    from util.props import Properties
    from cStringIO import StringIO
    from util.zero import zeroDecode

    # Open META table
    metaTable = pykdi.Table(meta)

    # Go through 'config' cells, extract 'server' variable, and write
    # 'location' cells
    for row,col,time,val in metaTable.scan('column = "config"'):
        table,dummy,last = zeroDecode(row)
        cfg = Properties(StringIO(val))
        loc = 'kdi://%s:%s/%s' % (cfg.get('server'), port, table)
        metaTable.set(row, 'location', time, loc)
    
    # Sync changes
    metaTable.sync()
    

def main():
    import optparse
    op = optparse.OptionParser()
    op.add_option('-m','--meta',help='location of META table')
    op.add_option('-p','--port',help='port to use for all locations')
    opt,args = op.parse_args()

    if not opt.meta:
        op.error('need --meta')
    if not opt.port:
        op.error('need --port')

    updateLocations(opt.meta, opt.port)

if __name__ == '__main__':
    main()
