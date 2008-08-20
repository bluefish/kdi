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

def createTable(meta, table, server, force):
    import pykdi

    # Open META table
    metaTable = pykdi.Table(meta)

    # Create the row key
    from util.zero import zeroEncode
    row = zeroEncode(table, '\x02', '')
    
    # Make sure the table hasn't already been created (note this is a
    # race if there are other processes trying to create the table).
    for r,c,t,v in metaTable.scan('row = %r' % row):
        if force:
            metaTable.erase(r,c,t)
        else:
            raise RuntimeError('table %r already exists' % table)
    
    metaTable.set(row, 'config', 0, 'server = %s\n' % server)
    metaTable.sync()
    

def main():
    import optparse
    op = optparse.OptionParser()
    op.add_option('-m','--meta',help='location of META table')
    op.add_option('-t','--table',help='name of table to create')
    op.add_option('-s','--server',help='server assignment for the table')
    op.add_option('-f','--force',help='server assignment for the table',
                  action='store_true')
    opt,args = op.parse_args()

    if not opt.meta:
        op.error('need --meta')
    if not opt.table:
        op.error('need --table')
    if not opt.server:
        op.error('need --server')

    createTable(opt.meta, opt.table, opt.server, opt.force)

if __name__ == '__main__':
    main()
