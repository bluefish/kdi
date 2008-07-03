#!/usr/bin/env python
#
# $Id: replace_headers.py $
#
# Created 2008/07/02
#
# Copyright 2008 Kosmix Corporation.  All rights reserved.
# Kosmix PROPRIETARY and CONFIDENTIAL.

import os
import cStringIO
import re

_extmap = {
    '.cc'   : 'cc',
    '.c'    : 'cc',
    '.cpp'  : 'cc',
    '.c++'  : 'cc',
    '.h'    : 'cc',
    '.hpp'  : 'cc',
    '.py'   : 'py',
    '.pyw'  : 'py',
}

def getFileType(path):
    ext = os.path.splitext(path)[1]
    ext = ext.lower()
    return _extmap.get(ext.lower(), '?')

def extractCppHeader(content):
    f = cStringIO.StringIO(content)
    h = ''
    inComment = False
    for line in f:
        l = line.strip()
        if not l:
            if h.strip(): break
            h += line
        elif l.startswith('//'):
            h += line
        elif inComment:
            h += line
            if l.endswith('*/'):
                inComment = False
        elif l.startswith('/*'):
            h += line
            inComment = not l.endswith('*/')
        else:
            break
    return h

def extractPyHeader(content):
    f = cStringIO.StringIO(content)
    h = ''
    for line in f:
        l = line.strip()
        if not l:
            if h.strip(): break
            h += line
        elif l.startswith('#'):
            h += line
        else:
            break
    return h

_headerExtractors = {
    'cc' : extractCppHeader,
    'py' : extractPyHeader,
}

_created = re.compile('Created (\d+)/\d+/\d+', re.I)
def extractCreated(header):
    m = _created.search(header)
    if m:
        return m.group(1),m.group(0)
    else:
        return '2008',''

def getCppHeader(oldHeader):
    year,creation = extractCreated(oldHeader)
    h = '''//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) %s Josh Taylor (Kosmix Corporation)
''' % year
    if creation:
        h += '// %s\n' % creation
    h += '''//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or any later version.
// 
// KDI is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------
'''
    return h

def getPyHeader(oldHeader):
    year,creation = extractCreated(oldHeader)
    h = '''#!/usr/bin/env python
#
# Copyright (C) %s Josh Taylor (Kosmix Corporation)
''' % year
    if creation:
        h += '# %s\n' % creation
    h += '''#
# This file is part of KDI.
# 
# KDI is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
# 
# KDI is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
'''
    return h

_headerReplacement = {
    'cc' : getCppHeader,
    'py' : getPyHeader,
}

def replaceHeader(path):
    type = getFileType(path)
    if type in _headerExtractors:
        print path
        content = file(path).read()
        header = _headerExtractors[type](content)
        rest = content[len(header):]
        newHeader = _headerReplacement[type](content)
        newPath = path+'.krom'
        file(newPath,'w').write(newHeader + rest)
        os.rename(newPath, path)
    else:
        print 'skipping',path

def processArg(path):
    for root, dirs, files in os.walk(path):
        for f in files:
            p = os.path.join(root, f)
            replaceHeader(p)

def main():
    import optparse
    op = optparse.OptionParser()
    opt,args = op.parse_args()

    for x in args:
        processArg(x)

if __name__ == '__main__':
    main()
