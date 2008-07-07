#!/usr/bin/env python
#
# Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
# Created 2008-07-02
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

def getHeaderLines(projectName, author, copyrightYear, creationDate=''):
    capitalProjectName = projectName[:1].upper() + projectName[1:]

    r = ['Copyright (C) %s %s' % (copyrightYear, author)]
    if creationDate:
        r.append('Created %s' % creationDate)
    r.append('')
    r.append('This file is part of %s.' % projectName)
    r.append('')
    r.append('%s is free software; you can redistribute it and/or '
             'modify it under the terms of the GNU General Public '
             'License as published by the Free Software Foundation; '
             'either version 2 of the License, or any later version.'
             % capitalProjectName)
    r.append('')
    r.append('%s is distributed in the hope that it will be useful, '
             'but WITHOUT ANY WARRANTY; without even the implied '
             'warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR '
             'PURPOSE.  See the GNU General Public License for more '
             'details.' % capitalProjectName)
    r.append('')
    r.append('You should have received a copy of the GNU General '
             'Public License along with this program; if not, write '
             'to the Free Software Foundation, Inc., 51 Franklin '
             'Street, Fifth Floor, Boston, MA 02110-1301, USA.')
    return r

def formatHeader(prefix, projectName, author, copyrightYear, creationDate=''):
    import textwrap
    w = textwrap.TextWrapper(
        width=76,
        initial_indent=prefix,
        subsequent_indent=prefix)
    lines = getHeaderLines(projectName, author, copyrightYear, creationDate)
    return '\n'.join(w.fill(line) or prefix for line in lines)

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

_created = re.compile('Created ((\d+)[/-]\d+[/-]\d+)', re.I)
def extractCreated(header):
    m = _created.search(header)
    if m:
        return m.group(2),m.group(1).replace('/','-')
    else:
        return '2008',''

def getCppHeader(oldHeader, projectName, author):
    copyrightYear,creationDate = extractCreated(oldHeader)
    h  = '//---------------------------------------------------------- -*- Mode: C++ -*-\n'
    h += formatHeader('// ', projectName, author, copyrightYear, creationDate) + '\n'
    h += '//----------------------------------------------------------------------------\n'
    return h

def getPyHeader(oldHeader, projectName, author):
    copyrightYear,creationDate = extractCreated(oldHeader)
    h  = '#!/usr/bin/env python\n'
    h += '#\n'
    h += formatHeader('# ', projectName, author, copyrightYear, creationDate) + '\n'
    return h

_headerReplacement = {
    'cc' : getCppHeader,
    'py' : getPyHeader,
}

def replaceHeader(path, projectName, author):
    type = getFileType(path)
    if type in _headerExtractors:
        print path
        content = file(path).read()
        header = _headerExtractors[type](content)
        rest = content[len(header):]
        newHeader = _headerReplacement[type](content, projectName, author)
        newPath = path+'.krom'
        file(newPath,'w').write(newHeader + rest)
        os.rename(newPath, path)
    else:
        print 'skipping',path

def processArg(path, projectName, author):
    if os.path.isfile(path):
        replaceHeader(path, projectName, author)
    else:
        for root, dirs, files in os.walk(path):
            for f in files:
                p = os.path.join(root, f)
                replaceHeader(p, projectName, author)

def main():
    import optparse
    op = optparse.OptionParser()
    op.add_option('-n', '--name', help='name of project')
    op.add_option('-a', '--author', help='name of author')
    opt,args = op.parse_args()

    if not opt.name:
        op.error('need --name')
    if not opt.author:
        op.error('need --author')

    for x in args:
        processArg(x, opt.name, opt.author)

if __name__ == '__main__':
    main()
