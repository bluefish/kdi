#!/usr/bin/env python
#
# Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
# Created 2008-03-06
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

# I started this program to make it easier to package up applications
# with their minimal set of required shared libraries.  I got
# distracted working on other things, but so far it prints out the
# paths to all linked libraries.  -- josh 2008-03-20

import os
import sys
import re

class MissingDependency(Exception):
    def __init__(self, binFile, missingDep):
        self.binFile = binFile
        self.missingDep = missingDep

    def __str__(self):
        return 'unresolved dependency in %r: %r' % (self.binFile, self.missingDep)
        

_ldd_rel_fail = re.compile(r'([^ ]+) => not found')
_ldd_rel =      re.compile(r'([^ /]+) => ([^ ]*) \((0x[0-9A-Fa-f]+)\)')
_ldd_abs =      re.compile(r'(/[^ ]+) \((0x[0-9A-Fa-f]+)\)')

def iterLdd(fn):
    #print 'ldd "%s"' % fn
    for l in os.popen('ldd "%s" 2>/dev/null'%fn):
        l = l.strip()
        if l == 'not a dynamic executable':
            return
        elif l == 'statically linked':
            return

        m = _ldd_rel_fail.match(l)
        if m:
            yield (m.group(1), None, None)
        else:
            m = _ldd_rel.match(l)
            if m:
                yield m.group(1,2,3)
            else:
                m = _ldd_abs.match(l)
                if m:
                    yield m.group(1,1,2)
                else:
                    raise RuntimeError('unexpected output from '
                                       'ldd %r: %r' % (fn, l))

def getDependentFiles(fn, _cache={}):
    if fn in _cache:
        return _cache[fn]

    targets = set()
    for sym,file,addr in iterLdd(fn):
        if not addr:
            raise MissingDependency(fn, sym)
        if not os.path.isfile(file) or os.path.isabs(sym):
            continue
        targets.add(file)
        targets.update(getDependentFiles(file))

    _cache[fn] = targets
    return targets

def buildPackage(outFn, files):
    import tempfile
    import subprocess
    import sys
    
    print >>sys.stderr, 'Making temporary directory for package'
    tmpdir = tempfile.mkdtemp()
    try:
        if files:
            print >>sys.stderr, 'Copying files to package directory'
            subprocess.call(['cp'] + files + [tmpdir])

        print >>sys.stderr, 'Building tarball:', outFn
        subprocess.call(['tar', '-czvf', outFn, '-C', tmpdir, '.'])
    finally:
        print >>sys.stderr, 'Cleaning package directory'
        subprocess.call(['rm', '-rf', tmpdir])

def main():
    import optparse
    op = optparse.OptionParser()
    op.add_option('-o', '--output')
    opt,args = op.parse_args()

    try:
        targets = set()
        for x in args:
            targets.add(x)
            targets.update(getDependentFiles(x))
    except MissingDependency, ex:
        print >>sys.stderr, ex
        sys.exit(1)

    out = [ x for x in targets
            if not x.startswith('/lib') and not x.startswith('/usr/lib') ]
    out.sort()

    if not opt.output:
        for x in out:
            print x
    else:
        buildPackage(opt.output, out)

if __name__ == '__main__':
    main()
