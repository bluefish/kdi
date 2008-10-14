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

class MissingDependency(Exception):
    def __init__(self, binFile, missingDep):
        self.binFile = binFile
        self.missingDep = missingDep

    def __str__(self):
        return 'unresolved dependency in %r: %r' % (self.binFile, self.missingDep)


def getDeps(fn, includeAbsolute=False):
    #print 'ldd "%s"' % fn
    deps = []
    for l in os.popen('ldd "%s" 2>/dev/null'%fn):
        l = l.strip();
        #print '   ',l

        i = l.rfind(' (0x')
        if i != -1:
            l = l[:i]

        if ' => ' in l:
            a,b = l.split(' => ')
            if not os.path.isfile(b):
                b = None
        else:
            if not includeAbsolute:
                continue
            if not os.path.isfile(l):
                continue
            a,b = l,l

        deps.append((a,b))
        #print '   ','%r : %r' % (a,b)

    return deps

def getDependentFiles(fn, _cache={}):
    if fn in _cache:
        return _cache[fn]

    targets = set()
    for sym,target in getDeps(fn):
        if not target:
            raise MissingDependency(fn, sym)
        targets.add(target)
        targets.update(getDependentFiles(target))

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
