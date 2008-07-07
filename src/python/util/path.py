#!/usr/bin/env python
#
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-03-29
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

def dirIter(basePath, yieldFilter=None, recurseFilter=None):
    for fn in os.listdir(basePath):
        p = os.path.join(basePath, fn)
        if not yieldFilter or yieldFilter(p):
            yield p
        if os.path.isdir(p) and (not recurseFilter or recurseFilter(p)):
            for x in dirIter(p, yieldFilter, recurseFilter):
                yield x

def iterFilesWithExtension(path, ext):
    return dirIter(path, lambda x: x.endswith(ext) and os.path.isfile(x))

def relpath(path, basedir):
    path = os.path.normpath(os.path.abspath(path))
    base = os.path.normpath(os.path.abspath(basedir))
    while base.endswith(os.sep):
        base = base[:-1]
    #while base and not os.path.isdir(base):
    #    base = os.path.dirname(base)
    path = path.split(os.sep)
    base = base.split(os.sep)
    while base and path and base[0] == path[0]:
        base.pop(0)
        path.pop(0)
    return os.sep.join(['..']*len(base) + path)

def replaceExt(path, ext):
    return os.path.splitext(path)[0] + ext

def pathFind(x, pathVar='PATH'):
    path = os.environ.get(pathVar, '').split(os.pathsep)
    x = x.split(os.pathsep)
    for d in path:
        p = os.path.join(d, *x)
        if os.path.exists(p):
            return p
    raise RuntimeError('%r not found in $%s' % x, pathVar)
