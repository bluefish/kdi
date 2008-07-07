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


#----------------------------------------------------------------------------
# Config
#----------------------------------------------------------------------------
class Config(object):
    def __init__(self, fn, **kwDef):
        self.fn = fn
        self.sym = {}
        self.sym.update(kwDef)
        execfile(fn, self.sym)

    def __getattr__(self, k):
        try:
            return self.sym[k]
        except KeyError, ex:
            raise AttributeError('Config %r has no key %r' % (self.fn, k))

    def __iter__(self):
        return self.sym.iteritems()

    def __str__(self):
        return '\n'.join(['%s = %r' % (k,v) for k,v in self])
