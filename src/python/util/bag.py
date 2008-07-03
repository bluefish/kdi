#!/usr/bin/env python
#
# Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
# Created 2005-10-05
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

class Bag(object):
    def __init__(self, **kw):
        self.__dict__.update(kw)
    def __getitem__(self, k):
        return self.__dict__[k]
    def __setitem__(self, k, v):
        self.__dict__[k] = v
    def __iter__(self):
        return self.__dict__.iteritems()
    def __repr__(self):
        return 'Bag('+','.join(['%s=%r'%(k,v) for k,v in self if not k.startswith('_')])+')'
