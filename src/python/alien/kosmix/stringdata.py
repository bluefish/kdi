#!/usr/bin/env python
#
# Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
# Created 2007-10-20
# 
# This file is part of Alien.
# 
# Alien is free software; you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or any later version.
# 
# Alien is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from alien.common import *
from alien.gzdata import GZippedData
from util.xmlfmt import xml

__all__ = dir()

class StringData(StructBase):
    __nobrowse__ = 1
    __format__ = {
        0 : [
        ('len', LsbInt),
        ('data', RawData(1)),
        ],
    }
    def __get_data(self):
        rtype = RawData(self.len)
        return rtype(self._buf, self._base + self._offsetof('data'))
    data = property(__get_data)

    def _sizeof(cls, ver=None):
        return lambda x: LsbInt._sizeof() + x.len
    _sizeof = classmethod(_sizeof)

    def __str__(self):
        return str(self.data)

    def __xml__(self):
        return xml(self.data)

class GzStringData(GZippedData(RawData(None), None, LsbInt)):
    def __str__(self):
        return str(self.unzipped)

class StringOffset(Offset(StringData)):
    __nobrowse__ = 1
    def __str__(self):
        return str(self.target)

class GzStringOffset(Offset(GzStringData)):
    __nobrowse__ = 1
    def __str__(self):
        return str(self.target)


# Export magic...
__all__ = [_x for _x in dir() if _x not in __all__ and not _x.startswith('_')]
