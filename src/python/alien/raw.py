#!/usr/bin/env python
#
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-03-29
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

#----------------------------------------------------------------------------
# RawData
#----------------------------------------------------------------------------
def RawData(sz):
    class RawData(object):
        __size__ = sz

        def __init__(self, buf, offset=0, ver=None):
            self._buf = buf
            self._off = offset

        def __repr__(self):
            return 'RawData(%s)' % self

        def __get_raw(self):
            if self.__size__ is None:
                return self._buf[self._off:]
            else:
                return self._buf[self._off : self._off + self.__size__]
        raw = property(__get_raw)

        def __len__(self):
            if self.__size__ is None:
                return len(self._buf) - self._off
            else:
                return self.__size__

        def __str__(self):
            return repr(self.raw)

        def _sizeof(cls, ver=None):
            if cls.__size__ is None:
                return lambda x: len(x._buf) - x._off
            else:
                return cls.__size__
        _sizeof = classmethod(_sizeof)

    return RawData

#----------------------------------------------------------------------------
# HexData
#----------------------------------------------------------------------------
def HexData(sz):
    class HexData(object):
        __size__ = sz 

        def __init__(self, buf, offset=0, ver=None):
            self._buf = buf
            self._base = offset

        def __get_raw(self):
            return self._buf[self._base : self._base + self.__size__]
        raw = property(__get_raw)

        def __str__(self):
            return ''.join(['%02x' % ord(x) for x in self.raw])

        def __repr__(self):
            return repr(self.raw)

        def _sizeof(cls, ver=None):
            return cls.__size__
        _sizeof = classmethod(_sizeof)

    return HexData
