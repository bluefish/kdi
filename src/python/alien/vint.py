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

"""Variable length integer types"""

#----------------------------------------------------------------------------
# VInt
#----------------------------------------------------------------------------
class VInt(object):
    def __new__(cls, buf, start=0, ver=None):
        value = 0
	shifts = 0
        b = ord(buf[start])
        start += 1
        while (b & 0x80) != 0:
            value |= (b & 0x7f) << shifts
            shifts += 7
            b = ord(buf[start])
            start += 1
        value |= (b & 0x7f) << shifts
        return value

    def calcsize(n):
        if n < 0:            return 5
        elif n < (1 << 7):   return 1
        elif n < (1 << 14):  return 2
        elif n < (1 << 21):  return 3
        elif n < (1 << 28):  return 4
        else:                return 5
    calcsize = staticmethod(calcsize)

    def _sizeof(cls, ver=None):
        return cls.calcsize
    _sizeof = classmethod(_sizeof)


#----------------------------------------------------------------------------
# VLong
#----------------------------------------------------------------------------
class VLong(object):
    def __new__(cls, buf, start=0, ver=None):
        value = 0
	shifts = 0
        b = ord(buf[start])
        start += 1
        while (b & 0x80) != 0:
            value |= (b & 0x7f) << shifts
            shifts += 7
            b = ord(buf[start])
            start += 1
        value |= (b & 0x7f) << shifts
        return value

    def calcsize(n):
        if n < 0:            return 10
        elif n < (1 << 7):   return 1
        elif n < (1 << 14):  return 2
        elif n < (1 << 21):  return 3
        elif n < (1 << 28):  return 4
        elif n < (1 << 35):  return 5
        elif n < (1 << 42):  return 6
        elif n < (1 << 49):  return 7
        elif n < (1 << 56):  return 8
        elif n < (1 << 63):  return 9
        else:                return 10
    calcsize = staticmethod(calcsize)

    def _sizeof(cls, ver=None):
        return cls.calcsize
    _sizeof = classmethod(_sizeof)
