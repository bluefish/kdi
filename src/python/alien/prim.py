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

import struct as _struct

#----------------------------------------------------------------------------
# PrimType
#----------------------------------------------------------------------------
class PrimType(type):
    def __init__(self, name, bases, body):
        type.__init__(self, name, bases, body)
        if '__format__' in body:
            self.__size__ = _struct.calcsize(self.__format__)


#----------------------------------------------------------------------------
# PrimBase
#----------------------------------------------------------------------------
class PrimBase(object):
    __metaclass__ = PrimType

    def __new__(klas, buffer, start=0, version=None):
        return _struct.unpack(klas.__format__,
                              buffer[start:start+klas.__size__])[0]

    def _sizeof(cls, ver=None):
        return cls.__size__
    _sizeof = classmethod(_sizeof)


#----------------------------------------------------------------------------
# Primitive types
#----------------------------------------------------------------------------
class Byte     (PrimBase): __format__ = 'b'
class UByte    (PrimBase): __format__ = 'B'

class MsbShort (PrimBase): __format__ = '>h'
class MsbInt   (PrimBase): __format__ = '>i'
class MsbLong  (PrimBase): __format__ = '>q'
class MsbUShort(PrimBase): __format__ = '>H'
class MsbUInt  (PrimBase): __format__ = '>I'
class MsbULong (PrimBase): __format__ = '>Q'
class MsbFloat (PrimBase): __format__ = '>f'
class MsbDouble(PrimBase): __format__ = '>d'

class LsbShort (PrimBase): __format__ = '<h'
class LsbInt   (PrimBase): __format__ = '<i'
class LsbLong  (PrimBase): __format__ = '<q'
class LsbUShort(PrimBase): __format__ = '<H'
class LsbUInt  (PrimBase): __format__ = '<I'
class LsbULong (PrimBase): __format__ = '<Q'
class LsbFloat (PrimBase): __format__ = '<f'
class LsbDouble(PrimBase): __format__ = '<d'

class NativeShort (PrimBase): __format__ = '=h'
class NativeInt   (PrimBase): __format__ = '=i'
class NativeLong  (PrimBase): __format__ = '=q'
class NativeUShort(PrimBase): __format__ = '=H'
class NativeUInt  (PrimBase): __format__ = '=I'
class NativeULong (PrimBase): __format__ = '=Q'
class NativeFloat (PrimBase): __format__ = '=f'
class NativeDouble(PrimBase): __format__ = '=d'


#----------------------------------------------------------------------------
# Java aliases
#----------------------------------------------------------------------------
JavaByte   = Byte
JavaShort  = MsbShort
JavaInt    = MsbInt
JavaLong   = MsbLong
JavaFloat  = MsbFloat
JavaDouble = MsbDouble
