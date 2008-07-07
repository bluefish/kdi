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

from alien.prim import LsbInt,LsbUInt
from alien.vstruct import StructBase
from alien.raw import RawData
from alien.tools import sizeof
from alien.array import FixedArray
from util.xmlfmt import xml

#----------------------------------------------------------------------------
# Offset
#----------------------------------------------------------------------------
def Offset(ttype, tver=None, otype=LsbInt, over=None):
    class Offset(StructBase):
        NULL_OFFSET = 1
        __format__ = {
            0 : [
            ('offset', otype, over),
            ('target', RawData(0)),
            ],
        }

        def isNull(self):
            return self.offset == self.NULL_OFFSET

        def __get_target(self):
            if self.isNull():
                return None
            else:
                return ttype(self._buf, self._base + self.offset, tver)
        target = property(__get_target)

        def __xml__(self):
            if self.isNull():
                return ''
            else:
                return xml(self.target)

    return Offset


#----------------------------------------------------------------------------
# ArrayOffset
#----------------------------------------------------------------------------
def ArrayOffset(ttype, tver=None,
                otype=LsbInt, over=None,
                stype=LsbUInt, sver=None):
    class ArrayOffset(Offset(ttype, tver, otype, over)):
        __format__ = {
            0 : [
            ('length', stype, sver),
            ],
        }

        def __len__(self):
            return self.length

        def __getitem__(self, idx):
            if not -self.length <= idx < self.length:
                raise IndexError('ArrayOffset index out of range: %r' % idx)
            if idx < 0:
                idx += self.length
            off = self._base + self.offset + sizeof(ttype, tver) * idx
            return ttype(self._buf, off, tver)

        def _iteritems(self):
            off = self._base + self.offset
            sz = sizeof(ttype, tver)
            for i in xrange(self.length):
                yield ttype(self._buf, off, tver)
                off += sz

        def __iter__(self):
            return self._iteritems()

        def __get_target(self):
            atype = FixedArray(self.length, ttype, tver)
            return atype(self._buf, self._base + self.offset)
        target = property(__get_target)

        def __xml__(self):
            return ''.join(['<item>%s</item>' % xml(x) for x in self._iteritems()])
        

    return ArrayOffset
