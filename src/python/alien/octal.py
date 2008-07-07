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
# OctalNumber
#----------------------------------------------------------------------------
def OctalNumber(sz):
    class OctalNumber(object):
        __size__ = sz

        def __new__(cls, buf, offset=0, ver=None):
            x = buf[offset : offset + cls.__size__]
            if len(x) > 0 and x[0] == '\x00':
                return 0
            else:
                return int(x, 8)

        def _sizeof(cls, ver=None):
            return cls.__size__
        _sizeof = classmethod(_sizeof)

    return OctalNumber
