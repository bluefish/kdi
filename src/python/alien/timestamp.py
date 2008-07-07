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

import time

#----------------------------------------------------------------------------
# Timestamp
#----------------------------------------------------------------------------
def Timestamp(dtype, dver=None, scaleToSeconds=1.0):

    class Timestamp(object):
        def __init__(self, buf, offset=0, ver=None):
            self._val = dtype(buf, offset, dver)

        def __str__(self):
            return time.ctime(self._val * scaleToSeconds).strip()

        def __repr__(self):
            return repr(self._val)

        def _sizeof(cls, ver=None):
            return dtype._sizeof(dver)
        _sizeof = classmethod(_sizeof)

        def __getRaw(self):
            return self._val
        raw = property(__getRaw)

    return Timestamp

#----------------------------------------------------------------------------
# MsTimestamp
#----------------------------------------------------------------------------
def MsTimestamp(dtype, dver=None):
    return Timestamp(dtype, dver, scaleToSeconds=1e-3)

#----------------------------------------------------------------------------
# NanoTimestamp
#----------------------------------------------------------------------------
def NanoTimestamp(dtype, dver=None):
    return Timestamp(dtype, dver, scaleToSeconds=1e-9)
