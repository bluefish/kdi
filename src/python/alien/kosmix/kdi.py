#!/usr/bin/env python
#
# Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
# Created 2007-10-20
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

from alien.common import *
from alien.timestamp import NanoTimestamp
from alien.kosmix.record import RecordBase
from alien.kosmix.stringdata import *

__all__ = dir()

class CellKey(StructBase):
    __format__ = {
        0 : [
        ('row', StringOffset),
        ('column', StringOffset),
        ('timestamp', LsbLong),
        ],
    }

class CellData(StructBase):
    __format__ = {
        0 : [
        ('key', CellKey, 0),
        ('value', StringOffset),
        ('__pad', LsbUInt),
        ]
    }

class CellBlock(RecordBase):
    __typecode__ = 'CelB'
    __format__ = {
        0 : [
        ('cells', ArrayOffset(CellData, 0)),
        ]
    }

class IndexEntry(StructBase):
    __format__ = {
        0 : [
        ('startKey', CellKey, 0),
        ('blockOffset', LsbULong),
        ]
    }

class BlockIndex(RecordBase):
    __typecode__ = 'CBIx'
    __format__ = {
        0 : [
        ('blocks', ArrayOffset(IndexEntry, 0)),
        ]
    }

class TableInfo(RecordBase):
    __typecode__ = 'TNfo'
    __format__ = {
        0 : [
        ('indexOffset', LsbULong),
        ]
    }

class LogCell(RecordBase):
    __typecode__ = 'LogC'
    __format__ = {
        0 : [
        ('row', StringOffset),
        ('column', StringOffset),
        ('timestamp', LsbLong),
        ('value', StringOffset),
        ]
    }

# Export magic...
__all__ = [_x for _x in dir() if _x not in __all__ and not _x.startswith('_')]
