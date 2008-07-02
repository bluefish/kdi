#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/kosmix/kdi.py#1 $
#
# Created 2007/10/20
#
# Copyright 2007 Kosmix Corporation.  All rights reserved.
# Kosmix PROPRIETARY and CONFIDENTIAL.

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
