#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/kosmix/stringdata.py#1 $
#
# Created 2007/10/20
#
# Copyright 2007 Kosmix Corporation.  All rights reserved.
# Kosmix PROPRIETARY and CONFIDENTIAL.

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
