#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/kosmix/record.py#1 $
#
# Created 2006/03/29
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

from alien.vstruct import StructType, StructBase
from alien.raw import RawData
from alien.prim import *


#----------------------------------------------------------------------------
# TypeCode
#----------------------------------------------------------------------------
class TypeCode(PrimBase):
    __format__ = '4s'


#----------------------------------------------------------------------------
# RecordType
#----------------------------------------------------------------------------
class RecordType(StructType):
    __typecode_registry__ = {}
    def __init__(self, name, bases, body):
        StructType.__init__(self, name, bases, body)
        if '__typecode__' in body:
            tc = self.__typecode__
            if tc in self.__typecode_registry__:
                raise ValueError(('Class %r cannot register typecode %r: '+
                                  'already registered by class %r') %
                                 (name, tc, __typecode_registry__[tc].__name__))
            self.__typecode_registry__[tc] = self

    def getTypecodeClass(cls, typecode):
        return cls.__typecode_registry__.get(typecode, None)
    getTypecodeClass = classmethod(getTypecodeClass)


#----------------------------------------------------------------------------
# RecordBase
#----------------------------------------------------------------------------
class RecordBase(StructBase):
    __metaclass__ = RecordType


#----------------------------------------------------------------------------
# Record
#----------------------------------------------------------------------------
class Record(StructBase):
    __format__ = {

        # Sirius record format
        0 : [ 
        ('typecode', TypeCode),
        ('version', JavaShort),
        ('length', JavaInt),
        ('data', RawData(0)),
        ],

        # Vega record format
        1 : [
        ('length', LsbInt),
        ('typecode', TypeCode),
        ('version', Byte),
        ('flags', Byte),
        ('data', RawData(0)),
        ],
    }

    def _sizeof(cls, ver=None):
        baseSize = StructBase._basesizeof(cls, ver)
        return lambda obj: baseSize + obj.length
    _sizeof = classmethod(_sizeof)

    def __get_data(self):
        rtype = RecordType.getTypecodeClass(self.typecode)
        if rtype is None:
            rtype = RawData(self.length)
            rver = None
        else:
            rver = self.version
        off = self._offsetof('data')
        return rtype(self._buf, self._base + off, rver)
    data = property(__get_data)

    def getRaw(self):
        return self._getraw()


#----------------------------------------------------------------------------
# util
#----------------------------------------------------------------------------
def iter_records_string(s, rver=None):
    off = 0
    end = len(s)
    while off < end:
        if end - off < StructBase._basesizeof(Record, rver):
            raise IOError('Partial record header')
        r = Record(s, off, rver)
        sz = r._getsize()
        if end - off < sz:
            raise IOError('Partial record data')
        off += sz
        yield r

def iter_records(f, rver=None):
    off = 0
    bufsz = 1 << 20
    buf = f.read(bufsz)
    end = len(buf)
    while True:
        if off == end:
            buf = f.read(bufsz)
            if not buf:
                return
            off = 0
            end = len(buf)
        
        if end - off < StructBase._basesizeof(Record, rver):
            x = f.read(bufsz)
            if x == '':
                raise IOError('Unexpected EOF')
            buf = buf[off:] + x
            off = 0
            end = len(buf)
            continue
        
        r = Record(buf, off, rver)
        sz = r._getsize()
        if end - off < sz:
            x = f.read(bufsz)
            if x == '':
                raise IOError('Unexpected EOF')
            buf = buf[off:] + x
            off = 0
            end = len(buf)
            continue

        off += sz
        yield r

class FileRecordIterator(object):
    def __init__(self, f, rver=None):
        self.f = f
        self.rver = None

    def __iter__(self):
        return iter_records(self.f, self.rver)

    def close(self):
        self.f.close()
