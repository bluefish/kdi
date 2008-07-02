#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/raw.py#1 $
#
# Created 2006/03/29
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

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
