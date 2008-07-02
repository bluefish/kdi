#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/vint.py#1 $
#
# Created 2006/03/29
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

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
