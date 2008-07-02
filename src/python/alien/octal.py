#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/octal.py#1 $
#
# Created 2006/03/29
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.


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
