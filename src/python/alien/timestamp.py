#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/timestamp.py#1 $
#
# Created 2006/03/29
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

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
