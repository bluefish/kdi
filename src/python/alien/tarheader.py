#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/tarheader.py#1 $
#
# Created 2006/03/29
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

from alien.vstruct import StructBase
from alien.raw import RawData
from alien.octal import OctalNumber
from alien.timestamp import Timestamp

#----------------------------------------------------------------------------
# TarHeader
#----------------------------------------------------------------------------
class TarHeader(StructBase):
    __format__ = {
        0 : [
        ('fileName', RawData(100)),
        ('fileMode', RawData(8)),
        ('ownerId', OctalNumber(8)),
        ('groupId', OctalNumber(8)),
        ('fileSize', OctalNumber(12)),
        ('modTime', Timestamp(OctalNumber(12))),
        ('checksum', RawData(8)),
        ('typeFlag', RawData(1)),
        ('linkTarget', RawData(100)),
        ('ustarMagic', RawData(6)),
        ('ustarVersion', RawData(2)),
        ('ownerName', RawData(32)),
        ('groupName', RawData(32)),
        ('deviceMajor', OctalNumber(8)),
        ('deviceMinor', OctalNumber(8)),
        ('filePrefix', RawData(155)),
        ('pad', RawData(12)),
        ],
    }


__all__ = ['TarHeader']
