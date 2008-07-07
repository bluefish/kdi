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
