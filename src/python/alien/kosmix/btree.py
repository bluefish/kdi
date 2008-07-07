#!/usr/bin/env python
#
# Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
# Created 2008-07-01
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

from alien.common import *

__all__ = dir()

#----------------------------------------------------------------------------
# Stream Index Types
#----------------------------------------------------------------------------
class BTreeHeader(StructBase):
    __format__ = {
        0 : [
        ('magic', LsbUInt),
        ('version', LsbUShort),
        ('trailerSize', LsbUShort),
        ('keyType', LsbUInt),
        ('keyVersion', LsbUShort),
        ('posType', UByte),
        ('pad', UByte),
        ],
    }

def BTreeTrailer(ptype=LsbULong, pver=None):
    class BTreeTrailer(StructBase):
        __format__ = {
            0 : [
            ('rootPos', ptype, pver),
            ('maxNodeSize', LsbUInt),
            ('treeHeight', LsbUInt),
            ],
        }
    return BTreeTrailer

def BTreeItem(ktype, kver=None, ptype=LsbULong, pver=None):
    class BTreeItem(StructBase):
        __format__ = {
             0 : [
             ('key', ktype, kver),
             ('pos', ptype, pver),
             ],
        }
    return BTreeItem

def BTreeNode(ktype, kver=None, ptype=LsbULong, pver=None):
    class BTreeNode(StructBase):
        __format__ = {
            0 : [
            ('firstPos', ptype, pver),
            ('keys', ArrayOffset(BTreeItem(ktype,kver,ptype,pver))),
            ],
        }
    return BTreeNode

# Export magic...
__all__ = [_x for _x in dir() if _x not in __all__ and not _x.startswith('_')]
