#!/usr/bin/env python
#
# $Id: btree.py $
#
# Created 2008/07/01
#
# Copyright 2008 Kosmix Corporation.  All rights reserved.
# Kosmix PROPRIETARY and CONFIDENTIAL.

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
