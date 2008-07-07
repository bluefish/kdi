#!/usr/bin/env python
#
# Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
# Created 2007-08-01
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

import types

__all__ = dir()

#----------------------------------------------------------------------------
# VariableType
#----------------------------------------------------------------------------
class VariableType(object):
    """VariableType
    """

    __is_variable_type__ = True
    
    def __init__(self, predicate, trueType, falseType):
        self.predicate = predicate
        self.trueType = trueType
        self.falseType = falseType

    def __call__(self, obj):
        """__call__(obj) --> (type, version)

        Determine a type and version pair that depends somehow on the
        given object.
        """
        # Determine predicate value
        if callable(self.predicate):
            predicateIsTrue = self.predicate(obj)
        else:
            predicateIsTrue = self.predicate

        # Get return type based on predicate
        if predicateIsTrue:
            if isinstance(self.trueType, types.FunctionType):
                rtype = self.trueType(obj)
            else:
                rtype = self.trueType
        else:
            if isinstance(self.falseType, types.FunctionType):
                rtype = self.falseType(obj)
            else:
                rtype = self.falseType

        # Return (type, version) pair
        if isinstance(rtype, tuple):
            assert len(rtype) == 2
            return rtype
        else:
            return (rtype, None)

    def _sizeof(self, ver):
        def getAttrSize(obj, attrObj):
            rtype,rver = self(obj)
            rsize = rtype._sizeof(rver)
            if callable(rsize):
                return rsize(attrObj)
            else:
                return rsize
        return getAttrSize


def chooseType(predicate, trueType, falseType):
    assert callable(predicate)
    return VariableType(predicate, trueType, falseType)

def varType(vtype):
    assert callable(vtype)
    return VariableType(True, vtype, (None,None))

# Export magic...
__all__ = [_x for _x in dir() if _x not in __all__ and not _x.startswith('_')]
