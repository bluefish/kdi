#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/vtype.py#1 $
#
# Created 2007/08/01
#
# Copyright 2007 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

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
