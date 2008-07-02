#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/util/bag.py#1 $
#
# Created 2005/10/05
#
# Copyright 2005 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

class Bag(object):
    def __init__(self, **kw):
        self.__dict__.update(kw)
    def __getitem__(self, k):
        return self.__dict__[k]
    def __setitem__(self, k, v):
        self.__dict__[k] = v
    def __iter__(self):
        return self.__dict__.iteritems()
    def __repr__(self):
        return 'Bag('+','.join(['%s=%r'%(k,v) for k,v in self if not k.startswith('_')])+')'
