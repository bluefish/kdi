#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/util/config.py#1 $
#
# Created 2006/03/29
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.


#----------------------------------------------------------------------------
# Config
#----------------------------------------------------------------------------
class Config(object):
    def __init__(self, fn, **kwDef):
        self.fn = fn
        self.sym = {}
        self.sym.update(kwDef)
        execfile(fn, self.sym)

    def __getattr__(self, k):
        try:
            return self.sym[k]
        except KeyError, ex:
            raise AttributeError('Config %r has no key %r' % (self.fn, k))

    def __iter__(self):
        return self.sym.iteritems()

    def __str__(self):
        return '\n'.join(['%s = %r' % (k,v) for k,v in self])
