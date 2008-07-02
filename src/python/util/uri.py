#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/util/uri.py#1 $
#
# Created 2006/11/30
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

import re

_rUri = re.compile(r'^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?')

class Uri(object):
    def __init__(self, s):
        m = _rUri.match(s)
        if not m:
            raise ValueError('error parsing Uri: %r', s)

        self.scheme       = m.group(2) or ''
        self.authority    = m.group(4) or ''
        self.path         = m.group(5)
        self.query        = m.group(7) or ''
        self.fragment     = m.group(9) or ''
        self.schemeDef    = bool(m.group(1))
        self.authorityDef = bool(m.group(3))
        self.queryDef     = bool(m.group(6))
        self.fragmentDef  = bool(m.group(8))

    def __str__(self):
        r = ''
        if self.schemeDef:
            r += self.scheme + ':'
        if self.authorityDef:
            r += '//' + self.authority
        r += self.path
        if self.queryDef:
            r += '?' + self.query
        if self.fragmentDef:
            r += '#' + self.fragment
        return r

    def __repr__(self):
        return 'Uri(%s)' % self.__str__()

    def schemeDefined(self):
        return self.schemeDef

    def authorityDefined(self):
        return self.authorityDef

    def queryDefined(self):
        return self.queryDef

    def fragmentDefined(self):
        return self.fragmentDef

    def dump(self):
        print 'uri:', self.uri
        if self.schemeDefined():
            print '     scheme:', self.scheme
        if self.authorityDefined():
            print '  authority:', self.authority
        print '       path:', self.path
        if self.queryDefined():
            print '      query:', self.query
        if self.fragmentDefined():
            print '   fragment:', self.fragment

def main():
    import sys
    for x in sys.argv[1:]:
        Uri(x).dump()
        print

if __name__ == '__main__':
    main()
