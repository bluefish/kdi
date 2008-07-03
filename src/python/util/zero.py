#!/usr/bin/env python
#
# Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
# Created 2007-01-20
#
# This file is part of KDI.
# 
# KDI is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or any later
# version.
# 
# KDI is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
# License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.

import cStringIO

def zeroEscape(s):
    """zeroEscape(s) --> string with nulls escaped by nulls

    Sequences of nulls will be replaced by a null followed by a count
    of contiguous nulls.  The resulting string is guaranteed never to
    contain two nulls in a row.

    This implementation is nothing like fast.
    """
    out = ''
    z = 0
    for x in s:
        if ord(x) == 0:
            z += 1
            if z == 255:
                out += '\0\xff'
                z = 0
        else:
            if z:
                out += '\0' + chr(z)
                z = 0
            out += x
    if z:
        out += '\0' + chr(z)
    return out

def zeroUnescape(s):
    """zeroUnescape(s) --> string

    Reverse zeroEscape() transformation.  That is:

       zeroUnescape(zeroEscape(x)) == x.

    This implementation is nothing like fast.
    """
    out = ''
    it = iter(s)
    for x in it:
        if ord(x) == 0:
            for x in it:
                out += '\0' * ord(x)
                break
        else:
            out += x
    return out

def zeroEncode(*args):
    """zeroEncode(...) --> string encoding of string arg sequence

    Takes several string arguments and encodes them into a single
    string, such that:

        zeroEncode(a, b, c, ...) < zeroEncode(x, y, z, ...)
           <==>
        (a, b, c, ...) < (x, y, z, ...)
    """
    buf = cStringIO.StringIO()
    for a in args:
        buf.write(zeroEscape(a))
        buf.write('\0\0')
    return buf.getvalue()

def zeroDecode(s):
    """zeroDecode(s) --> tuple of strings

    Reverse zeroEncode() transformation.  That is:

        zeroDecode(zeroEncode(*x)) == tuple(x)
    """
    if not s:
        return tuple()
    if not s.endswith('\0\0'):
        raise ValueError('invalid zeroEncoded string: %r' % s)
    return tuple(map(zeroUnescape, s[:-2].split('\0\0')))

def test():
    """Do a random test of the properties claimed for zero encoded
    tuples.
    """
    from random import choice,randrange
    alph = ''.join([chr(x) for x in range(256)])
    def rstr():
        return ''.join([choice(alph) for x in xrange(randrange(3,12))])
    def rtup():
        return tuple([rstr() for x in xrange(randrange(1,4))])
    tups = [rtup() for x in xrange(5000)]
    encs = [zeroEncode(*x) for x in tups]

    for x,y in zip(tups, encs):
        #print '%r  <>  %r' % (x,y)
        assert zeroDecode(y) == x

    tups.sort()
    encs.sort()

    for x,y in zip(tups, encs):
        #print '%r  <>  %r' % (x,y)
        assert zeroDecode(y) == x

def main():
    print 'testing zero encoding'
    test()
    print '  passed'

if __name__ == '__main__':
    main()
