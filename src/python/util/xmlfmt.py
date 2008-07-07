#!/usr/bin/env python
#
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-04-02
# 
# This file is part of the util library.
# 
# The util library is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or any later
# version.
# 
# The util library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import util.iter
import re

def iterPreorder(node):
    yield node
    for x in node.childNodes:
        for y in iterPreorder(x):
            yield y

def getText(node):
    return ''.join([str(x.data) for x in iterPreorder(node) if x.nodeType == x.TEXT_NODE])

def passUtf8(x, maxCode=4):
    """passUtf8(x [,maxCode=4]) -> sequence of valid Utf8 octets"""
    
    # UTF8-octets = *( UTF8-char )
    # UTF8-char   = UTF8-1 / UTF8-2 / UTF8-3 / UTF8-4
    # UTF8-1      = %x00-7F
    # UTF8-2      = %xC2-DF UTF8-tail
    #
    # UTF8-3      = %xE0 %xA0-BF UTF8-tail / %xE1-EC 2( UTF8-tail ) /
    #               %xED %x80-9F UTF8-tail / %xEE-EF 2( UTF8-tail )
    # UTF8-4      = %xF0 %x90-BF 2( UTF8-tail ) / %xF1-F3 3( UTF8-tail ) /
    #               %xF4 %x80-8F 2( UTF8-tail )
    # UTF8-tail   = %x80-BF

    if maxCode < 1 or maxCode > 4:
        raise ValueError('bad value for maxCode: %r' % maxCode)

    it = util.iter.pushback_iter(x)
    for a in it:
        # UTF8-1
        if a >= '\x00' and a <= '\x7f':
            yield a
        elif maxCode == 1:
            continue
        
        # UTF8-2
        elif a >= '\xc2' and a <= '\xdf':
            b = it.next()
            if b >= '\x80' and b <= '\xbf':
                yield a
                yield b
            else:
                it.push(b)
        elif maxCode == 2:
            continue

        # UTF8-3
        elif a == '\xe0':
            b = it.next()
            if b >= '\xa0' and b <= '\xbf':
                c = it.next()
                if c >= '\x80' and c <= '\xbf':
                    yield a
                    yield b
                    yield c
                else:
                    it.push(c)
            else:
                it.push(b)
        elif a == '\xed':
            b = it.next()
            if b >= '\x80' and b <= '\x9f':
                c = it.next()
                if c >= '\x80' and c <= '\xbf':
                    yield a
                    yield b
                    yield c
                else:
                    it.push(c)
            else:
                it.push(b)
        elif a >= '\xe0' and a <= '\xef':
            b = it.next()
            if b >= '\x80' and b <= '\xbf':
                c = it.next()
                if c >= '\x80' and c <= '\xbf':
                    yield a
                    yield b
                    yield c
                else:
                    it.push(c)
            else:
                it.push(b)
        elif maxCode == 3:
            continue

        # UTF8-4
        elif a == '\xf0':
            b = it.next()
            if b >= '\x90' and b <= '\xbf':
                c = it.next()
                if c >= '\x80' and c <= '\xbf':
                    d = it.next()
                    if d >= '\x80' and d <= '\xbf':
                        yield a
                        yield b
                        yield c
                        yield d
                    else:
                        it.push(d)
                else:
                    it.push(c)
            else:
                it.push(b)
        elif a == '\xf4':
            b = it.next()
            if b >= '\x80' and b <= '\x8f':
                c = it.next()
                if c >= '\x80' and c <= '\xbf':
                    d = it.next()
                    if d >= '\x80' and d <= '\xbf':
                        yield a
                        yield b
                        yield c
                        yield d
                    else:
                        it.push(d)
                else:
                    it.push(c)
            else:
                it.push(b)
        elif a >= '\xf1' and a <= '\xf3':
            b = it.next()
            if b >= '\x80' and b <= '\xbf':
                c = it.next()
                if c >= '\x80' and c <= '\xbf':
                    d = it.next()
                    if d >= '\x80' and d <= '\xbf':
                        yield a
                        yield b
                        yield c
                        yield d
                    else:
                        it.push(d)
                else:
                    it.push(c)
            else:
                it.push(b)

def filterUtf8(x,maxCode=4):
    """filterUtf8(x [,maxCode=4]) -> string x with all non-UTF8 characters removed"""
    return ''.join(passUtf8(x, maxCode))

def escape(x):
    """escape(x) -> XML character data string

    Replaces occurances of '&' with '&amp;', '<' with '&lt;', and '>'
    with '&gt;'.  Strictly speaking, we only need to escape
    greater-than if it occurs as part of the sequence ']]>'.  This
    function just escapes them all anyway.
    """
    return x.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

def qescape(x):
    """escape(x) -> XML single-quote attribute data string

    Replaces occurances of '&' with '&amp;', '<' with '&lt;', and
    single quotes with '&apos;'.
    """    
    return  x.replace('&','&amp;').replace('<','&lt;').replace("'", '&apos;')

def qqescape(x):
    """escape(x) -> XML double-quote attribute data string

    Replaces occurances of '&' with '&amp;', '<' with '&lt;', and
    double quotes with '&quot;'.
    """    
    return  x.replace('&','&amp;').replace('<','&lt;').replace('"', '&quot;')

def cdata(x):
    if ']]>' in x:
        raise ValueError('CDATA cannot contain "]]>"')
    return '<![CDATA[%s]]>' % x

_rId = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*')
def identifier(x):
    name = str(x)
    if not _rId.match(name):
        raise ValueError('invalid identifier: %r' % x)
    return name

def xml(x):
    if hasattr(x, '__xml__'):
        return x.__xml__()
    elif isinstance(x, dict):
        return ''.join([
            '<%s>%s</%s>' % (identifier(k), xml(v), identifier(k))
            for k,v in x.iteritems()])
    elif isinstance(x, (list,tuple)):
        return ''.join([
            '<i%d>%s</i%d>' % (i,xml(v),i) for i,v in enumerate(x)])
    else:
        return escape(filterUtf8(str(x)))

