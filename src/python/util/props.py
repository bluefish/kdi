#!/usr/bin/env python
#
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-09-08
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

import re

_bracket = re.compile(r'[\[\]]')
_kvsep = (re.compile(r'\s+'), re.compile(r'\s*:\s*'), re.compile(r'\s*=\s*'))
_var = re.compile(r'\$\(([^\):]+)(\:[^\)]+)?\)')


#----------------------------------------------------------------------------
# PropertyError
#----------------------------------------------------------------------------
class PropertyError(RuntimeError):
    pass


#----------------------------------------------------------------------------
# expandBits
#----------------------------------------------------------------------------
def expandBits(x):
    """expandBits(x) --> enumerate range sequence

    Takes a comma separated list emits all things in the list.  Some
    items in the comma list may be ranges of the form 'int_a..int_b'
    or 'char_a..char_b'.  These will be expanded to the full range,
    including both endpoints.  In the case of the integer range, the
    results will be zero padded to the length of the shortest
    endpoint.  The expansion is applied recursively, and may also
    contain range sequences.
    """
    for bitl in x.split(','):
        for bit in expandList(bitl):
            # Shortcut for non-range bits
            if '..' not in bit:
                yield bit
                continue
                
            # Attempt to expand as a range
            try:
                lo,hi = bit.split('..')
            except ValueError:
                # Didn't work
                yield bit
                continue

            # Try it as an integer range
            try:
                lo_num = int(lo)
                hi_num = int(hi)
                sz = min(len(lo), len(hi))
                for i in range(lo_num, hi_num + 1):
                    yield '%%0%dd' % sz % i
                continue
            except ValueError:
                pass

            # Try it as a character range
            try:
                lo_chr = ord(lo)
                hi_chr = ord(hi)
                for i in range(lo_chr, hi_chr + 1):
                    yield chr(i)
                continue
            except TypeError:
                pass

            # Maybe it's not a range after all
            yield bit


#----------------------------------------------------------------------------
# expandList
#----------------------------------------------------------------------------
def expandList(x):
    """expandList(x) --> expanded sequence

    Recursively expand '[...]' comprehensions embedded in the input
    string.  Generates the sequence of expanded strings.  When
    multiple comprehensions are included, the sequence will be the
    Cartesian product of all expansions, iterating left to right.
    """
    start = None
    count = 0
    emitted = 0
    for m in _bracket.finditer(x):
        if m.group() == '[':
            if count == 0:
                start = m.start()
            count += 1
        elif m.group() == ']':
            if count == 1:
                emitted += 1
                bgn = x[:start]
                mid = x[start+1:m.start()]
                end = x[m.end():]
                for rep in expandBits(mid):
                    for xx in expandList(bgn+rep+end):
                        yield xx
            if count > 0:
                count -= 1

    if not emitted:
        yield x


#----------------------------------------------------------------------------
# iterItems
#----------------------------------------------------------------------------
def iterItems(f):
    """iterItems(f) --> sequence of key,value tuples

    Reads f as a sequence of lines, and generates a sequence of key
    value pairs.  Empty lines are ignored, as are comment lines where
    the first non-whitespace character is a '#' or '!'.  Keys may be
    separated from values by whitespace, '=', or ':'.  Long values may
    be continued on multiple lines by ending the line with a
    backslash.
    """
    for line in f:
        line = line.strip()
        if not line or line.startswith('#') or line.startswith('!'):
            continue

        # Find first valid K/V separator
        m = None
        for sep in _kvsep:
            mm = sep.search(line)
            if mm and (not m or mm.start() < m.start() or \
                       (mm.start() == m.start() and mm.end() > m.end())):
                m = mm

        # Did we get a separator?
        if m:
            k = line[:m.start()].rstrip()
            v = line[m.end():].lstrip()
        else:
            k = line
            v = ''

        while v.endswith('\\'):
            for line in f:
                v = v[:-1] + line.strip()
                break
            else:
                v = v[:-1]
                break

        yield k,v


#----------------------------------------------------------------------------
# Properties
#----------------------------------------------------------------------------
class Properties(object):
    NO_DEFAULT = object()
    
    def __init__(self, fn=None, vars=None):
        if vars is not None:
            self.vars = vars
        else:
            self.vars = {}
        self.props = {}
        self.keys = []
        if fn is not None:
            self.load(fn)

    def set(self, key, val):
        key = str(key)
        if key not in self.props:
            self.keys.append(key)
        self.props[key] = str(val)

    def setSeq(self, kvSeq):
        for k,v in kvSeq:
            self.set(k,v)

    def load(self, fn):
        """load(f) --> None

        Load from a file.  Given argument may be a file-like object or
        a file name.
        """
        if isinstance(fn, (str, unicode)):
            f = file(fn)
            self.setSeq(iterItems(f))
            f.close()
        else:
            self.setSeq(iterItems(fn))

    def getVar(self, key, dflt=NO_DEFAULT):
        """getVar(key, default) --> variable value
        """
        try:
            val = self.props[key]
        except KeyError:
            try:
                val = self.vars[key]
            except KeyError:
                if dflt is not self.NO_DEFAULT:
                    val = dflt
                else:
                    raise PropertyError('undefined variable %r' % key)
        return self.expandVars(val)

    def expandVars(self, val):
        """expandVars(val) --> val with variables substituted
        """
        def repl(m):
            key, dflt = m.group(1,2)
            if dflt:
                # strip leading colon
                dflt = dflt[1:]
            else:
                dflt = self.NO_DEFAULT
            return self.getVar(key, dflt)
        return _var.sub(repl, val)

    def get(self, key, dflt=NO_DEFAULT, expandVars=True):
        """get(key, default=NO_DEFAULT, expandVars=True) --> property value
        """
        try:
            val = self.props[key]
        except KeyError:
            if dflt is not self.NO_DEFAULT:
                val = dflt
            else:
                raise PropertyError('undefined property %r' % key)
        if expandVars and isinstance(val, (str, unicode)):
            val = self.expandVars(val)
        return val

    def getSequence(self, key, dflt=NO_DEFAULT, sep=None,
                    expandVars=True, omitEmpty=True):
        """getSequence(key [, ...]) --> sequence of property values

        Expand a property value into a sequence.
        """
        val = self.get(key, dflt, expandVars)
        for item in val.split(sep):
            for subitem in expandList(item):
                if not subitem and omitEmpty:
                    continue
                yield subitem

    def getList(self, key, dflt=NO_DEFAULT, sep=None,
                expandVars=True, omitEmpty=True):
        """getList(key [, ...]) --> list of property values

        Expand a property value into a list.
        """
        return list(self.getSequence(key, dflt, sep, expandVars, omitEmpty))

    def __iter__(self):
        return iter(self.keys)

    def __contains__(self, key):
        return self.props.contains(key)

    def dump(self, out, keys=None, valuesOnly=False, noSpace=False):
        if keys is None:
            keys = self.keys
        if not keys:
            return
        if not valuesOnly and not noSpace:
            keyFmt = '%%-%ds' % (max([len(k) for k in keys]))
        for key in keys:
            val = ' '.join(self.getList(key))
            if valuesOnly:
                print >>out, val
            elif noSpace:
                print >>out, '%s=%s' % (key, val)
            else:
                print >>out, keyFmt % key, '=', val



#----------------------------------------------------------------------------
# main
#----------------------------------------------------------------------------
def main():
    import optparse
    import sys

    op = optparse.OptionParser('%prog [options] <config_file>')
    op.add_option('-k','--key', action='append',
                  help='Property file to load')
    op.add_option('-e','--env', action='store_true',
                  help='Use environment variables')
    op.add_option('-v','--value', action='store_true',
                  help='Output value only')
    op.add_option('','--nospace', action='store_true',
                  help='Omit spaces between key and value')

    opt,args = op.parse_args()

    vars = None
    if opt.env:
        import os
        vars = os.environ

    for fn in args:
        if len(args) > 1:
            print '# Source: %s' % fn

        props = Properties(fn, vars)
        try:
            props.dump(sys.stdout, opt.key or None, opt.value, opt.nospace)
        except PropertyError, ex:
            print >>sys.stderr, ' '.join(map(str,ex))
            sys.exit(1)

if __name__ == '__main__':
    main()
