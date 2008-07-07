#!/usr/bin/env python
#
# Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
# Created 2008-06-02
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


#----------------------------------------------------------------------------
# stringToFloat
#----------------------------------------------------------------------------
def stringToFloat(s, min='\0', max='\xff'):
    """stringToFloat(s) --> float in [0,1)

    A string can be thought of as a rational number:
       indexOf(s) / (256 ^ len(s))
    This function computes a float approximation of that number.
    """
    min = ord(min)
    max = ord(max)
    if max < min:
        raise ValueError('max less than min')
    range = max - min + 1
    n = 0.0
    for c in s:
        c = ord(c)
        if c < min:
            c = min
        elif c > max:
            c = max
        n = n * range + c - min
    f = n / (range ** len(s))
    return f

#----------------------------------------------------------------------------
# floatToString
#----------------------------------------------------------------------------
def floatToString(f, min='\0', max='\xff'):
    if f < 0.0 or f >= 1.0:
        raise ValueError('string float must be 0.0 <= f < 1.0, got: %r' % f)
    origF = f
    min = ord(min)
    max = ord(max)
    if max < min:
        raise ValueError('max less than min')
    range = max - min + 1
    chars = []
    while f > 0.0 and len(chars) < 8:
        f *= range
        i = int(f)
        f -= i
        chars.append(chr(min+i))
    s = ''.join(chars)
    return s

#----------------------------------------------------------------------------
# prefixLen
#----------------------------------------------------------------------------
def prefixLen(s0,s1):
    l = 0
    for a,b in zip(s0,s1):
        if a != b:
            break
        l += 1
    return l

#----------------------------------------------------------------------------
# interpolateString
#----------------------------------------------------------------------------
def interpolateString(t, s0, s1, min='\0', max='\xff'):
    p = prefixLen(s0, s1)
    n0 = stringToFloat(s0[p:], min, max)
    n1 = stringToFloat(s1[p:], min, max)
    n = n0 * (1.0 - t) + n1 * t
    if n < 0:
        n = 0.0
    elif n >= 1.0:
        n = 1.0 - 1e-9
    s = floatToString(n, min, max)
    return s0[:p] + s


#----------------------------------------------------------------------------
# Alphabet
#----------------------------------------------------------------------------
class Alphabet(object):
    def __init__(self, letters):
        self._chr = list(letters)
        self._ord = dict((c,i) for i,c in enumerate(self._chr))
        if len(self._ord) < len(self._chr):
            raise ValueError('alphabet cannot contain duplicate letters')
    
    def size(self):
        '''size() --> integer

        Return the number of letters in the alphabet.'''
        return len(self._chr)
    
    def ord(self, c):
        '''ord(c) --> integer

        Return the integer ordinal of the letter c in this alphabet.
        '''
        try:
            return self._ord(c);
        except KeyError:
            raise ValueError('character not in alphabet: %r' % c)
    
    def chr(self, i):
        '''chr(i) --> letter

        Return the letter with ordinal i in this alphabet.
        '''
        try:
            return self._chr[i]
        except IndexError:
            raise ValueError('index not in alphabet: %r' % c)
    
    def chrMap(self):
        return self._chr[:]
    
    def ordMap(self):
        return self._ord.copy()

#----------------------------------------------------------------------------
# chr_range
#----------------------------------------------------------------------------
def chr_range(min, max):
    return [chr(i) for i in xrange(ord(min), ord(max)+1)]

#----------------------------------------------------------------------------
# data
#----------------------------------------------------------------------------
_byteAlphabet = Alphabet(chr_range('\x00', '\xff'));


#----------------------------------------------------------------------------
# string_ord
#----------------------------------------------------------------------------
def string_ord(s, maxLen=None, alphabet=_byteAlphabet):
    '''string_ord(s [, maxLen [, alphabet]]) --> integer

    Get the index of a string in the sorted set of all strings up to a
    maximum length.  The strings are drawn from an alphabet of
    characters.  If the maximum length isn't given, it defaults to the
    length of the string.  The alphabet defaults to the binary
    alphabet of all character values between chr(0) and chr(255).
    '''
    if maxLen is None:
        maxLen = len(s)

    if len(s) > maxLen:
        s = s[:maxLen]

    A = alphabet.size()
    I = alphabet.ordMap()

    def _fixed_ord(s):
        'ordinal of string S in set of strings of length len(S)'
        r = 0
        for c in s:
            r = r * A + I[c]
        return r

    # For length L in [0, maxLen], find the number of strings of
    # length L that come before S

    # If L is less than the length of S, then the length-L prefix of S
    # and all its predecessors come before S, or 1 plus the fixed
    # width ordinal of the length-L prefix.

    # If L is equal to the length of S, then the fixed-width ordinal
    # of S is the number of strings of length L before S.

    # If L is greater than the length of S, ...


    # This is inefficient but correct.  It is O(maxLen^2), and could
    # probably be O(maxLen) or even O(len(s)).  We probably also don't
    # need to make all the prefix copies of S as we accumulate.
    r = 0
    for L in xrange(maxLen+1):
        if L < len(s):
            r += _fixed_ord(s[:L]) + 1
        else:
            r += _fixed_ord(s) * pow(A, maxLen - L)
    return r
    

#----------------------------------------------------------------------------
# string_chr
#----------------------------------------------------------------------------
def string_chr(idx, maxLen, alphabet=_byteAlphabet):
    '''string_chr(idx, maxLen [, alphabet]) --> string

    Get the string associated with an index in the sorted set of all
    strings up to a maximum length.  The strings are drawn from an
    alphabet of characters.  The alphabet defaults to the binary
    alphabet of all character values between chr(0) and chr(255).
    '''
    A = alphabet.size()
    C = alphabet.chrMap()

    k = 1
    K = [k]
    for i in xrange(1,maxLen):
        k *= A
        K.append(K[-1] + k)

    k = k*A + K[-1]
    if not 0 <= idx < k:
        raise ValueError('expected string index between '
                         '0 and %d, got %r' % ((k-1),idx))

    #print 'K=',K
    r = []
    while idx > 0:
        k = K.pop()
        idx -= 1
        i = int(idx / k)
        r.append(C[i])
        #print '--idx=%d k=%d (idx/k)=%d nextIdx=%d K=%r r=%r' % (idx,k,i,idx-i*k,K,r)
        idx -= i*k

    return ''.join(r)


#----------------------------------------------------------------------------
# interpolate_string
#----------------------------------------------------------------------------
def interpolate_string(t, s0, s1, interpLen=8, alphabet=_byteAlphabet):
    '''interpolate_string(t, s0, s1 [, interpLen=8 [, alphabet]]) --> string

    Get an interpolated string between s0 and s1.  The t parameter is
    the interpolation coefficient.  It should range between 0.0 for a
    string close to s0, and 1.0 for a string close to s1.  Since there
    is an infinite number of possible strings between any two unequal
    strings, this function chooses a an interpolated string from a set
    of bounded-length strings.  The maximum length is the length of
    the common prefix of s0 and s1, plus the optional parameter
    interpLen.  Another way of looking at this is that the
    interpolated string will have interpLen "significant characters"
    between s0 and s1.  The last parameter is the alphabet from which
    the strings are drawn.  It defaults to the binary alphabet of all
    character values between chr(0) and chr(255).
    '''
    if t <= 0: t = 0
    elif t >= 1: t = 1

    p = prefixLen(s0, s1)

    i0 = string_ord(s0[p:], interpLen, alphabet)
    i1 = string_ord(s1[p:], interpLen, alphabet)

    i = int(i0 + (i1 - i0) * t)
    return s0[:p] + string_chr(i, interpLen, alphabet)


#----------------------------------------------------------------------------
# test
#----------------------------------------------------------------------------
def time_test():
    import timeit
    
    tests = [
        ('x[66]', "x = [chr(i) for i in xrange(256)]"),
        ('x[66]', "x = ''.join([chr(i) for i in xrange(256)])"),
        ('x[66]', "from util.string_interpolate import Alphabet; x = Alphabet(chr(i) for i in xrange(256))._chr"),
        ('x.chr(66)', "from util.string_interpolate import Alphabet; x = Alphabet(chr(i) for i in xrange(256))"),
        ('x(66)', "from util.string_interpolate import Alphabet; x = Alphabet(chr(i) for i in xrange(256)).chr"),
        ('x.chr_nocheck(66)', "from util.string_interpolate import Alphabet; x = Alphabet(chr(i) for i in xrange(256))"),
        ('x(66)', "from util.string_interpolate import Alphabet; x = Alphabet(chr(i) for i in xrange(256)).chr_nocheck"),
        ('x(66)', "from util.string_interpolate import Alphabet; a = Alphabet(chr(i) for i in xrange(256)); x = lambda i: a._chr[i]"),
        ('x(66)', "from util.string_interpolate import Alphabet; c = Alphabet(chr(i) for i in xrange(256))._chr; x = lambda i: c[i]"),
        ('chr(66)', ""),
        ]
    
    for t in tests:
        print 'Setup:', t[1]
        print 'Timing:', t[0]
        print '  ', timeit.Timer(*t).timeit()
    

if __name__ == '__main__':
    a = Alphabet('abc')

    p = 'a b aa ab ba bb aaa aab aba abb baa bab bba bbb'.split()
    p.append('')
    p.sort()


    import random
    for i in xrange(10):
        t = random.uniform(-0.1, 1.1)
        s = interpolate_string(t, '', 'bbbbbbbbbbbbbb', 8, a)
        print '%f %s' % (t, s)

    #for x in p:
    #    print '%-3s %d' % (x, string_ord(x, 3, a))

    L = 3
    for s in p:
        ord = string_ord(s,L,a)
        #print 'len=%d: ord(%r) -> %d ' % (L, s, ord)
        chr = string_chr(ord,L,a)
        print 'len=%d: ord(%r) -> %d  chr(%d) -> %r' % (L, s, ord, ord, chr)
        #print

    for i in xrange(-2, 20):
        print ('string_chr(%d, 3, {a,b}) ->' % i),
        try:
            print '%r' % string_chr(i,3,a)
        except ValueError:
            print 'error'


    #for L in xrange(0,6):
    #    for s in 'a b aa ab bbb'.split():
    #        ord = string_ord(s,L,a)
    #        chr = string_chr(ord,L,a)
    #        print 'len=%d: ord(%r) -> %d  chr(%d) -> %r' % (L, s, ord, ord, chr)
