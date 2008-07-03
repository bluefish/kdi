#!/usr/bin/env python
#
# Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
# Created 2007-10-18
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

from math import log

def lg(x):
    return log(x+1, 2)

def cost(arr):
    return sum(arr) * lg(len(arr))

def applyCompaction(arr, i, j):
    return arr[:i] + [sum(arr[i:j])] + arr[j:]
    
def compactScore(arr, i, j):
    base = cost(arr)
    post = cost(applyCompaction(arr, i, j))
    work = cost(arr[i:j])
    sc = (base-post) / work
    print i,j,sc,arr
    return sc

def chooseCompaction(arr,maxlen):
    bestScore = 0.0
    best = (0, len(arr))
    off = max(len(arr) - maxlen + 1, 2)
    for i in range(len(arr)):
        for j in range(i+off, len(arr)+1):
            sc = compactScore(arr, i, j)
            if sc > bestScore:
                best = (i,j)
                bestScore = sc
    return best

#----------------------------------------------------------------------------
# Foo
#----------------------------------------------------------------------------
class Foo:
    def __init__(self, thresh):
        self.thresh = thresh
        self.tbls = []
        self.totalCompaction = 0

    def shouldCompact(self):
        #return len(self.tbls) > 8
        
        c = chooseCompaction(self.tbls, len(self.tbls))
        s = compactScore(self.tbls, *c)
        return s >= self.thresh

    def add(self,x):
        self.tbls.append(x)

        if self.shouldCompact():
            c = chooseCompaction(self.tbls, len(self.tbls))
            self.totalCompaction += sum(self.tbls[c[0]:c[1]])
            self.tbls = applyCompaction(self.tbls, *c)

#----------------------------------------------------------------------------
# main
#----------------------------------------------------------------------------
def main():
    from random import randrange
    f = Foo(0.7)
    for x in xrange(100):
        sz = randrange(100,150)
        print '%s + [%s] =' % (f.tbls,sz),
        f.add(sz)
        print '%s total=%s ratio=%.3f' % (f.tbls, f.totalCompaction, f.totalCompaction*1.0/sum(f.tbls))

if __name__ == '__main__':
    pass
    #main()
