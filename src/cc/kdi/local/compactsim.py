#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/local/compactsim.py#1 $
#
# Created 2007/10/18
#
# Copyright 2007 Kosmix Corporation.  All rights reserved.
# Kosmix PROPRIETARY and CONFIDENTIAL.

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
