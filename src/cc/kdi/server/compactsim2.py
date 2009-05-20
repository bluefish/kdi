#!/usr/bin/env python
#
# Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
# Created 2009-05-19
#
# This file is part of KDI.
#
# KDI is free software; you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or any later version.
#
# KDI is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

#----------------------------------------------------------------------------
# Imports
#----------------------------------------------------------------------------
import random

#----------------------------------------------------------------------------
# Constants
#----------------------------------------------------------------------------
INITIAL_DATA_SIZE = 128
NUM_GROUPS = 1
SPLIT_THRESHOLD = 200
MAX_WIDTH = 32

#----------------------------------------------------------------------------
# alphanum
#----------------------------------------------------------------------------

#_alpha = 'abcdefghijklmnopqrstuvwxyz' \
#         'ABCDEFGHIJKLMNOPQRSTUVWXYZ' \
#         '0123456789'

_alpha = 'abcdefghijklmnopqrstuvwxyz'

def alphanum(n,w=1):
    n = int(n)
    k = len(_alpha)
    r = []
    while n or w > 0:
        w -= 1
        r.append(_alpha[n%k])
        n = int(n/k)
    r.reverse()
    return ''.join(r)

#----------------------------------------------------------------------------
# Fragment
#----------------------------------------------------------------------------
_fidx = 0
class Fragment:
    def __init__(self):
        self.dataSize = INITIAL_DATA_SIZE
        self.refCount = 0

        global _fidx
        self.name = alphanum(_fidx, 2)
        _fidx += 1

    def getDataSize(self):
        return self.dataSize

    def getSplitSize(self):
        return float(self.dataSize) / self.refCount

    def addRef(self):
        self.refCount += 1

#----------------------------------------------------------------------------
# Tablet
#----------------------------------------------------------------------------
class Tablet:
    def __init__(self):
        self.frags = [ [] for i in xrange(NUM_GROUPS) ]
    
    def getDataSize(self, groupIndex):
        return sum(f.getSplitSize() for f in self.frags[groupIndex])

    def getTotalSize(self):
        return sum(self.getDataSize(i) for i in xrange(NUM_GROUPS))

    def appendFragment(self, frag, groupIndex):
        self.frags[groupIndex].append(frag)
        frag.addRef()

    def splitTablet(self):
        t2 = Tablet()
        for idx,frags in enumerate(self.frags):
            for f in frags:
                t2.appendFragment(f, idx)
        return t2,self

    def getChainLength(self, groupIndex):
        return len(self.frags[groupIndex])

    def getFragments(self, groupIndex):
        return self.frags[groupIndex]

#----------------------------------------------------------------------------
# Table
#----------------------------------------------------------------------------
class Table:
    def __init__(self):
        self.tablets = [ Tablet() ]
    
    def appendFragment(self, frag):
        nTablets = min(int(random.paretovariate(2)), len(self.tablets))
        nGroups = min(int(random.paretovariate(2)), NUM_GROUPS)
        enumTablets = random.sample(list(enumerate(self.tablets)), nTablets)
        groups = random.sample(xrange(NUM_GROUPS), nGroups)
        
        enumTablets.sort()
        enumTablets.reverse()

        for ti,t in enumTablets:
            for groupIndex in groups:
                t.appendFragment(frag, groupIndex)
            
            if t.getTotalSize() >= SPLIT_THRESHOLD:
                self.tablets[ti:ti+1] = list(t.splitTablet())

    def getMaxChainLength(self, groupIndex):
        return max(t.getChainLength(groupIndex) for t in self.tablets)

    def getGroupCount(self):
        return NUM_GROUPS

    def replaceSet(self, cset, frag, groupIndex):
        for t in self.tablets:
            if t not in cset:
                continue
            frags = t.frags[groupIndex]
            old = cset[t]
            i = frags.index(old[0])
            frags[i:i+len(old)] = [frag]
            frag.addRef()

#----------------------------------------------------------------------------
# chooseCompactionGroup
#----------------------------------------------------------------------------
def chooseCompactionGroup(tables):
    maxLen = 4
    r = (None,None)
    for t in tables:
        for i in xrange(t.getGroupCount()):
            ln = t.getMaxChainLength(i)
            if ln > maxLen:
                r = (t,i)
                maxLen = ln
    return r

#----------------------------------------------------------------------------
# iterSegs
#----------------------------------------------------------------------------
def iterSegs(chain, width):
    if len(chain) <= width:
        yield chain
    else:
        for i in xrange(len(chain)+1-width):
            yield chain[i:i+width]

#----------------------------------------------------------------------------
# findMinWeightSegment
#----------------------------------------------------------------------------
def findMinWeightSegment(chain, width):
    def wt(seg):
        return sum(f.getSplitSize() for f in seg)
    return min((wt(seg),seg) for seg in iterSegs(chain,width))[1]

#----------------------------------------------------------------------------
# iterStarts
#----------------------------------------------------------------------------
def iterStarts(chain, yes, no):
    inSeg = False
    for i,x in enumerate(chain):
        if x in no:
            inSeg = False
        elif x in yes:
            if not inSeg:
                inSeg = True
                yield i

#----------------------------------------------------------------------------
# iterEnds
#----------------------------------------------------------------------------
def iterEnds(chain, start, width, yes, no):
    inYes = True
    for i in xrange(start+1, min(start+width, len(chain))):
        f = chain[i]
        if f in no:
            yield i
            return
        elif f not in yes:
            if inYes:
                inYes = False
                yield i
        else:
            inYes = True
    if inYes:
        yield min(start+width, len(chain))

#----------------------------------------------------------------------------
# findCompactionSegment
#----------------------------------------------------------------------------
def findCompactionSegment(chain, yes, no, width):
    maxRange = None
    maxScore = None
    for start in iterStarts(chain, yes, no):
        for end in iterEnds(chain, start, width, yes, no):
            sc = 0
            sz = 0
            for f in chain[start:end]:
                sz += f.getSplitSize()
                if f in yes: sc += 1
                else:        sc -= 1

            score = (sc,-sz)
            if not maxRange or score > maxScore:
                maxScore = score
                maxRange = (start,end)
    if maxRange:
        lo,hi = maxRange
        return chain[lo:hi]
    else:
        return []

#----------------------------------------------------------------------------
# chooseCompactionSet
#----------------------------------------------------------------------------
def chooseCompactionSet(table, groupIndex):
    # Find max chain
    maxLen = 0
    maxTablet = None
    for t in table.tablets:
        ln = t.getChainLength(groupIndex)
        if ln > maxLen:
            maxLen = ln
            maxTablet = t
    
    if not maxTablet:
        return {}

    # Find the min-weight segment of max width
    seg = findMinWeightSegment(
        maxTablet.getFragments(groupIndex),
        MAX_WIDTH)

    yes = set(seg)
    no = set(f for f in maxTablet.getFragments(groupIndex) if f not in yes)
    
    cset = {}
    for t in table.tablets:
        seg = findCompactionSegment(
            t.getFragments(groupIndex), yes, no, MAX_WIDTH)
        if len(seg) < 2:
            continue
        cset[t] = seg

    return cset
    


#----------------------------------------------------------------------------
# ptable
#----------------------------------------------------------------------------
def ptable(table, compactPair=None):
    print '%d tablet(s)' % len(table.tablets)

    allLens = [ [] for i in xrange(table.getGroupCount()) ]
    for gi,lens in enumerate(allLens):
        for t in table.tablets:
            lens.append(len(t.frags[gi]))
        lens.sort()
        if not lens:
            lens.append(0)
        print 'group %d' % (gi+1)
        print '  nFrags = %d' % sum(lens)
        print '  avg chain = %.2f' % (sum(lens) * 1.0 / len(lens))
        print '  max chain = %d' % lens[-1]
        print '  pct: %s' % (' '.join('%d=%d' % (p,lens[int(p/100.0*len(lens))])
                                      for p in xrange(10,99,10)))
    return

    def fname(t,gi,f):
        if compactPair and gi == compactPair[0]:
            if t in compactPair[1] and f in compactPair[1][t]:
                return f.name.upper()
        return f.name

    def flist(t,gi):
        return ' '.join(fname(t,gi,f) for f in t.frags[gi])

    for ti,t in enumerate(table.tablets):
        print '  %2d %d' % (ti+1, t.getTotalSize())
        for gi in xrange(table.getGroupCount()):
            print '    %2d %5d %s' % (gi+1, t.getDataSize(gi), flist(t,gi))


#----------------------------------------------------------------------------
# main
#----------------------------------------------------------------------------
def main():
    t = Table()
    
    ptable(t)
    print '-'*80

    delay = 0

    for i in xrange(5000):

        if i < 2000:
            t.appendFragment(Fragment())
        cpair = None

        if not delay:
            tbl,grp = chooseCompactionGroup([t])
            if tbl:
                cpair = (grp, chooseCompactionSet(tbl, grp))
        else:
            delay -= 1

        ptable(t,cpair)
        print '-'*80

        if cpair:
            newFrag = Fragment()
            newFrag.dataSize = 0
            for flist in cpair[1].itervalues():
                for f in flist:
                    newFrag.dataSize += f.getSplitSize()
            t.replaceSet(cpair[1], newFrag, cpair[0])
            print 'Compaction cost: %d' % newFrag.dataSize
            delay = int(newFrag.dataSize / 50.0) + 1

if __name__ == '__main__':
    main()
