#!/usr/bin/env python
#
# Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
# Created 2009-02-27
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

import heapq

class Heap:
    def __init__(self):
        self._h = []

    def push(self, x):
        heapq.heappush(self._h, x)
    
    def pop(self):
        return heapq.heappop(self._h)

    def top(self):
        return self._h[0]

    def __len__(self):
        return len(self._h)

class End:
    def __cmp__(self, other):
        return 1

class Cell:
    def __init__(self, r, v):
        self.r = r
        self.v = v

    def __cmp__(self, other):
        return cmp(self.r, other.r)

    def __repr__(self):
        return '(%r,%r)' % (self.r, self.v)

    def __len__(self):
        if self.v is None:
            return 0
        else:
            return 1

class Fragment:
    def __init__(self, seq, idx):
        self.key = None
        self.idx = idx
        self.it = iter(seq)
    
    def advance(self):
        for self.key in self.it:
            break
        else:
            self.key = None

    def readUntil(self, k):
        r = []
        while self.hasMore() and self.key < k:
            r.append(self.key)
            self.advance()
        return r

    def hasMore(self):
        return self.key is not None

    def __cmp__(self, other):
        return cmp(self.key, other.key) or cmp(self.idx, other.idx)


def merge(*seqs):
    heap = Heap()
    for i,seq in enumerate(seqs):
        f = Fragment(seq, i)
        f.advance()
        if f.hasMore():
            heap.push(f)

    while heap:
        f = heap.pop()
        while heap and heap.top().key <= f.key:
            f2 = heap.pop()
            f2.advance()
            if f2.hasMore():
                heap.push(f2)

        if heap:
            block = f.readUntil(heap.top().key)
        else:
            block = f.readUntil(End())

        yield f.idx, block
        
        if f.hasMore():
            heap.push(f)

def main():
    a = [1,2,6,9,21,22,23,24]
    b = [1,3,5,7,14,15,16,17]
    c = [9,10,11,12,18,19,20]
    d = [4,8,11,13]

    for x in enumerate((a,b,c,d)):
        print x

    print

    for x in merge(a,b,c,d):
        print x

if __name__ == '__main__':
    main()
