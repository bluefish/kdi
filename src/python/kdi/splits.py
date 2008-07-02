#!/usr/bin/env python
#
# $Id: splits.py $
#
# Created 2008/06/15
#
# Copyright 2008 Kosmix Corporation.  All rights reserved.
# Kosmix PROPRIETARY and CONFIDENTIAL.

def iterSplitRanges(splitPoints, maxKey):
    minT = 0.0
    minKey = ''
    
    for t,s in splitPoints:
        yield (minT, t, minKey, s)
        minT, minKey = t,s

    yield (minT, 1.0, minKey, maxKey)

def iterSplits(nPartitions, splitPoints, alphabet):
    if nPartitions < 2:
        return

    maxKey = alphabet.chr(alphabet.size()-1) * 10
    splitRanges = iterSplitRanges(splitPoints, maxKey)
    minT,maxT,minStr,maxStr = splitRanges.next()

    from util.string_interpolate import interpolate_string

    for i in xrange(1, nPartitions):
        t = float(i) / nPartitions

        while t > maxT:
            minT,maxT,minStr,maxStr = splitRanges.next()

        pt = (t - minT) / (maxT - minT)
        s = interpolate_string(pt, minStr, maxStr, 8, alphabet)
        yield s

def iterRowPredicates(nParts, splitPoints, alphabet):
    if nParts < 2:
        yield ''
    else:
        splits = iterSplits(nParts, splitPoints, alphabet)
        
        hi = splits.next()
        yield 'row < %r' % hi
        lo = hi
        
        for hi in splits:
            yield '%r <= row < %r' % (lo, hi)
            lo = hi

        yield 'row >= %r' % lo

def loadSplitPointsFromFile(fn):
    f = file(fn)
    try:
        pts = []
        for line in f:
            t,s = line.strip().split()
            t = float(t)
            if 0 <= t <= 1:
                pts.append((t,s))
            else:
                raise ValueError('split out of range: %f %r' % (t,s))
    finally:
        f.close()
    pts.sort()
    return pts

def loadSplitPointsFromMeta(metaUri):
    if not metaUri.startswith('meta+'):
        raise ValueError('not a meta table: %r' % metaUri)
    metaUri = metaUri[5:]
    
    import urllib,cgi
    p,q = urllib.splitquery(metaUri)
    q = cgi.parse_qs(q or '')
    tableName = q.get('name',[''])[0]

    if not tableName:
        raise ValueError('meta uri needs table name')

    from util.zero import zeroEncode,zeroDecode
    pred = '%r <= row < %r and column = "location"' % \
        (zeroEncode(tableName, '\x01', ''),
         zeroEncode(tableName, '\x02', ''))

    rows = []
    import pykdi
    for r,c,t,v in pykdi.Table(metaUri).scan(pred):
        n,x,r = zeroDecode(r)
        rows.append(r)

    f = 1.0 / (len(rows) + 1)
    return [((i+1)*f, r) for i,r in enumerate(rows)]

def main():
    import optparse
    op = optparse.OptionParser()
    op.add_option('-m','--meta',help='Load splits from a meta-table')
    op.add_option('-f','--file',help='Load splits from a file')
    op.add_option('-s','--splits',action='store_true',help='Dump split points')
    op.add_option('-n','--num',type='int',default=1,help='Number of partitions to use')
    op.add_option('-i','--index',type='int',help='Emit row predicate for single partition')
    op.add_option('-r','--rows',action='store_true',help='Dump row predicates for all partitions')
    op.add_option('-b','--binary',action='store_true',help='Generate splits for binary strings')
    opt,args = op.parse_args()

    from util.string_interpolate import Alphabet,chr_range
    if opt.binary:
        alpha = Alphabet(chr_range('\x00', '\xff'))
    else:
        alpha = Alphabet(chr_range(' ', '~'))

    pts = []
    if opt.meta:
        pts = loadSplitPointsFromMeta(opt.meta)
    if opt.file:
        pts = loadSplitPointsFromFile(opt.file)
    if opt.splits:
        for t,s in pts:
            print t,s
    rows = list(iterRowPredicates(opt.num, pts, alpha))
    if opt.rows:
        for r in rows:
            print r
    if opt.index is not None:
        print rows[opt.index]

if __name__ == '__main__':
    main()
