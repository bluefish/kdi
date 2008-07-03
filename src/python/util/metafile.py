#!/usr/bin/env python
#
# Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
# Created 2005-10-03
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

from util.iter import pushback_iter

#----------------------------------------------------------------------------
# MetaBase
#----------------------------------------------------------------------------
class MetaBase(object):
    def __len__(self):
        return self.numChildren()
    def __iter__(self):
        for i in range(self.numChildren()):
            yield self.getChild(i)
    def __getitem__(self, i):
        return self.getChild(i)
    def __eq__(self, o):
        return self is o
    def __ne__(self, o):
        return not (self == o)
    def numChildren(self):
        return 0
    def getChild(self, i):
        raise IndexError()
    def addChild(self, ch):
        raise NotImplementedError()
    def open(self, mode='r'):
        raise NotImplementedError()
    def save(self, metaFn):
        f = file(metaFn, 'w')
        f.write(repr(self))
        f.write('\n')
        f.close()
    def load(metaFn):
        f = file(metaFn)
        s = f.read()
        f.close()
        return eval(s)
    load = staticmethod(load)

def _checkBase(x, base=MetaBase):
    if not isinstance(x, base):
        raise ValueError('Must derive from %s' % base.__name__)

#----------------------------------------------------------------------------
# MetaNull
#----------------------------------------------------------------------------
class MetaNull(MetaBase):
    def open(self, mode='r'):
        if mode == 'r':
            return NullInputFile()
        else:
            raise NotImplementedError()
    def __eq__(self, o):
        return isinstance(o, MetaNull)
    def __repr__(self):
        return 'MetaNull()'


#----------------------------------------------------------------------------
# MetaFile
#----------------------------------------------------------------------------
class MetaFile(MetaBase):
    def __init__(self, fn):
        self.fn = fn
    def __eq__(self, o):
        return isinstance(o, MetaFile) and self.fn == o.fn
    def open(self, mode='r'):
        return RegularFile(self.fn, mode)
    def __repr__(self):
        return 'MetaFile(%r)'%self.fn
    

#----------------------------------------------------------------------------
# MetaGroup
#----------------------------------------------------------------------------
class MetaGroup(MetaBase):
    def __init__(self, *args):
        self.children = []
        for a in args:
            self.addChild(a)
    def __eq__(self, o):
        return isinstance(o, MetaGroup) and self.children == o.children
    def numChildren(self):
        return len(self.children)
    def addChild(self, ch):
        _checkBase(ch)
        self.children.append(ch)
    def eraseChildren(self, newEnd):
        del self.children[newEnd:]
    def getChild(self, i):
        return self.children[i]
    def open(self, mode='r', gen=None):
        # Default interpretation is as a single serial logical file
        return SerialMultiFile(self, mode, gen)
    def __repr__(self):
        return 'MetaGroup(%s)'%(', '.join([repr(x) for x in self.children]))


#----------------------------------------------------------------------------
# NullInputFile
#----------------------------------------------------------------------------
class NullInputFile(object):
    def read(self):
        return ''
    def close(self):
        pass
    def seek(self, pos):
        assert pos == 0
    def seekToEnd(self):
        pass
    def tell(self, pos):
        return 0


#----------------------------------------------------------------------------
# RegularFile
#----------------------------------------------------------------------------
class RegularFile(file):
    def seekToEnd(self):
        self.seek(0, 2)


#----------------------------------------------------------------------------
# SerialMultiFile
#----------------------------------------------------------------------------
class SerialMultiFile(object):
    __WRITE_MODES = ('r+', 'w', 'a', 'a+', 'w+')
    __READ_MODES = ('r', 'r+', 'a+', 'w+')
    __SUPPORTED_MODES = ('r', 'r+', 'w', 'a', 'a+', 'w+')
    def __init__(self, meta, mode, gen):
        _checkBase(meta, MetaGroup)
        if mode not in self.__SUPPORTED_MODES:
            # Don't support 'a': initial position is not file start
            raise ValueError('Unsupported mode: %r' % mode)
        self.meta = meta
        self.mode = mode
        self.gen = pushback_iter(gen and gen or '')
        self.idx = -1
        self.eofPos = None
        self.fp = None
        self.closed = False
        if 'w' in mode:
            self.truncate()
        elif mode == 'a':
            self.seekToEnd()
            self.mode = 'w'

    def closeCurrent(self):
        if self.fp:
            self.fp.seekToEnd()
            self.eofPos = self.fp.tell()
            self.fp.close()
            self.fp = None

    def openNext(self, create=False):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        self.closeCurrent()
        if not create and self.idx == self.meta.numChildren() - 1:
            return False
        for mf in self.gen:
            self.idx += 1
            if self.idx < self.meta.numChildren():
                mf2 = self.meta.getChild(self.idx)
                if mf != mf2:
                    raise RuntimeError("Generator doesn't match existing meta group")
                # Use the existing one instead
                mf = mf2
            else:
                self.meta.addChild(mf)
            break
        else:
            if self.idx+1 < self.meta.numChildren():
                self.idx += 1
                mf = self.meta.getChild(self.idx)
            else:
                return False
        try:
            self.fp = mf.open(self.mode)
            return True
        except IOError:
            self.gen.push(mf)
            self.idx -= 1
            return False

    def tell(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        # State: (file_idx, file_pos, file_open)
        # Initial state:  (-1, None, False)
        # End of file index i: (i, eof_i, False)
        # In file j, at pos p: (j, p, True)
        if self.fp:
            fpos = self.fp.tell()
        else:
            fpos = self.eofPos
        return (self.idx, fpos, self.fp != None)

    def seek(self, pos):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        idx,fpos,op = pos
        if idx < self.idx:
            raise ValueError('Cannot seek backwards')
        while self.idx < idx:
            if not self.openNext():
                break
        if self.fp:
            if op:
                self.fp.seek(fpos)
            else:
                self.closeCurrent()

    def seekToEnd(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        nc = self.meta.numChildren()
        while self.idx < nc-1:
            if not self.openNext():
                break
        if self.fp:
            self.fp.seekToEnd()

    def write(self, x):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if x:
            if self.mode == 'a+':
                # Emulate strange append behavior
                self.seekToEnd()
                self.mode = 'w+'
            if not self.fp:
                if not self.openNext(create=True):
                    raise IOError('Could not generate file')
            assert self.fp
            self.fp.write(x)

    def read(self, n):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        r = ''
        while n > 0:
            if not self.fp:
                if not self.openNext():
                    break
            x = self.fp.read(n)
            if not x:
                self.closeCurrent()
            else:
                r += x
                n -= len(x)
        return r

    def flush(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if self.fp:
            self.fp.flush()
            if self.mode in self.__WRITE_MODES:
                self.closeCurrent()

    def close(self):
        if not self.closed:
            self.closeCurrent()
            self.closed = True

    def truncate(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if not self.mode in self.__WRITE_MODES:
            raise IOError('Invalid argument')
        self.meta.eraseChildren(self.idx + 1)
        if self.fp:
            self.fp.truncate()


#----------------------------------------------------------------------------
# ParallelMultiFile
#----------------------------------------------------------------------------
class ParallelMultiFile(object):
    def __init__(self, meta, mode='r'):
        _checkBase(meta, MetaGroup)
        self.fp = [ meta.getChild(i).open(mode) for i in range(meta.numChildren()) ]
    def numFiles(self):
        return len(self.fp)
    def __len__(self):
        return len(self.fp)
    def getFile(self, idx):
        return self.fp[idx]
    def __getitem__(self, idx):
        return self.fp[idx]
    def tell(self):
        return tuple([f.tell() for f in self.fp])
    def seek(self, pos):
        if len(pos) != len(self.fp):
            raise ValueError('Wrong state size')
        for f,p in zip(self.fp, pos):
            f.seek(p)
    def seekToEnd(self):
        for f in self.fp:
            f.seekToEnd()
    def flush(self):
        for f in self.fp:
            f.flush()
    def close(self):
        for f in self.fp:
            f.close()



#----------------------------------------------------------------------------
# testing
#----------------------------------------------------------------------------

# x = MetaGroup(*[MetaFile(x) for x in 'foo bar baz quux'.split()])
# x.open()
# 
# x = MetaGroup()
# f = x.open('w', (MetaFile('foo%d'%x) for x in count_forever()))

# import os
# os.chdir('/tmp/chk')
# 
# def fngen(stop=None):
#     if stop == None:
#         x = 0
#         while True:
#             yield 'foo%d' % x
#             x += 1
#     else:
#         for x in range(stop):
#             yield 'foo%d' % x
# 
# def mfgen(stop=None):
#     for fn in fngen(stop):
#         yield MetaFile(fn)
