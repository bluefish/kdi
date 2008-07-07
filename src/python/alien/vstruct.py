#!/usr/bin/env python
#
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-03-29
# 
# This file is part of Alien.
# 
# Alien is free software; you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or any later version.
# 
# Alien is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

"""Versioned, structured data types"""

from util.xmlfmt import xml

#----------------------------------------------------------------------------
# VersionError
#----------------------------------------------------------------------------
class VersionError(StandardError):
    def __init__(self, cls, ver):
        StandardError.__init__(self, 'Unsupported version: %s version %r'
                               % (cls.__name__, ver))


#----------------------------------------------------------------------------
# Utils
#----------------------------------------------------------------------------
def _fadd(a,b):
    if callable(a):
        if callable(b):
            return lambda *x: a(*x) + b(*x)
        else:
            return lambda *x: a(*x) + b
    elif callable(b):
        return lambda *x: a + b(*x)
    else:
        return a + b

def _struct_thunk(func, attr, offset, isVarType):
    if callable(offset):
        if isVarType:
            def dv_thunk(obj):
                off = offset(obj)
                #print 'dv_thunk', func, obj, attr, offset, off
                return off + func(obj, obj._getattr(attr, off))
            return dv_thunk
        else:
            def d_thunk(obj):
                off = offset(obj)
                #print 'd_thunk', func, obj, attr, offset, off
                return off + func(obj._getattr(attr, off))
            return d_thunk
    else:
        if isVarType:
            def sv_thunk(obj):
                #print 'sv_thunk', func, obj, attr, offset
                return offset + func(obj, obj._getattr(attr, offset))
            return sv_thunk
        else:
            def s_thunk(obj):
                #print 's_thunk', func, obj, attr, offset
                return offset + func(obj._getattr(attr, offset))
            return s_thunk

#----------------------------------------------------------------------------
# StructType
#----------------------------------------------------------------------------
class StructType(type):
    """StructType

    A type derived from StructType has some magic attributes.

       __format__          versioned dict of attribute declarations
       __version__         version number to be used by default
       __min_version__     minimum supported version
       __base_versions__   versioned dict base class versions
       __vinfo__           versioned dict of struct info

       Attribute declarations are lists of (name, type, version)
       tuples.  The version is optional.

       Each struct info consists of an attribute table and a size.
       The size may be dynamic.  The attribute table is a dictionary
       that maps attribute names to (offset, type, version) tuples.
       Each offset may be dynamic.
    """
    def __init__(self, name, bases, body):
        type.__init__(self, name, bases, body)

        #print 'vstruct: %s' % name

        # Set default __format__ if necessary
        if '__format__' not in body:
            if '__version__' in body:
                self.__format__ = { self.__version__ : [] }
            else:
                self.__format__ = { 0 : [] }

        # Set default __version__ if necessary
        if '__version__' not in body:
            self.__version__ = max(self.__format__.keys())

        # Set default __min_version__ if necessary
        if '__min_version__' not in body:
            self.__min_version__ = min(self.__format__.keys())

        # Make sure __min_version__ is valid
        if not self.__min_version__ <= self.__version__:
            raise AttributeError('Min version (%r) greater than latest '
                                 'version (%r): %s' %
                                 (self.__min_version__, self.__version__,
                                  name))

        # Set default __base_versions__ if necessary
        if '__base_versions__' not in body:
            self.__base_versions__ = {}

        # Build __vinfo__ table for all versions
        self.__vinfo__ = {}
        for ver in xrange(self.__min_version__, self.__version__ + 1):
            # Make sure version is valid
            if ver not in self.__format__:
                raise AttributeError('Missing format specification for '+
                                     '%s version %r' % (name, ver))

            # Set default __base_versions__ for this version if necessary
            if ver not in self.__base_versions__:
                self.__base_versions__[ver] = [ None ] * len(bases)

            # Otherwise, make sure we have the right number of base versions
            elif len(self.__base_versions__[ver]) != len(bases):
                raise AttributeError('Base versions list size does not match '
                                     'base list size for version %r: %s'
                                     % (ver, name))

            # Build attribute table for this version
            attrs = {}
            offset = 0

            # Collect attributes from all base classes
            # FIX: This base class code only works when there is zero
            #      or one struct base (and it comes first).  For
            #      multiple inheritance, the offsets of later bases
            #      will be wrong in the attribute table.
            for base,bver in zip(bases, self.__base_versions__[ver]):
                # Can only get attributes from StructType bases
                if not isinstance(base, StructType):
                    continue

                # Choose default version if none specified
                if bver is None:
                    bver = base.__version__

                # Get base attribute table and size
                battrs, bsize = base.__vinfo__[bver]

                # Update our attribute table
                attrs.update(battrs)

                # Offset attributes in this class by the size of the
                # base class.
                offset = _fadd(offset, bsize)

            # Add attributes from attribute declaration table
            for field in self.__format__[ver]:

                # Field is (name, type, version); version is optional
                fname = field[0]
                ftype = field[1]
                if len(field) >= 3: fver = field[2]
                else: fver = None

                #print '   attr: %s.%s %r' % (name, fname, (offset, ftype, fver))

                # Make an attribute entry in the table:
                #   name -> (offset, type, version)

                attrs[fname] = (offset, ftype, fver)
                #attrs[fname] = AttributeInfo(fname, ftype, fver, offset)

                # Advance base offset for next attribute
                fsize = ftype._sizeof(fver)
                if callable(fsize):
                    isVarType = getattr(ftype, '__is_variable_type__', False)
                    offset = _struct_thunk(fsize, fname, offset, isVarType)
                else:
                    offset = _fadd(offset, fsize)

            # Make __vinfo__ entry
            self.__vinfo__[ver] = (attrs, offset)
    

#----------------------------------------------------------------------------
# StructBase
#----------------------------------------------------------------------------
class StructBase(object):
    __metaclass__ = StructType

    def __init__(self, buffer, start=0, version=None):
        self._buf = buffer
        self._base = start
        if version is not None:
            self._version = version
        else:
            self._version = self.__version__
        try:
            self._attrs, self._size = self.__vinfo__[self._version]
        except KeyError:
            raise VersionError(self.__class__, self._version)

    def _fixoffsets(self):
        # Go through attribute list and compute final types and
        # offsets for all fields.
        offset = 0
        for field in self.__format__[self._version]:
            fname = field[0]
            oldOffset,ftype,fver = self._attrs[fname]
            while getattr(ftype, '__is_variable_type__', False):
                ftype,fver = ftype(self)
            self._attrs[fname] = (offset, ftype, fver)
            fsize = ftype._sizeof(fver)
            if callable(fsize):
                offset += fsize(ftype(self._buf, self._base + offset, fver))
            else:
                offset += fsize
        self._size = offset

    def _offsetof(self, attr):
        try:
            offset, ftype, fver = self._attrs[attr]
        except KeyError:
            raise AttributeError('%r object has no attribute %r' %
                                 (self.__class__.__name__, attr))
        if callable(offset):
            offset = offset(self)
        return offset

    def _getattr(self, attr, useOffset=None):
        try:
            offset, ftype, fver = self._attrs[attr]
        except KeyError:
            raise AttributeError('%r object has no attribute %r' %
                                 (self.__class__.__name__, attr))
        if useOffset is not None:
            offset = useOffset
        elif callable(offset):
            offset = offset(self)

        #print '%s.getAttr: %r %r' % (self.__class__.__name__, attr, offset)
        while getattr(ftype, '__is_variable_type__', False):
            #print '  vartype: %r' % (ftype)
            ftype,fver = ftype(self)

        #print '  type: %r %r' % (ftype, fver)
        try:
            return ftype(self._buf, self._base + offset, fver)
        except:
            print 'Boom: ftype=%r fver=%r buflen=%r pos=%r' % \
                  (ftype, fver, len(self._buf), self._base + offset)
            raise


    def __getattr__(self, attr):
        return self._getattr(attr)

    def _basesizeof(cls, ver=None):
        if ver is None:
            ver = cls.__version__
        try:
            return cls.__vinfo__[ver][1]
        except KeyError:
            raise VersionError(cls, ver)
    _sizeof = classmethod(_basesizeof)
    _basesizeof = staticmethod(_basesizeof)

    def _getsize(self):
        sz = self._sizeof(self._version)
        if callable(sz):
            sz = sz(self)
        return sz

    def _getraw(self):
        return self._buf[self._base : self._base + self._getsize()]

    def __repr__(self):
        return '%s(%r,%r,%r)' % (self.__class__.__name__, self._getraw(),
                                 0, self._version)

    def __str__(self):
        return '<%s v%r>' % (self.__class__.__name__, self._version)

    def _iterattrs(self, cls=None, cver=None):

        #print 'Iter: %s, %s, %s' % (self, cls, cver)
        if cls is None:
            cls = self.__class__
            cver = self._version
        elif cver is None:
            cver = cls.__version__

        #print 'Class: %s, %s' % (cls, cver)

        #print 'Base: %s, %s' % (cls.__bases__, cls.__base_versions__[cver])
            
        for base, bver in zip(cls.__bases__, cls.__base_versions__[cver]):
            #print 'Recurse: %s, %s' % (base, bver)
            if hasattr(base, '_iterattrs'):
                for attr in base._iterattrs(self, base, bver):
                    yield attr
        for field in cls.__format__[cver]:
            yield field[0]

    def __iter__(self):
        return self._iterattrs()

    def __xml__(self):
        return ''.join(['<%s>%s</%s>' % (a, xml(getattr(self,a)), a)
                        for a in self._iterattrs()])
