//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-11
// 
// This file is part of the oort library.
// 
// The oort library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The oort library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef OORT_RECORD_H
#define OORT_RECORD_H

#include <oort/exception.h>
#include <warp/atomic.h>
#include <warp/pack.h>
#include <stdint.h>
#include <string>
// For swap and iter_swap specializations
#include <algorithm>

namespace oort
{
    class Record;
    class RecordStore;
    class Allocator;
    class HeaderSpec;

    class record_size;
}


#define OORT_TYPE(b0, b1, b2, b3) WARP_PACK4(b0, b1, b2, b3)


//----------------------------------------------------------------------------
// RecordStore
//----------------------------------------------------------------------------
class oort::RecordStore
{
    warp::AtomicCounter refCount;

public:
    RecordStore() : refCount(1) {}

protected:
    virtual ~RecordStore() { assert(refCount.get() == 0); }
        
public:
    bool unique() const { return refCount.get() == 1; }
    void ref() { refCount.increment(); }
    void release()
    {
        if(refCount.decrementAndTest())
            delete this;
    }

    virtual uint32_t     getLength(char const * ptr) const = 0;
    virtual uint32_t     getType(char const * ptr) const = 0;
    virtual uint32_t     getFlags(char const * ptr) const = 0;
    virtual uint32_t     getVersion(char const * ptr) const = 0;
    virtual char const * getData(char const * ptr) const = 0;
};


//----------------------------------------------------------------------------
// Record
//----------------------------------------------------------------------------
class oort::Record
{
    RecordStore * store;
    char const * ptr;

public:
    Record() :
        store(0)
    {}
    Record(Record const & r) :
        store(r.store), ptr(r.ptr)
    {
        if(store)
            store->ref();
    }
    Record(RecordStore * store, char const * ptr) :
        store(store), ptr(ptr)
    {
        if(store)
            store->ref();
    }
    ~Record()
    {
        if(store)
            store->release();
    }

    Record const & operator=(Record const & r)
    {
        if(&r != this)
        {
            if(store)
                store->release();
            store = r.store;
            if(store)
                store->ref();
            ptr = r.ptr;
        }
        return *this;
    }

    bool isNull() const { return store == 0; }
    operator bool() const { return !isNull(); }

    /// Reset to null reference
    void release()
    {
        if(store)
        {
            store->release();
            store = 0;
        }
    }

    uint32_t     getLength()   const { return store->getLength (ptr); }
    uint32_t     getType()     const { return store->getType   (ptr); }
    uint32_t     getFlags()    const { return store->getFlags  (ptr); }
    uint32_t     getVersion()  const { return store->getVersion(ptr); }
    char const * getData()     const { return store->getData   (ptr); }

    std::string  getTypeStr()  const;
    std::string  toString()    const;

    /// Copy all record data to a new record with independent storage.
    Record clone() const;

    /// Return data portion casted to the given data type.  This cast
    /// is unchecked.  Use as() or tryAs() for checked casts.
    template <class DataType>
    DataType const * cast() const
    {
        return reinterpret_cast<DataType const *>(getData());
    }

    /// Return data portion casted to the given data type.  The cast
    /// is checked, and a cast to the wrong type or version will raise
    /// a TypeError or VersionError, respectively.
    template <class DataType>
    DataType const * as() const
    {
        if(getType() != DataType::TYPECODE)
            ex::raise<TypeError>
                ("record type mismatch, got: %s", toString().c_str());
        else if(getVersion() != DataType::VERSION)
            ex::raise<VersionError>
                ("record version mismatch, got: %s", toString().c_str());
        else
            return cast<DataType>();
    }

    /// Return data portion casted to the given data type.  The cast
    /// is checked, and a cast to the wrong type or version will
    /// return a null pointer.
    template <class DataType>
    DataType const * tryAs() const
    {
        if(getType() != DataType::TYPECODE)
            return 0;
        else if(getVersion() != DataType::VERSION)
            return 0;
        else
            return cast<DataType>();
    }

    void swap(Record & o)
    {
        std::swap(store, o.store);
        std::swap(ptr, o.ptr);
    }

private:
    // Records are not directly comparable
    bool operator==(Record const & o) const;
    bool operator!=(Record const & o) const;
    bool operator< (Record const & o) const;
    bool operator<=(Record const & o) const;
    bool operator> (Record const & o) const;
    bool operator>=(Record const & o) const;
};

namespace std
{
    template <>
    inline void swap<oort::Record>(oort::Record & a, oort::Record & b)
    {
        a.swap(b);
    }

    template <>
    inline void iter_swap<oort::Record *, oort::Record *>(oort::Record * a, oort::Record * b)
    {
        a->swap(*b);
    }
}


//----------------------------------------------------------------------------
// HeaderSpec
//----------------------------------------------------------------------------
class oort::HeaderSpec
{
public:
    struct Fields
    {
        uint32_t length;
        uint32_t type;
        uint32_t version;
        uint32_t flags;

        Fields() {}
        Fields(Record const & r) :
            length(r.getLength()),
            type(r.getType()),
            version(r.getVersion()),
            flags(r.getFlags()) {}

        template <class DataType>
        void setFromType() {
            length = sizeof(DataType);
            type = DataType::TYPECODE;
            version = DataType::VERSION;
            flags = DataType::FLAGS;
        }
    };

public:
    virtual ~HeaderSpec() {}

    virtual size_t headerSize() const = 0;
    virtual void serialize(Fields const & src, void * dst) const = 0;
    virtual void deserialize(void const * src, Fields & dst) const = 0;
};


//----------------------------------------------------------------------------
// Allocator
//----------------------------------------------------------------------------
class oort::Allocator
{
public:
    virtual ~Allocator() {}
        
    /// Allocate a record with the given record header fields.
    /// Returns a non-const pointer to the record's data section,
    /// guaranteed to be at least f.length bytes long.  Never returns
    /// null -- should throw if there's a memory error.
    virtual char * alloc(Record & r, HeaderSpec::Fields const & f) = 0;


    /// Allocate a record for a fixed-size type.  This convenience
    /// method fills in the appropriate HeaderSpec fields, using
    /// sizeof(T) for the length field.  Do not use this method for
    /// variable length records.
    template <class T>
    char * allocForType(Record & r)
    {
        HeaderSpec::Fields f;
        f.setFromType<T>();
        return alloc(r, f);
    }

    /// Construct convenience methods:
    ///   Allocate and construct a simple fixed-size record type.
    ///   This only works for fixed-size data types, where the record
    ///   length equals sizeof(T).  Note that once constructed, the
    ///   record is treated as const-data, just like any other record,
    ///   so the constructor must fully initialize the record data.
    ///   Variable-size records and records with more complicated
    ///   initialization should be constructed with a RecordBuilder.

    /// Construct using the default constructor.  Probably not useful.
    template <class T>
    void construct(Record & r) {
        new (allocForType<T>(r)) T();
    }

    /// Construct using single parameter constructor.
    template <class T, class A1>
    void construct(Record & r, A1 a1) {
        new (allocForType<T>(r)) T(a1);
    }

    /// Construct using two parameter constructor.
    template <class T, class A1, class A2>
    void construct(Record & r, A1 a1, A2 a2) {
        new (allocForType<T>(r)) T(a1, a2);
    }

    /// Construct using three parameter constructor.
    template <class T, class A1, class A2, class A3>
    void construct(Record & r, A1 a1, A2 a2, A3 a3) {
        new (allocForType<T>(r)) T(a1, a2, a3);
    }

    /// Construct using four parameter constructor.
    template <class T, class A1, class A2, class A3, class A4>
    void construct(Record & r, A1 a1, A2 a2, A3 a3, A4 a4) {
        new (allocForType<T>(r)) T(a1, a2, a3, a4);
    }

    /// Construct using five  parameter constructor.
    template <class T, class A1, class A2, class A3, class A4, class A5>
    void construct(Record & r, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5) {
      new (allocForType<T>(r)) T(a1, a2, a3, a4, a5);
    }

    /// Construct using six parameter constructor.
    template <class T, class A1, class A2, class A3, class A4, class A5, class A6>
    void construct(Record & r, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6) {
      new (allocForType<T>(r)) T(a1, a2, a3, a4, a5, a6);
    }

};


//----------------------------------------------------------------------------
// record_size
//----------------------------------------------------------------------------
struct oort::record_size
{
    size_t operator()(Record const & r) const
    {
        return sizeof(Record) + r.getLength();
    }
};


#endif // OORT_RECORD_H
