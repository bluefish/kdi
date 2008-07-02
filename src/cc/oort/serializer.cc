//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/serializer.cc#1 $
//
// Created 2006/05/24
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "serializer.h"
#include <string.h>
#include <algorithm>
#include <iostream>

using namespace oort;

InputStreamSerializer::InputStreamSerializer(RecordStreamHandle const & input,
                      HeaderSpec const * spec) :
    input(input),
    spec(spec),
    headerBuf(new char[spec->headerSize()]),
    headerLeft(0),
    dataLeft(0)
{
}
    
size_t InputStreamSerializer::read(void * dst, size_t sz)
{
    using std::cerr;
    using std::endl;

    char * begin = reinterpret_cast<char *>(dst);
    char * end = begin + sz;
    char * ptr = begin;

    // cerr << "   read: " << dst << " " << sz << endl;

    while(ptr != end)
    {
        if(headerLeft)
        {
            // cerr << "   partial header: " << (void*)ptr << " " << headerLeft << " " << (end-ptr) << endl;

            ptrdiff_t copySz = std::min(ptrdiff_t(headerLeft), end - ptr);
            assert(copySz >= 0 && copySz <= (ptrdiff_t)headerLeft);

            memcpy(ptr, headerBuf.get() + (spec->headerSize() - headerLeft), copySz);
            ptr += copySz;
            headerLeft -= copySz;
        }
        else if(dataLeft)
        {
            // cerr << "   partial data  : " << (void*)ptr << " " << dataLeft << " " << (end-ptr) << endl;

            ptrdiff_t copySz = std::min(ptrdiff_t(dataLeft), end - ptr);
            assert(copySz >= 0 && copySz <= (ptrdiff_t)dataLeft);

            memcpy(ptr, rec.getData() + (rec.getLength() - dataLeft), copySz);
            ptr += copySz;
            dataLeft -= copySz;
        }
        else
        {
            // cerr << "  get record " << (void*)input.get() << endl;

            if(!input->get(rec))
            {
                // cerr << "  no record" << endl;
                break;
            }

            // cerr << "  got record" << endl;

            ptrdiff_t headerSz = spec->headerSize();
            ptrdiff_t dataSz = rec.getLength();
                    
            if(end - ptr >= headerSz)
            {
                // cerr << "   full header: " << (void*)ptr << " " << headerSz << " " << (end-ptr) << endl;
            
                spec->serialize(rec, ptr);
                ptr += headerSz;

                if(end - ptr >= dataSz)
                {
                    // cerr << "   full data  : " << (void*)ptr << " " << dataSz << " " << (end-ptr) << endl;

                    memcpy(ptr, rec.getData(), dataSz);
                    ptr += dataSz;
                }
                else
                {
                    dataLeft = dataSz;
                }
            }
            else
            {
                // cerr << "   save header" << endl;

                spec->serialize(rec, headerBuf.get());
                headerLeft = headerSz;
                dataLeft = dataSz;
            }
        }
    }
            
    return ptr - begin;
}
