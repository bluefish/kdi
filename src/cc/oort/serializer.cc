//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-05-24
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <oort/serializer.h>
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
