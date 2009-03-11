//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-05
//
// This file is part of KDI.
//
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
//
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_ERRORS_H
#define KDI_SERVER_ERRORS_H

#include <exception>

#define KDI_SERVER_EXCEPTION(NAME, PARENT)              \
    class NAME : public PARENT                          \
    {                                                   \
    public:                                             \
        typedef PARENT super;                           \
        NAME() {}                                       \
        virtual char const * what() const throw()       \
        { return #NAME; }                               \
    }

namespace kdi {
namespace server {

    KDI_SERVER_EXCEPTION(ServerError,                     std::exception);
    KDI_SERVER_EXCEPTION(  BadScanModeError,              ServerError);
    KDI_SERVER_EXCEPTION(  NotLoadedError,                ServerError);
    KDI_SERVER_EXCEPTION(    TableNotLoadedError,         NotLoadedError);
    KDI_SERVER_EXCEPTION(    TabletNotLoadedError,        NotLoadedError);
    KDI_SERVER_EXCEPTION(  CellDataError,                 ServerError);
    KDI_SERVER_EXCEPTION(    BadMagicError,               CellDataError);
    KDI_SERVER_EXCEPTION(    BadChecksumError,            CellDataError);
    KDI_SERVER_EXCEPTION(    BadOrderError,               CellDataError);
    KDI_SERVER_EXCEPTION(  TransactionError,              ServerError);
    KDI_SERVER_EXCEPTION(    MutationConflictError,       TransactionError);
    KDI_SERVER_EXCEPTION(    ScanConflictError,           TransactionError);

} // namespace server
} // namespace kdi

#endif // KDI_SERVER_ERRORS_H
