//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-08-23
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_SIGNAL_BLOCKER_H
#define WARP_SIGNAL_BLOCKER_H

#include <signal.h>
#include <boost/noncopyable.hpp>
#include <ex/exception.h>

namespace warp
{
    class SignalBlocker : private boost::noncopyable
    {
        sigset_t old_mask;

    public:
        SignalBlocker()
        {
            sigset_t new_mask;
            sigfillset(&new_mask);
            if(int err = pthread_sigmask(SIG_BLOCK, &new_mask, &old_mask))
            {
                using namespace ex;
                raise<RuntimeError>("failed to block signals: %s",
                                    getStdError(err));
            }
        }

        ~SignalBlocker()
        {
            pthread_sigmask(SIG_SETMASK, &old_mask, 0);
        }
    };
}

#endif // WARP_SIGNAL_BLOCKER_H
