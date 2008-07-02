//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: signal_blocker.h $
//
// Created 2007/08/23
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_SIGNAL_BLOCKER_H
#define WARP_SIGNAL_BLOCKER_H

#include <signal.h>
#include <boost/noncopyable.hpp>
#include "ex/exception.h"

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
