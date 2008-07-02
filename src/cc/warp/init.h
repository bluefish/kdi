//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/init.h#1 $
//
// Created 2006/08/13
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_INIT_H
#define WARP_INIT_H

#ifndef NDEBUG
#include <stdio.h>
#endif

/// Get the name of an init function.
#define WARP_INIT_NAME(objRoot) ridiculous_init_func_for_##objRoot

/// Get the name of the variable guarding against multiple invokations
/// of an init function.
#define WARP_INIT_VAR_NAME(objRoot) ridiculous_init_var_for_##objRoot

/// Get the name of the constructor function responsible for calling
/// the init function at dynamic library load time.
#define WARP_INIT_AUTO_NAME(objRoot) ridiculous_init_autocall_for_##objRoot

/// Log that an init function has been called.  Only enabled in debug
/// builds.
#ifndef NDEBUG
#define WARP_LOG_INIT(objRoot) fprintf(stderr, "WARP_INIT: %s\n", #objRoot)
#else
#define WARP_LOG_INIT(objRoot) (void)0
#endif

/// Call an init function defined with WARP_DEFINE_INIT.  Each init
/// function can only be called once.  Subsequent invokations through
/// WARP_CALL_INIT will do nothing.
#define WARP_CALL_INIT(objRoot)                                 \
    do {                                                        \
        extern void WARP_INIT_NAME(objRoot)(void);              \
        extern volatile bool WARP_INIT_VAR_NAME(objRoot);       \
        if(!(WARP_INIT_VAR_NAME(objRoot))) {                    \
            WARP_LOG_INIT(objRoot);                             \
            WARP_INIT_NAME(objRoot)();                          \
            WARP_INIT_VAR_NAME(objRoot) = true;                 \
        }                                                       \
    } while(0)

/// Define the init function.  Also generate an function to
/// automatically call it in dynamically loaded libraries.  The init
/// function will have to be called manually with WARP_CALL_INIT for
/// statically linked libraries.
#define WARP_DEFINE_INIT(objRoot)                       \
    volatile bool WARP_INIT_VAR_NAME(objRoot) = false;  \
    extern void WARP_INIT_AUTO_NAME(objRoot)(void)      \
       __attribute__ ((constructor));                   \
    void WARP_INIT_AUTO_NAME(objRoot)(void) {           \
        WARP_CALL_INIT(objRoot);                        \
    }                                                   \
    void WARP_INIT_NAME(objRoot)(void)


namespace warp
{
    void initModule();
}

#endif // WARP_INIT_H
