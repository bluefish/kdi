//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/forward.h $
//
// Created 2008/05/19
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_FORWARD_H
#define KDI_TABLET_FORWARD_H

#include <warp/synchronized.h>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

namespace kdi {

    class Table;
    typedef boost::shared_ptr<Table> TablePtr;

namespace tablet {

    class Tablet;
    class Scanner;
    class SharedLogger;
    class SharedCompactor;
    class ConfigManager;
    class LogWriter;

    typedef boost::shared_ptr<Tablet> TabletPtr;
    typedef boost::shared_ptr<Tablet const> TabletCPtr;

    typedef boost::shared_ptr<Scanner> ScannerPtr;
    typedef boost::shared_ptr<SharedCompactor> SharedCompactorPtr;
    typedef boost::shared_ptr<ConfigManager> ConfigManagerPtr;
    typedef boost::shared_ptr<LogWriter> LogWriterPtr;

    typedef boost::weak_ptr<Tablet> TabletWeakPtr;
    typedef boost::weak_ptr<Scanner> ScannerWeakPtr;

    typedef boost::shared_ptr< warp::Synchronized<SharedLogger> > SharedLoggerSyncPtr;

} // namespace tablet
} // namespace kdi

#endif // KDI_TABLET_FORWARD_H
