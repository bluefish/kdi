//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-19
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

#ifndef KDI_TABLET_FORWARD_H
#define KDI_TABLET_FORWARD_H

#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

namespace kdi {

    class Table;
    typedef boost::shared_ptr<Table> TablePtr;

namespace tablet {

    class Tablet;
    class SuperTablet;
    class Scanner;
    class SuperScanner;
    class Fragment;
    class SharedLogger;
    class SharedCompactor;
    class WorkQueue;
    class ConfigManager;
    class FileTracker;
    class LogWriter;
    class TabletConfig;

    typedef boost::shared_ptr<Tablet> TabletPtr;
    typedef boost::shared_ptr<Tablet const> TabletCPtr;

    typedef boost::shared_ptr<SuperTablet> SuperTabletPtr;
    typedef boost::shared_ptr<SuperTablet const> SuperTabletCPtr;

    typedef boost::shared_ptr<Fragment> FragmentPtr;
    typedef boost::shared_ptr<Scanner> ScannerPtr;
    typedef boost::shared_ptr<SuperScanner> SuperScannerPtr;
    typedef boost::shared_ptr<SharedLogger> SharedLoggerPtr;
    typedef boost::shared_ptr<SharedCompactor> SharedCompactorPtr;
    typedef boost::shared_ptr<WorkQueue> WorkQueuePtr;
    typedef boost::shared_ptr<ConfigManager> ConfigManagerPtr;
    typedef boost::shared_ptr<FileTracker> FileTrackerPtr;
    typedef boost::shared_ptr<LogWriter> LogWriterPtr;

    typedef boost::weak_ptr<Tablet> TabletWeakPtr;
    typedef boost::weak_ptr<Scanner> ScannerWeakPtr;
    typedef boost::weak_ptr<SuperScanner> SuperScannerWeakPtr;
    typedef boost::weak_ptr<Fragment> FragmentWeakPtr;

} // namespace tablet
} // namespace kdi

#endif // KDI_TABLET_FORWARD_H
