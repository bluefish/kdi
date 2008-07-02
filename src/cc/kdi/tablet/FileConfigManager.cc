//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/FileConfigManager.cc $
//
// Created 2008/05/21
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/tablet/FileConfigManager.h>
#include <kdi/tablet/LogReader.h>
#include <kdi/tablet/TabletConfig.h>
#include <kdi/local/disk_table_writer.h>
#include <kdi/local/disk_table.h>
#include <warp/uri.h>
#include <warp/file.h>
#include <warp/fs.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;
using boost::lexical_cast;
using boost::format;

using kdi::local::DiskTableWriter;
using kdi::local::DiskTable;

//----------------------------------------------------------------------------
// FileConfigManager::DirInfo
//----------------------------------------------------------------------------
class FileConfigManager::DirInfo
{
    std::string dir;
    size_t nextIndex;
        
public:
    DirInfo(std::string const & dir) :
        dir(dir), nextIndex(0)
    {
        log("FileConfigManager: open directory %s", dir);

        // Make directory if it doesn't already exist
        fs::makedirs(dir);

        // Acquire lock file
        std::string lockfile = fs::resolve(dir, "lock");
        File::open(lockfile, O_CREAT | O_WRONLY | O_EXCL | O_TRUNC);

        // Init nextIndex
        Config state = loadState();
        if(Config const * n = state.findChild("nextIndex"))
            nextIndex = n->getAs<size_t>();
    }

    ~DirInfo()
    {
        log("FileConfigManager: close directory %s", dir);

        // Release lock file
        fs::remove(fs::resolve(dir, "lock"));
    }

    Config loadState() const
    {
        // Load from "state" file, or default to empty config
        try {
            return Config(fs::resolve(dir, "state"));
        }
        catch(IOError const &) {
            return Config();
        }
    }

    void saveState(Config const & cfg) const
    {
        // Copy config and add nextIndex field
        Config cfgCopy(cfg);
        cfgCopy.set("nextIndex", lexical_cast<string>(nextIndex));

        // Save to "state" file.  Use write and rename.
        string fn = fs::resolve(dir, "state");
        string fnTmp = fs::changeExtension(fn, ".tmp");

        cfgCopy.writeProperties(fnTmp);
        fs::rename(fnTmp, fn, true);
    }

    std::string newFile()
    {
        // Make a new file name and advance nextIndex
        return fs::resolve(dir, lexical_cast<string>(nextIndex++));
    }
};


//----------------------------------------------------------------------------
// FileConfigManager::DirMap
//----------------------------------------------------------------------------
FileConfigManager::DirMap::DirMap(std::string const & rootDir) :
    rootDir(rootDir)
{
}

FileConfigManager::DirInfo &
FileConfigManager::DirMap::getDir(std::string const & name)
{
    // Get the DirInfo for the named tablet.  If it doesn't
    // exist, make a new one.
    map_t::iterator i = infos.find(name);
    if(i == infos.end())
    {
        dir_ptr_t dir(new DirInfo(fs::resolve(rootDir, name)));
        i = infos.insert(make_pair(name, dir)).first;
    }
    return *i->second;
}


//----------------------------------------------------------------------------
// FileConfigManager
//----------------------------------------------------------------------------
FileConfigManager::FileConfigManager(std::string const & rootDir) :
    syncDirMap(rootDir)
{
    log("FileConfigManager: root=%s", rootDir);

}
FileConfigManager::~FileConfigManager()
{
    log("FileConfigManager: destroyed");
}

TabletConfig FileConfigManager::getTabletConfig(std::string const & tabletName)
{
    // Load tablet state
    Config state = LockedPtr<DirMap>(syncDirMap)->getDir(tabletName).loadState();

    // Get URI list from state
    TabletConfig::uri_vec_t uris;
    if(Config const * n = state.findChild("tables"))
    {
        for(size_t i = 0; i < n->numChildren(); ++i)
            uris.push_back(n->getChild(i).get());
    }

    // Put URI list in a TabletConfig
    TabletConfig cfg;
    cfg.setTableUris(uris);
    return cfg;
}

void FileConfigManager::setTabletConfig(std::string const & tabletName, TabletConfig const & cfg)
{
    // Save a state file.

    // Build warp::Config state object from the URI list
    TabletConfig::uri_vec_t const & uris = cfg.getTableUris();
    warp::Config state;
    size_t idx = 0;
    for(TabletConfig::uri_vec_t::const_iterator i = uris.begin();
        i != uris.end(); ++i, ++idx)
    {
        state.set(str(format("tables.i%d") % idx), *i);
    }

    // Save state
    LockedPtr<DirMap>(syncDirMap)->getDir(tabletName).saveState(state);
}

warp::Interval<std::string> FileConfigManager::getTabletRange(std::string const & tabletName)
{
    // Everything, all the time.
    return Interval<string>().setInfinite();
}

std::string FileConfigManager::getNewTabletFile(std::string const & tabletName)
{
    // Get the next index in the directory, advance the number
    return LockedPtr<DirMap>(syncDirMap)->getDir(tabletName).newFile();
}

std::string FileConfigManager::getNewLogFile()
{
    // Get the next index in the directory, advance the number
    // (rewrite config so we don't reuse the log in future instances)
    LockedPtr<DirMap> dirMap(syncDirMap);
    DirInfo & dirInfo = dirMap->getDir("LOGS");
    std::string fn = dirInfo.newFile();
    dirInfo.saveState(warp::Config());
    return fn;
}

std::pair<TablePtr, std::string>
FileConfigManager::openTable(std::string const & uri)
{
    log("FileConfigManager: openTable %s", uri);

    string scheme = uriTopScheme(uri);
    if(scheme == "disk")
    {
        // Disk table: open and return
        TablePtr p(
            new DiskTable(
                uriPushScheme(uriPopScheme(uri), "cache")
                )
            );
        return make_pair(p, uri);
    }
    else if(scheme == "sharedlog")
    {
        // Log table: serialize to a disk table and return that.

        // Get the tablet name from the URI.
        string tabletName = uriDecode(
            uriGetParameter(uri, "tablet"),
            true);

        log("FileConfigManager: loading tablet %s from log %s", tabletName, uri);

        // Read the log cells into a vector
        vector<Cell> cells;
        {
            LogReader reader(
                uriPushScheme(uriPopScheme(uri), "cache"),
                tabletName);
            Cell x;
            while(reader.get(x))
                cells.push_back(x);
        }

        // Sort the cells (use a stable sort since ordering between
        // duplicates is important)
        std::stable_sort(cells.begin(), cells.end());

        // Unique the cells.  We want the last cell of each duplicate
        // sequence instead of the first, so we'll unique using
        // reverse iterators.  The end returned by unique will be the
        // first cell to keep in the forward iterator world.
        vector<Cell>::iterator first = std::unique(
            cells.rbegin(), cells.rend()
            ).base();

        // Write the remaining cells to a disk table
        string diskFn = getNewTabletFile(tabletName);
        {
            log("FileConfigManager: writing new table %s", diskFn);

            DiskTableWriter writer(diskFn, 64<<10);
            for(; first != cells.end(); ++first)
                writer.put(*first);
            writer.close();
        }

        // Reopen disk table for reading
        TablePtr table(new DiskTable(uriPushScheme(diskFn, "cache")));
        
        // Return new table and URI
        return make_pair(table, uriPushScheme(diskFn, "disk"));
    }

    // Unknown scheme
    raise<RuntimeError>("unknown table scheme %s: %s", scheme, uri);
}
