//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-07-14
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

#include <kdi/tablet/MetaConfigManager.h>
#include <kdi/tablet/TabletName.h>
#include <kdi/tablet/LogReader.h>
#include <kdi/tablet/TabletConfig.h>
#include <kdi/local/disk_table_writer.h>
#include <kdi/local/disk_table.h>
#include <warp/config.h>
#include <warp/memfile.h>
#include <warp/uri.h>
#include <warp/file.h>
#include <warp/fs.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

using kdi::local::DiskTableWriter;
using kdi::local::DiskTable;

namespace
{
    /// Get the table URI relative to the data root
    std::string resolveTableUri(std::string const & root,
                                std::string const & tableUri)
    {
        // Remove the table scheme, resolve the rest relative to the
        // root, and put the table scheme back
        Uri uri(tableUri);
        return uriPushScheme(
            fs::resolve(root, uri.popScheme().toString()),
            uri.topScheme());
    }

    /// Remove the data root from a table URI
    std::string unrootTableUri(std::string const & root,
                               std::string const & tableUri)
    {
        // Make sure root isn't empty
        if(root.empty())
            raise<ValueError>("empty root");

        // We depend on having a trailing slash.  Add it if we need to.
        if(root[root.size()-1] != '/')
            return unrootTableUri(root + '/', tableUri);

        // Separate prefix scheme from the rest of the URI
        Uri uri(tableUri);
        StringRange rest = uri.popScheme();

        // Make sure the URI contains the root
        if(!boost::algorithm::starts_with(rest, root))
        {
            raise<ValueError>("table URI not under root '%s': %s",
                              root, tableUri);
        }
        
        // Remove the root
        rest = StringRange(rest.begin() + root.size(), rest.end());

        // Should have a non-empty relative path left over
        if(!rest || rest.front() == '/')
        {
            raise<ValueError>("table URI invalid after removing "
                              "root '%s': %s", root, tableUri);
        }
    
        // Put the table scheme back on and return
        return uriPushScheme(rest, uri.topScheme());
    }
}


//----------------------------------------------------------------------------
// MetaConfigManager::FixedAdapter
//----------------------------------------------------------------------------
class MetaConfigManager::FixedAdapter
    : public ConfigManager
{
    MetaConfigManagerPtr base;

    string getStatePath(std::string const & tabletName) const
    {
        return fs::resolve(
            fs::resolve(base->rootDir, tabletName),
            "state");
    }

public:
    explicit FixedAdapter(MetaConfigManagerPtr const & base) :
        base(base) {}

    virtual TabletConfig getTabletConfig(std::string const & tabletName)
    {
        // Get the file name of the state file
        string stateFn = getStatePath(tabletName);

        // Try to load the file
        FilePtr fp;
        try {
            fp = File::input(stateFn);
        }
        catch(IOError const &) {
            // If it doesn't exist, return an empty config
            return TabletConfig(
                Interval<string>().setInfinite(),
                vector<string>(),
                base->getServerName());
        }

        // Read the contents of the file
        vector<char> val(256<<10);
        size_t sz = fp->read(&val[0], val.size());
        val.erase(val.begin() + sz, val.end());

        // Fake a TabletName for the fixed table
        TabletName name(
            tabletName,
            IntervalPoint<string>("", PT_INFINITE_UPPER_BOUND));

        // Make a fake config cell for the fixed table, and get the
        // TabletConfig from that.
        // XXX: the common code should be refactored so that this
        // hackery isn't required.
        return base->getConfigFromCell(
            makeCell(
                name.getEncoded(),
                "config",
                0,
                val)
            );
    }

    virtual void setTabletConfig(std::string const & tabletName, TabletConfig const & cfg)
    {
        if(!cfg.getTabletRows().isInfinite())
            raise<ValueError>("fixed tablet config shouldn't have a "
                              "restricted row range");

        // Get a string value for the config
        string val = base->getConfigCellValue(cfg);

        // Create a temp file
        string dir = fs::resolve(base->rootDir, tabletName);
        fs::makedirs(dir);
        std::pair<FilePtr, string> tmp = File::openUnique(
            fs::resolve(dir, "$UNIQUE"));

        try {
            // Write config to a temp file
            size_t sz = tmp.first->write(val.c_str(), val.size());
            if(sz < val.size())
                raise<RuntimeError>("couldn't write tmp config");
            tmp.first->close();

            // Replace config with temp file
            fs::rename(tmp.second, getStatePath(tabletName), true);
        }
        catch(...) {
            // Clean up temp file if there's a problem
            fs::remove(tmp.second);
            throw;
        }
    }

    virtual std::string getNewTabletFile(std::string const & tabletName)
    {
        return base->getNewFile(tabletName);
    }

    virtual std::string getNewLogFile()
    {
        return base->getNewLogFile();
    }

    virtual std::pair<TablePtr, std::string> openTable(std::string const & uri)
    {
        return base->openTable(uri);
    }
};

//----------------------------------------------------------------------------
// MetaConfigManager
//----------------------------------------------------------------------------
MetaConfigManager::MetaConfigManager(std::string const & rootDir,
                                     std::string const & serverName,
                                     std::string const & metaTableUri) :
    rootDir(rootDir),
    serverName(serverName),
    metaTableUri(metaTableUri)
{
    log("MetaConfigManager: root=%s, server=%s metaTable=%s",
        rootDir, serverName, metaTableUri);

    if(serverName.empty())
        raise<ValueError>("empty server name");
}

MetaConfigManager::~MetaConfigManager()
{
    log("MetaConfigManager: destroyed");
}

TabletConfig MetaConfigManager::getTabletConfig(std::string const & encodedName)
{
    // Get the config cell for the named tablet out of the META table
    ostringstream pred;
    pred << "row=" << reprString(encodedName) << " and column='config'";
    CellStreamPtr scanner = getMetaTable()->scan(pred.str());
    Cell configCell;
    if(!scanner->get(configCell))
        raise<RuntimeError>("Tablet has no config in META table: %s",
                            reprString(encodedName));
    
    // Load the config
    return getConfigFromCell(configCell);
}

void MetaConfigManager::setTabletConfig(std::string const & encodedName, TabletConfig const & cfg)
{
    // Parse the tablet name
    TabletName tabletName(encodedName);

    // Do some sanity checking on the config rows
    Interval<string> const & rows = cfg.getTabletRows();
    if(rows.getUpperBound() != tabletName.getLastRow())
        raise<ValueError>("config upper bound does not match tablet name");
    if(cfg.getServer() != serverName)
        raise<ValueError>("config server is not this server");
    
    // Write the tablet config cell
    TablePtr table = getMetaTable();
    table->set(encodedName, "config", 0, getConfigCellValue(cfg));
    table->sync();
}

std::string MetaConfigManager::getNewTabletFile(std::string const & encodedName)
{
    TabletName tabletName(encodedName);
    return getNewFile(tabletName.getTableName());
}

std::string MetaConfigManager::getNewLogFile()
{
    return getNewFile("LOGS");
}

std::string MetaConfigManager::getNewFile(std::string const & tableDir) const
{
    string dir = fs::resolve(rootDir, tableDir);
    
    // XXX: this should be cached -- only need to make the directory
    // once per table
    fs::makedirs(dir);
    
    return File::openUnique(fs::resolve(dir, "$UNIQUE")).second;
}

std::pair<TablePtr, std::string>
MetaConfigManager::openTable(std::string const & uri)
{
    log("MetaConfigManager: openTable %s", uri);

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

        log("MetaConfigManager: loading tablet %s from log %s", tabletName, uri);

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
        string diskFn = getNewTabletFile(tabletName); // XXX : fail
        {
            log("MetaConfigManager: writing new table %s", diskFn);

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


ConfigManagerPtr MetaConfigManager::getFixedAdapter()
{
    ConfigManagerPtr p(new FixedAdapter(shared_from_this()));
    return p;
}

TabletConfig MetaConfigManager::getConfigFromCell(Cell const & configCell) const
{
    // Parse the tablet name from cell row
    TabletName tabletName(configCell.getRow());

    // Parse config from cell value
    StringRange val = configCell.getValue();
    FilePtr fp(new MemFile(val.begin(), val.size()));
    Config state(fp);

    // Get URI list from state
    vector<string> uris;
    if(Config const * n = state.findChild("tables"))
    {
        for(size_t i = 0; i < n->numChildren(); ++i)
        {
            uris.push_back(
                resolveTableUri(rootDir, n->getChild(i).get())
                );
        }
    }

    // Get the row lower bound
    IntervalPoint<string> minRow;
    if(Config const * n = state.findChild("minRow"))
        minRow = IntervalPoint<string>(n->get(), PT_EXCLUSIVE_LOWER_BOUND);
    else
        minRow = IntervalPoint<string>(string(), PT_INFINITE_LOWER_BOUND);

    // Return the TabletConfig
    return TabletConfig(
        Interval<string>(minRow, tabletName.getLastRow()),
        uris,
        state.get("server", "")
        );
}

std::string MetaConfigManager::getConfigCellValue(TabletConfig const & config) const
{
    // Serialize tablet config using warp::Config
    warp::Config state;

    // Add URI list
    vector<string> const & uris = config.getTableUris();
    size_t idx = 0;
    for(vector<string>::const_iterator i = uris.begin();
        i != uris.end(); ++i, ++idx)
    {
        state.set(
            str(format("tables.i%d") % idx),
            unrootTableUri(rootDir, *i)
            );
    }
    
    // Set the min row
    Interval<string> const & rows = config.getTabletRows();
    switch(rows.getLowerBound().getType())
    {
        case PT_INFINITE_LOWER_BOUND:
            // Set nothing
            break;

        case PT_EXCLUSIVE_LOWER_BOUND:
            // Set minRow
            state.set("minRow", rows.getLowerBound().getValue());
            break;

        default:
            raise<ValueError>("config has invalid lower bound");
    }

    // Set the server
    state.set("server", config.getServer());

    // Serialize the config
    ostringstream oss;
    oss << state;
    return oss.str();
}

TablePtr const & MetaConfigManager::getMetaTable() const
{
    log("Getting META table");

    if(!metaTable)
    {
        boost::mutex::scoped_lock lock(metaTableMutex);
        if(!metaTable)
        {
            log("Connecting to META table: %s", metaTableUri);
            metaTable = Table::open(metaTableUri);
            log("Connected to META table: %s", metaTableUri);
        }
    }

    return metaTable;
}
