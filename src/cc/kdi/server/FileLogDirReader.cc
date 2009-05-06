//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-05
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

#include <kdi/server/FileLogDirReader.h>
#include <kdi/server/FileLogReader.h>
#include <kdi/server/FileLog.h>
#include <warp/file.h>
#include <warp/dir.h>
#include <warp/fs.h>
#include <warp/strutil.h>
#include <warp/log.h>
#include <queue>
#include <vector>
#include <algorithm>
#include <string>

using namespace kdi::server;
using namespace warp;

//----------------------------------------------------------------------------
// Iter
//----------------------------------------------------------------------------
namespace {

    std::auto_ptr<LogReader> openLogFile(std::string const & fn)
    {
        std::auto_ptr<LogReader> p;

        FilePtr fp = File::input(fn);

        FileLogHeader hdr;
        if(sizeof(hdr) != fp->read(&hdr, sizeof(hdr)))
        {
            log("Failed to read log header: %s", fn);
            return p;
        }

        if(hdr.magic != FileLogHeader::MAGIC)
        {
            log("Bad magic in log header: %s", fn);
            return p;
        }

        switch(hdr.version)
        {
            default:
                log("Unexpected log version %d: %s", hdr.version, fn);
                return p;

            case 0:
                p.reset(new FileLogReaderV0(fp));
                break;
        }
        
        log("Reading log (v%d): %s", hdr.version, fn);
        return p;
    }

    class Iter
        : public LogDirReader::Iterator
    {
    public:
        void push(std::string const & fn)
        {
            logFiles.push(fn);
        }

        virtual std::auto_ptr<LogReader> next()
        {
            std::auto_ptr<LogReader> p;
            while(!logFiles.empty())
            {
                std::string fn = logFiles.front();
                logFiles.pop();
                p = openLogFile(fn);
                if(p.get())
                    return p;
            }
            return p;
        }

    private:
        std::queue<std::string> logFiles;
    };

}



//----------------------------------------------------------------------------
// FileLogReader
//----------------------------------------------------------------------------
std::auto_ptr<LogDirReader::Iterator>
FileLogDirReader::readLogDir(std::string const & logDir) const
{
    DirPtr dir = Directory::open(logDir);
    std::vector<std::string> names;
    for(std::string x; dir->read(x); )
        names.push_back(x);
    dir.reset();

    std::sort(names.begin(), names.end(), PseudoNumericLt());

    std::auto_ptr<Iter> p(new Iter);
    for(std::vector<std::string>::const_iterator i = names.begin();
        i != names.end(); ++i)
    {
        std::string fn = fs::resolve(logDir, *i);
        if(fs::isFile(fn))
            p->push(fn);
    }
    
    return std::auto_ptr<LogDirReader::Iterator>(p);
}

