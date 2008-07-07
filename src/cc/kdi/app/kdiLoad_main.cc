//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-20
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

#include <kdi/table.h>
#include <warp/xml/xml_parser.h>
#include <warp/options.h>
#include <warp/vstring.h>
#include <boost/scoped_ptr.hpp>
#include <string>
#include <vector>
#include <ctype.h>

#include <boost/spirit.hpp>
#include <warp/parsing/timestamp.h>
#include <warp/parsing/quoted_str.h>
#include <warp/buffer.h>

using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// util
//----------------------------------------------------------------------------
namespace
{
    bool isWhitespace(Buffer const & b)
    {
        for(char const * p = b.position(); p != b.limit(); ++p)
        {
            if(!isspace(*p))
                return false;
        }
        return true;
    }
}

//----------------------------------------------------------------------------
// LoaderBase
//----------------------------------------------------------------------------
class LoaderBase
{
public:
    virtual ~LoaderBase() {}
    virtual void load(FilePtr const & fp) = 0;
};


//----------------------------------------------------------------------------
// TupleLoader
//----------------------------------------------------------------------------
class TupleLoader : public LoaderBase
{
    TablePtr table;
    
public:
    explicit TupleLoader(TablePtr const & table) :
        table(table)
    {
        EX_CHECK_NULL(table);
    }

    void load(FilePtr const & fp)
    {
        string row;
        string col;
        string val;
        int64_t time;

        using namespace boost::spirit;
        using namespace warp::parsing;

        Buffer buf(32 << 20);
        while(buf.read(fp))
        {
            buf.flip();

            parse_info<char *> info = parse(
                buf.position(), buf.limit(),
                *(
                    ch_p('(') >> quoted_str_p[assign_a(row)] >>
                    ch_p(',') >> quoted_str_p[assign_a(col)] >>
                    ch_p(',') >> timestamp_p[assign_a(time)] >>
                    ch_p(',') >> quoted_str_p[assign_a(val)] >>
                    ch_p(')')
                 )[SetCell(table,row,col,time,val)],
                space_p);
            
            buf.position(info.stop);
            buf.compact();
        }

        buf.flip();
        if(buf.remaining() && !isWhitespace(buf))
        {
            raise<IOError>("parse failed at: %s#%d %s", fp->getName(),
                           fp->tell() - buf.remaining(),
                           reprString(buf.position(), buf.remaining()));
        }
    }
    
private:
    struct SetCell
    {
        TablePtr const & table;
        string const & row;
        string const & col;
        int64_t const & time;
        string const & val;

        SetCell(TablePtr const & table, string const & row, string const & col,
                int64_t const & time, string const & val) :
            table(table), row(row), col(col), time(time), val(val)
        {
        }

        template <class Iter>
        void operator()(Iter, Iter) const
        {
            //cerr << "set: " << row << ", " << col << ", "
            //     << time << ", " << val << endl;
            table->set(row, col, time, val);
        }
    };
};


//----------------------------------------------------------------------------
// XmlLoader
//----------------------------------------------------------------------------
class XmlLoader
    : public warp::xml::XmlParser,
      public LoaderBase
{
    TablePtr table;

    vector<char> row;
    vector<char> column;
    vector<char> timestamp;
    vector<char> value;

    vector<char> * collector;

public:
    explicit XmlLoader(TablePtr const & table) :
        table(table), collector(0)
    {
    }

    virtual void startElement(str_data_t const & name, AttrMap const & attrs)
    {
        if(name == "cell")
        {
            row.clear();
            column.clear();
            timestamp.clear();
            value.clear();
        }
        else if(name == "r")
            collector = &row;
        else if(name == "c")
            collector = &column;
        else if(name == "t")
            collector = &timestamp;
        else if(name == "v")
            collector = &value;
    }
    virtual void endElement(str_data_t const & name)
    {
        collector = 0;
        if(name == "cell")
        {
            int64_t ts;
            if(!row.empty() && !column.empty() &&
               parseInt(ts, string_wrapper(timestamp)))
            {
                table->set(row, column, ts, value);
            }
            else
            {
                cerr << "Skipping malformed cell" << endl;
            }
        }
    }
    virtual void characterData(str_data_t const & data)
    {
        if(collector)
            collector->insert(collector->end(), data.begin(), data.end());
    }

    void load(FilePtr const & fp)
    {
        parse(fp);
    }
};

//----------------------------------------------------------------------------
// AolLoader
//----------------------------------------------------------------------------
class AolLoader
    : public warp::xml::XmlParser,
      public LoaderBase
{
    typedef pair<VString,VString> cv_pair;
    typedef vector<cv_pair> cv_vec;

    TablePtr table;
    
    VString row;
    VString column;
    VString value;
    int64_t timestamp;
    cv_vec cells;

    bool setRow;
    bool collectValue;

public:
    explicit AolLoader(TablePtr const & table) :
        table(table),
        timestamp(0),
        setRow(false),
        collectValue(false)
    {
    }

    virtual void startElement(str_data_t const & name, AttrMap const & attrs)
    {
        if(name == "element")
        {
            setRow = (attrs.get("name") == "sbid");
            column = "aol:";
            column += attrs.get("name");
        }
        else if(name == "value")
        {
            collectValue = true;
            value.clear();
        }
    }
    virtual void endElement(str_data_t const & name)
    {
        if(name == "document")
        {
            for(cv_vec::const_iterator i = cells.begin();
                i != cells.end(); ++i)
            {
                table->set(row, i->first, timestamp, i->second);
            }
            cells.clear();
            row.clear();
        }
        else if(name == "element")
        {
            cells.push_back(cv_pair(column, value));
            if(setRow)
            {
                row = value;
                setRow = false;
            }
        }
        collectValue = false;
    }
    virtual void characterData(str_data_t const & data)
    {
        if(collectValue)
            value += data;
    }

    void load(FilePtr const & fp)
    {
        parse(fp);
    }
};


//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op("%prog [options] <input file> ...");
    {
        using namespace boost::program_options;
        op.addOption("table,t", value<string>(), "Destination table");
        op.addOption("tuple", "Use tuple parser (default)");
        op.addOption("xml,x", "Use XML parser");
        op.addOption("aol", "Use AOL Snowchains parser");
    }
    
    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    string arg;
    if(!opt.get("table", arg))
        op.error("need --table");
    
    TablePtr table = Table::open(arg);

    boost::scoped_ptr<LoaderBase> loader;
    if(hasopt(opt, "xml"))
    {
        if(hasopt(opt, "aol") || hasopt(opt, "tuple"))
            op.error("--xml, --aol, and --tuple are mutually exclusive");
        loader.reset(new XmlLoader(table));
    }
    else if(hasopt(opt, "aol"))
    {
        if(hasopt(opt, "tuple"))
            op.error("--xml, --aol, and --tuple are mutually exclusive");
        loader.reset(new AolLoader(table));
    }
    else
        loader.reset(new TupleLoader(table));

    for(ArgumentList::const_iterator ai = args.begin();
        ai != args.end(); ++ai)
    {
        loader->load(File::input(*ai));
    }
    
    table->sync();

    return 0;
}
