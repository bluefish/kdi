//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-06-11
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

#include <warp/memfs.h>
#include <warp/string_range.h>
#include <warp/file.h>
#include <unittest/main.h>
#include <ex/exception.h>
#include <string>

extern "C" {
#include <string.h>
}


using namespace warp;
using namespace ex;
using namespace std;

namespace
{
    class CleanFixture
    {
    public:
        CleanFixture()
        {
            MemoryFilesystem::get()->clear();
        }

        ~CleanFixture()
        {
            MemoryFilesystem::get()->clear();
        }
    };
}

BOOST_AUTO_TEST_CASE(fs_registration)
{
    FsPtr a = Filesystem::get("memfs:foo");
    FsPtr b = Filesystem::get("memfs:bar");

    BOOST_CHECK(a && a.get() == b.get());
}

BOOST_AUTO_TEST_CASE(empty_fs)
{
    BOOST_CHECK(!fs::exists("memfs:foo"));
    BOOST_CHECK(!fs::isFile("memfs:foo"));
    BOOST_CHECK(!fs::isDirectory("memfs:foo"));
    BOOST_CHECK_THROW(fs::filesize("memfs:foo"), IOError);
    BOOST_CHECK_THROW(fs::isEmpty("memfs:foo"), IOError);
    BOOST_CHECK_THROW(fs::modificationTime("memfs:foo"), IOError);
    BOOST_CHECK_THROW(fs::creationTime("memfs:foo"), IOError);
    BOOST_CHECK_THROW(fs::accessTime("memfs:foo"), IOError);
    BOOST_CHECK_EQUAL(fs::remove("memfs:foo"), false);
}

BOOST_AUTO_TEST_CASE(file_test)
{
    CleanFixture fx;
    string fn("memfs:foo");
    char buf[1024];
    char const testData[] =
        "Here are some bytes.\n"
        "Yay!\n"
        "\n"
        "Whee"
        ;
    size_t const dataLen = strlen(testData);

    // Nothing there, can't open it for reading
    BOOST_CHECK(!fs::exists(fn));
    BOOST_CHECK_THROW(File::input(fn), IOError);

    // Open for writing, then reading
    FilePtr w = File::output(fn);
    FilePtr r = File::input(fn);

    // Now the file is there, empty
    BOOST_CHECK(fs::exists(fn));
    BOOST_CHECK_EQUAL(fs::filesize(fn), size_t(0));
    BOOST_CHECK_EQUAL(r->read(buf, sizeof(buf)), 0ul);

    // Write some data to it and note the filesize change
    BOOST_CHECK(fs::exists(fn));
    BOOST_CHECK_EQUAL(w->write(testData, dataLen), dataLen);
    BOOST_CHECK_EQUAL(fs::filesize(fn), dataLen);
    
    // Read data out of the file, make sure it is the same data
    BOOST_CHECK_EQUAL(r->read(buf, sizeof(buf)), dataLen);
    BOOST_CHECK_EQUAL(testData, StringRange(buf, buf+dataLen));

    // Can't read further
    BOOST_CHECK_EQUAL(r->read(buf, sizeof(buf)), 0ul);

    // Rewind and try again
    r->seek(0);

    // Read data out of the file, make sure it is the same data
    BOOST_CHECK_EQUAL(r->read(buf, sizeof(buf)), dataLen);
    BOOST_CHECK_EQUAL(StringRange(buf, buf+dataLen), testData);

    // Rewind again
    r->seek(0);

    // Read file by lines
    BOOST_CHECK_EQUAL(r->readline(buf, sizeof(buf)), strlen("Here are some bytes.\n"));
    BOOST_CHECK_EQUAL(buf, "Here are some bytes.\n");
    BOOST_CHECK_EQUAL(r->readline(buf, sizeof(buf)), strlen("Yay!\n"));
    BOOST_CHECK_EQUAL(buf, "Yay!\n");
    BOOST_CHECK_EQUAL(r->readline(buf, sizeof(buf)), strlen("\n"));
    BOOST_CHECK_EQUAL(buf, "\n");
    BOOST_CHECK_EQUAL(r->readline(buf, sizeof(buf)), strlen("Whee"));
    BOOST_CHECK_EQUAL(buf, "Whee");
    BOOST_CHECK_EQUAL(r->readline(buf, sizeof(buf)), 0ul);

    // Close file handles and make sure the file still exists
    w->close();
    r->close();

    BOOST_CHECK(fs::exists(fn));
    BOOST_CHECK_EQUAL(fs::filesize(fn), dataLen);

    // Delete file handles and make sure the file still exists
    w.reset();
    r.reset();

    BOOST_CHECK(fs::exists(fn));
    BOOST_CHECK_EQUAL(fs::filesize(fn), dataLen);

    // Remove file and make sure it doesn't exist
    BOOST_CHECK_EQUAL(fs::remove(fn), true);
    BOOST_CHECK(!fs::exists(fn));
}

BOOST_AUTO_TEST_CASE(dir_test)
{
    CleanFixture fixture;
    string fn("memfs:foo");

    // Should be nothing here
    BOOST_CHECK(!fs::exists(fn));
    BOOST_CHECK(!fs::isFile(fn));
    BOOST_CHECK(!fs::isDirectory(fn));

    // Make a directory -- shouldn't already exist
    BOOST_CHECK_EQUAL(fs::mkdir(fn), true);

    // Should exist and be a directory
    BOOST_CHECK( fs::exists(fn));
    BOOST_CHECK(!fs::isFile(fn));
    BOOST_CHECK( fs::isDirectory(fn));

    // Make it again
    BOOST_CHECK_EQUAL(fs::mkdir(fn), false);

    // Should exist and be a directory - same same
    BOOST_CHECK( fs::exists(fn));
    BOOST_CHECK(!fs::isFile(fn));
    BOOST_CHECK( fs::isDirectory(fn));

    // We shouldn't be able to make a file named the same thing
    BOOST_CHECK_THROW(File::output(fn), IOError);
    
    // Remove is unimplemented for directories
    BOOST_CHECK_THROW(fs::remove(fn), NotImplementedError);
}

BOOST_AUTO_TEST_CASE(multidir_test)
{
    CleanFixture fixture;
    
    fs::makedirs("memfs:/one/two/three");

    BOOST_CHECK(fs::isDirectory("memfs:/"));
    BOOST_CHECK(fs::isDirectory("memfs:/one"));
    BOOST_CHECK(fs::isDirectory("memfs:/one/"));
    BOOST_CHECK(fs::isDirectory("memfs:/one/two"));
    BOOST_CHECK(fs::isDirectory("memfs:/one/two/"));
    BOOST_CHECK(fs::isDirectory("memfs:/one/two/three"));
    BOOST_CHECK(fs::isDirectory("memfs:/one/two/three/"));
}

BOOST_AUTO_TEST_CASE(rename_test)
{
    CleanFixture fixture;

    // Can't rename unless source exists
    BOOST_CHECK_THROW(fs::rename("memfs:/nothing", "memfs:/something", false), IOError);

    // Make a file
    File::output("memfs:/f1");
    BOOST_CHECK(fs::isFile("memfs:/f1"));

    // Rename it -- no overwrite
    fs::rename("memfs:/f1", "memfs:/f2", false);
    BOOST_CHECK(!fs::exists("memfs:/f1"));
    BOOST_CHECK(fs::isFile("memfs:/f2"));

    // Make another file
    File::output("memfs:/f3");
    BOOST_CHECK(fs::isFile("memfs:/f3"));
    
    // Rename shouldn't overwrite unless told to
    BOOST_CHECK_THROW(fs::rename("memfs:/f2", "memfs:/f3", false), IOError);
    BOOST_CHECK(fs::isFile("memfs:/f2"));
    BOOST_CHECK(fs::isFile("memfs:/f3"));
    
    // Rename it with overwrite
    fs::rename("memfs:/f2", "memfs:/f3", true);
    BOOST_CHECK(!fs::exists("memfs:/f2"));
    BOOST_CHECK(fs::isFile("memfs:/f3"));
    
    // Make a directory
    fs::mkdir("memfs:/d1");
    BOOST_CHECK(fs::isDirectory("memfs:/d1"));

    // Rename it
    fs::rename("memfs:/d1", "memfs:/d2", false);
    BOOST_CHECK(!fs::exists("memfs:/d1"));
    BOOST_CHECK(fs::isDirectory("memfs:/d2"));

    // Make another directory
    fs::mkdir("memfs:/d3");
    BOOST_CHECK(fs::isDirectory("memfs:/d3"));
    
    // Rename shouldn't overwrite unless told to
    BOOST_CHECK_THROW(fs::rename("memfs:/d2", "memfs:/d3", false), IOError);
    BOOST_CHECK(fs::isDirectory("memfs:/d2"));
    BOOST_CHECK(fs::isDirectory("memfs:/d3"));

    // Rename shouldn't overwrite directories
    BOOST_CHECK_THROW(fs::rename("memfs:/d2", "memfs:/d3", true), IOError);
    BOOST_CHECK(fs::isDirectory("memfs:/d2"));
    BOOST_CHECK(fs::isDirectory("memfs:/d3"));

    // Rename shouldn't overwrite directories
    BOOST_CHECK_THROW(fs::rename("memfs:/f3", "memfs:/d3", true), IOError);
    BOOST_CHECK(fs::isFile("memfs:/f3"));
    BOOST_CHECK(fs::isDirectory("memfs:/d3"));

    // Rename shouldn't overwrite files with directories
    BOOST_CHECK_THROW(fs::rename("memfs:/d3", "memfs:/f3", true), IOError);
    BOOST_CHECK(fs::isDirectory("memfs:/d3"));
    BOOST_CHECK(fs::isFile("memfs:/f3"));

    // Rename shouldn't overwrite files with themselves
    BOOST_CHECK_THROW(fs::rename("memfs:/f3", "memfs:/f3", true), IOError);
    BOOST_CHECK(fs::isFile("memfs:/f3"));
}

