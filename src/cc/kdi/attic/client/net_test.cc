//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/attic/client/net_test.cc#1 $
//
// Created 2007/11/12
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "ex/exception.h"
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include "warp/strref.h"
#include <iostream>
#include <boost/format.hpp>
#include <boost/system/system_error.hpp>
#include <boost/thread.hpp>

#include <boost/future/future.hpp>

//using namespace kdi;
//using namespace kdi::client;

namespace asio = boost::asio;
using asio::ip::tcp;
using boost::system::error_code;
using namespace warp;
using namespace std;
using boost::format;

using boost::future;
using boost::promise;

using namespace ex;


class Resolver
{
    tcp::resolver r;

    void handle_resolve(error_code err, tcp::resolver::iterator it)
    {
        if(err)
            cerr << "error: " << err << " -- " << err.message() << endl;
        else
        {
            for(tcp::resolver::iterator end; it != end; ++it)
            {
                cerr << format("%s %s: %s")
                    % it->host_name()
                    % it->service_name()
                    % it->endpoint()
                     << endl;
            }
        }
    }

    void handle_future_resolve(promise<tcp::resolver::iterator> p,
                               error_code err, tcp::resolver::iterator it)
    {
        if(err)
            p.set_exception(boost::system::system_error(err));
        else
            p.set(it);
    }

public:
    Resolver(asio::io_service & io) : r(io) {}

    void resolve(strref_t host, strref_t port)
    {
        r.async_resolve(
            tcp::resolver::query(str(host), str(port)),
            boost::bind(
                &Resolver::handle_resolve,
                this,
                asio::placeholders::error,
                asio::placeholders::iterator
                )
            );
    }

    future<tcp::resolver::iterator> future_resolve(strref_t host, strref_t port)
    {
        promise<tcp::resolver::iterator> p;

        r.async_resolve(
            tcp::resolver::query(str(host), str(port)),
            boost::bind(
                &Resolver::handle_future_resolve,
                this,
                p,
                asio::placeholders::error,
                asio::placeholders::iterator
                )
            );

        return p;
    }
};


class WorkerPool
{
    typedef boost::shared_ptr<boost::thread> thread_ptr_t;
    typedef std::vector<thread_ptr_t> thread_vec_t;

    asio::io_service io;
    asio::io_service::work work;
    boost::thread_group threads;

    void run()
    {
        try {
            io.run();
        }
        catch(...) {
            std::terminate();
        }
    }

public:
    explicit WorkerPool(size_t n) :
        io(), work(io)
    {
        if(!n)
            raise<ValueError>("zero workers");

        for(size_t i = 0; i < n; ++i)
        {
            threads.create_thread(
                boost::bind(
                    &WorkerPool::run,
                    this)
                );
        }
    }

    ~WorkerPool()
    {
        io.stop();
        threads.join_all();
    }

    asio::io_service & get_io_service()
    {
        return io;
    }
};


int main(int ac, char ** av)
{
    WorkerPool pool(2);
    Resolver r(pool.get_io_service());

    typedef std::pair<std::string, std::string> name_t;
    typedef tcp::resolver::iterator iter_t;
    typedef future<iter_t> future_iter_t;
    typedef std::pair<name_t, future_iter_t> value_t;
    typedef std::vector<value_t> vec_t;

    cout << "Setting futures" << endl;

    vec_t vec;
    for(int i = 2; i < ac; i += 2)
    {
        name_t n(av[i-1], av[i]);
        future_iter_t f = r.future_resolve(n.first, n.second);
        vec.push_back(value_t(n,f));
    }

    cout << "Reading futures" << endl;

    for(vec_t::const_iterator i = vec.begin(); i != vec.end(); ++i)
    {
        cout << format("%s %s:") % i->first.first % i->first.second << flush;
        try {
            for(tcp::resolver::iterator end, it = i->second.get();
                it != end; ++it)
            {
                cout << ' ' << it->endpoint();
            }
            cout << endl;
        }
        catch(std::exception const & ex) {
            cout << " error: " << ex.what() << endl;
        }
    }

    return 0;
}
