//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2009-01-09
// 
// This file is part of the flux library.
// 
// The flux library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The flux library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef FLUX_THREADED_READER_H
#define FLUX_THREADED_READER_H

#include <warp/WorkerPool.h>
#include <flux/stream.h>
#include <queue>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/scoped_ptr.hpp>

namespace flux {

    template <class T>
    class ThreadedReader : public Stream<T>
    {
    public:
        typedef ThreadedReader<T> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        class ReadTask : public warp::Runnable
        {
            warp::WorkerPool & pool;
            base_handle_t & input;
            size_t readAhead;

            std::queue<T> inQ;
            
            bool scheduled;
            bool done;
            
            boost::mutex mutex;
            boost::condition completedRun;

            typedef boost::mutex::scoped_lock lock_t;

        public:
            ReadTask(warp::WorkerPool & pool, base_handle_t & input, size_t readAhead) :
                pool(pool),
                input(input),
                readAhead(readAhead),
                scheduled(true),
                done(false)
            {
                pool.submit(this);
            }

            void run()
            {
                T x;
                bool got = input->get(x);

                lock_t lock(mutex);
                if(got)
                    inQ.push(x);
                else
                    done = true;

                if(!done && inQ.size() < readAhead)
                    pool.submit(this);
                else
                    scheduled = false;

                completedRun.notify_all();
            }

            bool get(T & x)
            {
                lock_t lock(mutex);
                while(inQ.empty() && !done)
                    completedRun.wait(lock);

                if(!inQ.empty())
                {
                    x = inQ.front();
                    inQ.pop();
                    if(!scheduled && !done)
                    {
                        scheduled = true;
                        pool.submit(this);
                    }
                    return true;
                }

                return false;
            }

            void cancel()
            {
                lock_t lock(mutex);
                done = true;
                while(scheduled)
                    completedRun.wait(lock);
            }
        };

    private:
        warp::WorkerPool & pool;
        base_handle_t input;
        size_t readAhead;
        boost::scoped_ptr<ReadTask> task;

    public:
        ThreadedReader(warp::WorkerPool & pool, size_t readAhead) :
            pool(pool), readAhead(readAhead) {}

        ~ThreadedReader()
        {
            if(task)
                task->cancel();
        }

        bool get(T & x)
        {
            if(!task)
            {
                if(!input)
                    return false;
                task.reset(new ReadTask(pool, input, readAhead));
            }

            return task->get(x);
        }

        void pipeFrom(base_handle_t const & input)
        {
            if(task)
            {
                task->cancel();
                task.reset();
            }
            
            this->input = input;
        }
    };

} // namespace flux

#endif // FLUX_THREADED_READER_H
