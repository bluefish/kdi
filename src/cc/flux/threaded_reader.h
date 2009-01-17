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
#include <warp/functional.h>
#include <flux/stream.h>
#include <vector>
#include <deque>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/scoped_ptr.hpp>

namespace flux {

    template <class T,
              class SizeOfT=warp::unary_constant<size_t,1>
              >
    class ThreadedReader : public Stream<T>
    {
    public:
        typedef ThreadedReader<T, SizeOfT> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        class ReadTask : public warp::Runnable
        {
            warp::WorkerPool & pool;
            base_handle_t & input;
            size_t readAhead;
            size_t readUpTo;

            std::vector<T> buf;
            std::deque<T> syncQ;
            std::deque<T> readQ;
            
            size_t readSoFar;
            bool scheduled;
            bool done;

            SizeOfT sizeOfT;
            
            boost::mutex mutex;
            boost::condition completedRun;

            typedef boost::mutex::scoped_lock lock_t;

        public:
            ReadTask(warp::WorkerPool & pool, base_handle_t & input,
                     size_t readAhead, size_t readUpTo,
                     SizeOfT const & sizeOfT) :
                pool(pool),
                input(input),
                readAhead(readAhead),
                readUpTo(readUpTo),
                readSoFar(0),
                scheduled(true),
                done(false),
                sizeOfT(sizeOfT)
            {
                pool.submit(this);
            }

            void run()
            {
                lock_t lock(mutex);
                size_t readMax = std::min(readAhead - readSoFar, readUpTo);
                lock.unlock();
                
                bool eos = false;
                size_t readSz = 0;
                {
                    T x;
                    while(readSz < readMax)
                    {
                        if(!input->get(x))
                        {
                            eos = true;
                            break;
                        }
                        buf.push_back(x);
                        readSz += sizeOfT(x);
                    }
                }

                lock.lock();

                syncQ.insert(syncQ.end(), buf.begin(), buf.end());
                buf.clear();
                readSoFar += readSz;

                if(eos)
                    done = true;

                if(!done && readSoFar < readAhead)
                    pool.submit(this);
                else
                    scheduled = false;

                completedRun.notify_all();
            }

            bool get(T & x)
            {
                if(readQ.empty())
                {
                    lock_t lock(mutex);
                    while(syncQ.empty() && !done)
                        completedRun.wait(lock);

                    typename std::deque<T>::iterator i;
                    size_t bufferedSz = 0;
                    for(i = syncQ.begin(); i != syncQ.end() &&
                            bufferedSz < readUpTo; ++i)
                    {
                        bufferedSz += sizeOfT(*i);
                    }
                    readQ.insert(readQ.end(), syncQ.begin(), i);
                    syncQ.erase(syncQ.begin(), i);
                    readSoFar -= bufferedSz;

                    if(!scheduled && !done)
                    {
                        scheduled = true;
                        pool.submit(this);
                    }

                    if(readQ.empty())
                        return false;
                }

                x = readQ.front();
                readQ.pop_front();
                return true;
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
        size_t readUpTo;
        boost::scoped_ptr<ReadTask> task;
        SizeOfT sizeOfT;

    public:
        ThreadedReader(warp::WorkerPool & pool,
                       size_t readAhead,
                       size_t readUpTo,
                       SizeOfT const & sizeOfT=SizeOfT()) :
            pool(pool),
            readAhead(readAhead),
            readUpTo(readUpTo),
            sizeOfT(sizeOfT)
        {}

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

                task.reset(
                    new ReadTask(
                        pool, input, readAhead, readUpTo, sizeOfT
                        )
                    );
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

    template <class T, class SizeOfT>
    typename ThreadedReader<T,SizeOfT>::handle_t
    makeThreadedReader(warp::WorkerPool & pool,
                       size_t readAhead,
                       size_t readUpTo,
                       SizeOfT const & sizeOfT)
    {
        typename ThreadedReader<T,SizeOfT>::handle_t p(
            new ThreadedReader<T,SizeOfT>(
                pool, readAhead, readUpTo, sizeOfT
                )
            );
        return p;
    }

    template <class T>
    typename ThreadedReader<T>::handle_t
    makeThreadedReader(warp::WorkerPool & pool,
                       size_t readAhead)
    {
        typename ThreadedReader<T>::handle_t p(
            new ThreadedReader<T>(
                pool, readAhead, 1
                )
            );
        return p;
    }
    

} // namespace flux

#endif // FLUX_THREADED_READER_H
