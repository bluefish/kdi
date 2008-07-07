//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-04-28
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

#ifndef FLUX_ASYNCSORT_H
#define FLUX_ASYNCSORT_H

#include <flux/stream.h>
#include <flux/sequence.h>
#include <flux/merge.h>

#include <warp/util.h>
#include <warp/syncqueue.h>

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>

#include <vector>
#include <algorithm>
#include <utility>
#include <functional>
#include <iostream>

namespace flux
{
    //------------------------------------------------------------------------
    // AsyncSortParams
    //------------------------------------------------------------------------
    struct AsyncSortParams
    {
        /// Number of worker threads to use for sorting.
        size_t nSortWorkers;

        /// Dispatch an async sort when input buffer reaches \c
        /// dispatchThreshold bytes.
        size_t dispatchThreshold;

        /// Limit input buffer allocation to \c maxBuffers when
        /// there's a pending async merge.  To prevent deadlock,
        /// buffers will continue to be allocated if there is no
        /// pending merge.  This parameter intended to allow input
        /// throttling when \c asyncOutput is used.
        size_t maxBuffers;

        /// Allow merge and output to be serviced a worker thread
        /// instead of the main thread.  The implication is that
        /// flush() will return immediately and more data can be
        /// put().  However, subsequent calls to put() may block and
        /// wait for available buffers to be released by the pending
        /// merge.
        bool asyncOutput;

        AsyncSortParams() :
            nSortWorkers(2),
            dispatchThreshold(64 << 20),
            maxBuffers(0),
            asyncOutput(true)
        {
        }
    };

    //------------------------------------------------------------------------
    // AsyncSort
    //------------------------------------------------------------------------
    /// @param T Stream data type
    /// @param Lt Less-than binary functor on type \c T.
    /// @param Sz Size-of unary functor on type \c T.
    template <class T, class Lt=std::less<T>, class Sz=warp::size_of >
    class AsyncSort : public Stream<T>
    {
    public:
        typedef AsyncSort<T,Lt,Sz> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        typedef std::vector<T> vec_t;
        typedef Sequence< vec_t, warp::back_pos<vec_t> > buf_t;
        typedef typename buf_t::handle_t buf_handle_t;

        typedef std::pair<buf_handle_t, int> sort_job_t;
        typedef std::pair<int, size_t> merge_job_t;

        typedef boost::thread thread_t;
        typedef boost::shared_ptr<thread_t> thread_handle_t;

        typedef warp::SyncQueue<buf_handle_t> buf_queue_t;

        // Inter-thread queues
        warp::SyncQueue<buf_handle_t> freeQueue;
        warp::SyncQueue<sort_job_t>   sortQueue;
        warp::SyncQueue<buf_handle_t> mergeBufferQueue[2];
        warp::SyncQueue<merge_job_t>  mergeJobQueue;

        // Controlled by main thread
        int mergeIndex;
        size_t sortsPending[2];
        buf_handle_t putBuf;
        size_t putBufSize;
        AsyncSortParams params;
        
        // Controlled by merge thread
        base_handle_t output;
        bool isMerging;

        // Worker threads
        std::vector<thread_handle_t> sortThreads;
        thread_handle_t mergeThread;

        // Comparison functor (merge thread only)
        Lt lt;

        // Size functor (main thread only)
        Sz sz;

    public:
        explicit AsyncSort(AsyncSortParams const & params = AsyncSortParams(),
                           Lt const & lt = Lt(),
                           Sz const & sz = Sz()) :
            mergeJobQueue(1),
            mergeIndex(0),
            putBufSize(0),
            params(params),
            isMerging(false),
            lt(lt),
            sz(sz)
        {
            using boost::bind;
            using namespace ex;

            // Pre-populate buffer queue
            for(size_t i = 0; i < params.maxBuffers; ++i)
            {
                buf_handle_t b(new buf_t());
                freeQueue.push(b);
            }

            // Init pending counts
            sortsPending[0] = sortsPending[1] = 0;

            // Sort in reverse order to allow back-popping during merge
            warp::reverse_order<T,Lt> rlt(lt);

            // Make sort threads
            if(!params.nSortWorkers)
                raise<ValueError>("need positive number of sort worker threads");
            for(size_t i = 0; i < params.nSortWorkers; ++i)
            {
                thread_handle_t h(new thread_t(bind(&my_t::sortWorkerLoop, this, rlt)));
                sortThreads.push_back(h);
            }

            // If we allow async output, make a thread for merging
            if(params.asyncOutput)
                mergeThread.reset(new thread_t(bind(&my_t::mergeWorkerLoop, this)));
        }

        ~AsyncSort()
        {
            try {
                // Flush output
                flush();

                // Join sort workers
                sortQueue.cancelWaits();
                for(std::vector<thread_handle_t>::const_iterator ti = sortThreads.begin();
                    ti != sortThreads.end(); ++ti)
                {
                    (*ti)->join();
                }

                // Join merge thread
                mergeJobQueue.cancelWaits();
                if(mergeThread)
                    mergeThread->join();
            }
            catch(...) {
                // No exceptions!
            }
        }

        virtual void pipeTo(base_handle_t const & output)
        {
            this->output = output;
        }

        virtual void put(T const & x)
        {
            // Get an accumulation buffer if we don't already have one
            if(!putBuf)
            {
                if(!freeQueue.pop(putBuf, isMerging))
                    putBuf.reset(new buf_t());
                putBufSize = 0;
            }

            // Append to the current buffer and accumulate size
            putBuf->put(x);
            putBufSize += sz(x);

            // If the current buffer is sufficiently large, dispatch
            // it for sorting
            if(putBufSize >= params.dispatchThreshold)
            {
                ++sortsPending[mergeIndex];
                sortQueue.push(sort_job_t(putBuf, mergeIndex));
                putBuf.reset();
            }
        }

        virtual void flush()
        {
            // Dump current buffer
            if(putBuf)
            {
                ++sortsPending[mergeIndex];
                sortQueue.push(sort_job_t(putBuf, mergeIndex));
                putBuf.reset();
            }

            // Enqueue merge job
            merge_job_t job(mergeIndex, sortsPending[mergeIndex]);
            mergeJobQueue.push(job);
            sortsPending[mergeIndex] = 0;
            mergeIndex = 1 - mergeIndex;

            if(!params.asyncOutput)
            {
                // Do merge immediately
                doMerge();
            }
        }

    private:
        bool doMerge()
        {
            using namespace ex;

            // Get merge job spec: (queueIndex, nItems)
            merge_job_t job;
            if(!mergeJobQueue.pop(job))
                return false;

            // Shortcut empty merge
            if(!job.second)
                return true;

            // Set merging flag
            isMerging = true;

            // Set up merge
            std::vector<buf_handle_t> buffers;
            base_handle_t merge = makeMerge<T>(lt);
            for(size_t i = 0; i < job.second; ++i)
            {
                buf_handle_t buf;
                if(!mergeBufferQueue[job.first].pop(buf))
                    raise<RuntimeError>("missing buffer for merge");

                merge->pipeFrom(buf);
                buffers.push_back(buf);
            }

            // Copy merge stream to output (includes flush)
            if(output)
                copyStream(*merge, *output);

            // Release buffers
            for(typename std::vector<buf_handle_t>::const_iterator bi
                    = buffers.begin(); bi != buffers.end(); ++bi)
            {
                (*bi)->clear();
                freeQueue.push(*bi);
            }

            // Clear merging flag
            isMerging = false;

            return true;
        }

        void mergeWorkerLoop()
        {
            using namespace ex;
            using namespace std;

            try {
                while(doMerge())
                    ;
            }
            catch(Exception const & ex) {
                cerr << "merge worker: " << ex << endl;
                exit(1);
            }
            catch(exception const & ex) {
                cerr << "merge worker: Error: " << ex.what() << endl;
                exit(1);
            }
            catch(...) {
                cerr << "merge worker: unhandled exception" << endl;
                exit(1);
            }
        }

        void sortWorkerLoop(warp::reverse_order<T,Lt> rlt)
        {
            using namespace ex;
            using namespace std;

            try {
                for(;;)
                {
                    // Get sort job spec: (buffer, outQueueIndex)
                    sort_job_t job;
                    if(!sortQueue.pop(job))
                        return;

                    // Sort buffer
                    std::sort(job.first->begin(), job.first->end(), rlt);

                    // Put result in output queue
                    mergeBufferQueue[job.second].push(job.first);
                }
            }
            catch(Exception const & ex) {
                cerr << "sort worker: " << ex << endl;
                exit(1);
            }
            catch(exception const & ex) {
                cerr << "sort worker: Error: " << ex.what() << endl;
                exit(1);
            }
            catch(...) {
                cerr << "sort worker: unhandled exception" << endl;
                exit(1);
            }
        }
    };


    template <class T>
    typename AsyncSort<T>::handle_t makeAsyncSort()
    {
        typename AsyncSort<T>::handle_t h(new AsyncSort<T>());
        return h;
    }

    template <class T>
    typename AsyncSort<T>::handle_t makeAsyncSort(
        AsyncSortParams const & params)
    {
        typename AsyncSort<T>::handle_t h(new AsyncSort<T>(params));
        return h;
    }

    template <class T, class Lt>
    typename AsyncSort<T,Lt>::handle_t makeAsyncSort(
        AsyncSortParams const & params,
        Lt const & lt)
    {
        typename AsyncSort<T,Lt>::handle_t h(
            new AsyncSort<T,Lt>(params, lt));
        return h;
    }

    template <class T, class Lt, class Sz>
    typename AsyncSort<T,Lt,Sz>::handle_t makeAsyncSort(
        AsyncSortParams const & params,
        Lt const & lt, Sz const & sz)
    {
        typename AsyncSort<T,Lt,Sz>::handle_t h(
            new AsyncSort<T,Lt,Sz>(params, lt, sz));
        return h;
    }
}

#endif // FLUX_ASYNCSORT_H
