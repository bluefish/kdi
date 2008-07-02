//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/select.h#1 $
//
// Created 2006/04/07
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_SELECT_H
#define FLUX_SELECT_H

#include "stream.h"
#include "warp/util.h"
#include <boost/shared_ptr.hpp>

namespace flux
{
    //------------------------------------------------------------------------
    // NoOpPositioner
    //------------------------------------------------------------------------
    /// Positioner functor for Select that does nothing.
    struct NoOpPositioner
    {
        template <class K>
        bool operator()(K const & key)
        {
            return false;
        }
    };
    
    

    //------------------------------------------------------------------------
    // Select
    //------------------------------------------------------------------------
    /// Select items from an input stream matching keys from another
    /// stream.  Items in the input stream are assumed to be in
    /// non-decreasing order with respect to the key ordering.
    /// @param T Item type
    /// @param K Key type
    /// @param Lt Less-than ordering between items and keys
    /// @param Pos Functor to position input stream for reading a
    /// given key.  Should accept key type as argument and return a
    /// boolean indicating that the stream position has changed.
    template <class T, class K, class Lt=warp::less, class Pos=NoOpPositioner>
    class Select : public Stream<T>
    {
    public:
        typedef Select<T,K,Lt,Pos> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;
        typedef typename Stream<K>::handle_t key_handle_t;

    private:
        key_handle_t keyInput;
        base_handle_t input;
        K currentKey;
        bool inSequence;
        Lt lt;
        Pos positionInputForKey;

    public:
        explicit Select(Lt const & lt = Lt(),
                        Pos const & positionInputForKey = Pos(),
                        key_handle_t const & keyInput = key_handle_t()) :
            keyInput(keyInput),
            lt(lt),
            positionInputForKey(positionInputForKey)
        {
        }

        void pipeFrom(base_handle_t const & input)
        {
            this->input = input;
            currentKey = K();
            inSequence = false;
        }

        void setKeyInput(key_handle_t const & keyInput)
        {
            this->keyInput = keyInput;
            currentKey = K();
            inSequence = false;
        }

        bool get(T & x)
        {
            // Input stream is assumed to be in non-decreasing key order.
            bool haveKey = false;

            if(inSequence)
            {
                // We have a current key and have already read at least
                // one matching item.
                if(input->get(x))
                {
                    haveKey = true;
                    if(lt(currentKey, x))
                    {
                        // Sequence has ended
                        inSequence = false;
                    }
                    else
                    {
                        // Still in sequence
                        assert(!lt(x, currentKey));
                        return true;
                    }
                }
                else
                {
                    // End of stream: sequence has ended
                    inSequence = false;
                }
            }

            // We're not in a sequence.  We need to find the next key that
            // has at least one matching item and call that the next
            // sequence.
            for(;;)
            {
                // Get next key
                if(!keyInput->get(currentKey))
                {
                    // No more keys - we're done.
                    return false;
                }

                // Prepare start position to read items for next key
                if(positionInputForKey(currentKey))
                {
                    // Position changed
                    haveKey = false;
                }

                // Look for a matching record
                while(haveKey || (haveKey = input->get(x)))
                {
                    if(lt(x, currentKey))
                    {
                        // Not there yet
                        haveKey = false;
                        continue;
                    }
                    else if(lt(currentKey, x))
                    {
                        // Gone too far.  The input stream has no items
                        // for this key.  Continue with next key.
                        break;
                    }
                    else
                    {
                        // Found a match.  This is the start of the
                        // sequence.
                        inSequence = true;
                        return true;
                    }
                }
            }
        }
    };

    template <class T, class K>
    typename Select<T,K>::handle_t makeSelect()
    {
        typename Select<T,K>::handle_t h(new Select<T,K>());
        return h;
    }

    template <class T, class K, class Lt>
    typename Select<T,K,Lt>::handle_t makeSelect(Lt const & lt)
    {
        typename Select<T,K,Lt>::handle_t h(new Select<T,K,Lt>(lt));
        return h;
    }

    template <class T, class K, class Lt, class Pos>
    typename Select<T,K,Lt,Pos>::handle_t makeSelect(
        Lt const & lt, Pos const & pos)
    {
        typename Select<T,K,Lt,Pos>::handle_t h(
            new Select<T,K,Lt,Pos>(lt,pos));
        return h;
    }

    template <class T, class K, class Lt, class Pos>
    typename Select<T,K,Lt,Pos>::handle_t makeSelect(
        Lt const & lt, Pos const & pos,
        typename Stream<K>::handle_t const & keyStream)
    {
        typename Select<T,K,Lt,Pos>::handle_t h(
            new Select<T,K,Lt,Pos>(lt,pos,keyStream));
        return h;
    }
}

#endif // FLUX_SELECT_H
