//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/assocvec.h#1 $
//
// Created 2006/04/21
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_ASSOCVEC_H
#define WARP_ASSOCVEC_H

#include "util.h"
#include <boost/iterator/indirect_iterator.hpp>
#include <memory>
#include <vector>
#include <functional>
#include <utility>
#include <algorithm>

namespace warp
{
    template <class K, class V>
    struct AVDirectStorage
    {
        typedef K key_type;
        typedef V mapped_type;
        typedef std::pair<K, V> value_type;
        typedef std::vector<value_type> vec_t;
        typedef typename vec_t::iterator iterator;
        typedef typename vec_t::const_iterator const_iterator;

        iterator insert(vec_t & v, iterator pos, value_type const & val) const
        {
            return v.insert(pos, val);
        }
            
        void erase(vec_t & v, iterator pos) const
        {
            v.erase(pos);
        }

        void cleanup(vec_t &) const
        {
        }
    };

    template <class K, class V>
    struct AVIndirectStorage
    {
        typedef K key_type;
        typedef V mapped_type;
        typedef std::pair<K, V> value_type;
        typedef std::vector<value_type *> vec_t;

        typedef boost::indirect_iterator<typename vec_t::iterator> iterator;
        typedef boost::indirect_iterator<typename vec_t::const_iterator> const_iterator;

        iterator insert(vec_t & v, iterator pos, value_type const & val) const
        {
            return v.insert(pos.base(), new value_type(val));
        }

        void erase(vec_t & v, iterator pos) const
        {
            delete *(pos.base());
            v.erase(pos.base());
        }
            
        void cleanup(vec_t & v) const
        {
            std::for_each(v.begin(), v.end(), delete_ptr());
        }
    };

    template <class K, class V, class Lt=std::less<K>,
              class Stor=AVIndirectStorage<K,V> >
    class AssocVector;
}


//----------------------------------------------------------------------------
// assoc_vec
//----------------------------------------------------------------------------
template <class K, class V, class Lt, class Stor>
class warp::AssocVector
{
public:
    typedef typename Stor::key_type key_type;
    typedef typename Stor::mapped_type mapped_type;
    typedef typename Stor::value_type value_type;
    typedef Lt key_compare;

    typedef AssocVector<K,V,Lt,Stor> my_t;
    typedef typename Stor::vec_t vec_t;

    typedef typename Stor::iterator iterator;
    typedef typename Stor::const_iterator const_iterator;


private:
    vec_t v;
    key_compare lt;
    Stor stor;

public:
    explicit AssocVector(Lt const & lt = Lt()) :
        lt(lt)
    {
    }
    AssocVector(my_t const & o) :
        lt(o.lt)
    {
        v.reserve(o.v.size());
        for(typename vec_t::const_iterator i = o.v.begin();
            i != o.v.end(); ++i)
        {
            stor.insert(v, v.end(), **i);
        }
    }
    template <class It>
    AssocVector(It first, It last, Lt const & lt = Lt()) :
        lt(lt)
    {
        insert(first, last);
    }
    ~AssocVector()
    {
        stor.cleanup(v);
    }

    void swap(my_t & o)
    {
        v.swap(o.v);
        swap(lt, o.lt);
    }

    key_compare const & key_comp() const { return lt; }

    void clear()
    {
        stor.cleanup(v);
        v.clear();
    }

    my_t const & operator=(my_t const & o)
    {
        clear();
        v.reserve(o.v.size());
        for(typename vec_t::const_iterator i = o.v.begin();
            i != o.v.end(); ++i)
        {
            stor.insert(v, v.end(), **i);
        }
        return *this;
    }

    size_t size() const { return v.size(); }

    iterator begin() { return v.begin(); }
    iterator end() { return v.end(); }

    template <class Kx>
    iterator lower_bound(Kx const & k)
    {
        iterator lo = v.begin();
        iterator hi = v.end();
        while(lo < hi)
        {
            iterator mid = lo + (hi-lo)/2;
            if(lt(mid->first, k))
                lo = mid+1;
            else
                hi = mid;
        }
        return hi;
    }

    template <class Kx>
    iterator upper_bound(Kx const & k)
    {
        iterator lo = v.begin();
        iterator hi = v.end();
        while(lo < hi)
        {
            iterator mid = lo + (hi-lo)/2;
            if(lt(k, mid->first))
                hi = mid;
            else
                lo = mid+1;
        }
        return hi;
    }

    template <class Kx>
    iterator find(Kx const & k)
    {
        iterator it = lower_bound(k);
        if(it == end() || lt(k, it->first))
            return end();
        else
            return it;
    }

    std::pair<iterator, bool> insert(value_type const & i)
    {
        std::pair<iterator,bool> r(lower_bound(i.first), false);
        if(r.first == end() || lt(i.first, r.first->first))
        {
            r.first = stor.insert(v, r.first, i);
            r.second = true;
        }
        else
            r.first->second = i.second;
        return r;
    }

    iterator insert(iterator pos, value_type const & i)
    {
        // Check to see if the hint pos is a good place to insert the
        // value.
        if(pos == end() || lt(i.first, pos->first))
        {
            // We should indeed insert before the hint position
            if(pos == begin() || lt((pos-1)->first, i.first))
            {
                // And we should not insert before the next lower
                // position.  This is a good hint.  Insert here.
                return stor.insert(v, pos, i);
            }
            else if(!lt(i.first, (pos-1)->first))
            {
                // The next lower position is less-than or equal to
                // the value (but not less-than because of the
                // previous test).  The key exists at the next lower
                // position.  Update that.
                --pos;
                pos->second = i.second;
                return pos;
            }
        }
        else if(!lt(pos->first, i.first))
        {
            // The key exists as the hint position.  Update here.
            pos->second = i.second;
            return pos;
        }

        // Hint is bad, search for a new position
        pos = lower_bound(i.first);
        if(pos == end() || lt(i.first, pos->first))
        {
            // Key does not exist -- insert
            pos = stor.insert(v, pos, i);
        }
        else
        {
            // Key exists -- update
            pos->second = i.second;
        }
        return pos;
    }

    template <class It>
    void insert(It first, It last)
    {
        iterator pos = end();
        while(first != last)
        {
            pos = insert(pos, *first);
            ++first;
        }
    }

    mapped_type & operator[](key_type const & k)
    {
        iterator it = lower_bound(k);
        if(it == end() || lt(k, it->first))
            it = stor.insert(v, it, value_type(k,mapped_type()));
        return it->second;
    }

    void erase(iterator pos)
    {
        stor.erase(v, pos);
    }

    void reserve(size_t sz)
    {
        v.reserve(sz);
    }

    // Const methods
    const_iterator begin() const { return v.begin(); }
    const_iterator end() const { return v.end(); }

    template <class Kx>
    const_iterator lower_bound(Kx const & k) const {
        return const_cast<my_t *>(this)->lower_bound(k);
    }

    template <class Kx>
    const_iterator upper_bound(Kx const & k) const {
        return const_cast<my_t *>(this)->upper_bound(k);
    }

    template <class Kx>
    const_iterator find(Kx const & k) const {
        return const_cast<my_t *>(this)->find(k);
    }
};


#endif // WARP_ASSOCVEC_H
