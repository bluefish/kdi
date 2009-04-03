//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-06
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

#ifndef WARP_INTERVAL_H
#define WARP_INTERVAL_H

#include <set>
#include <functional>
#include <ostream>
#include <stdint.h>
#include <ex/exception.h>

namespace warp {

    /// Type identifier for IntervalPoint
    enum PointType
    {
        PT_INFINITE_LOWER_BOUND,     // [-INF
        PT_EXCLUSIVE_UPPER_BOUND,    //   x)
        PT_INCLUSIVE_LOWER_BOUND,    //  [x
        PT_POINT,                    //   x
        PT_INCLUSIVE_UPPER_BOUND,    //   x]
        PT_EXCLUSIVE_LOWER_BOUND,    //  (x 
        PT_INFINITE_UPPER_BOUND,     //  INF]
    };

    /// Type identifer for interval bounds
    enum BoundType
    {
        BT_INFINITE,
        BT_INCLUSIVE,
        BT_EXCLUSIVE,
    };

    /// A point in an interval.  It may be an endpoint (an upper or
    /// lower bound), or it can be a normal point contained in an
    /// interval.
    template <class T>
    class IntervalPoint;

    /// Ordering between IntervalPoints, parameterized by a value
    /// ordering function.
    template <class Lt>
    class IntervalPointOrder;

    /// A continuous interval.
    template <class T>
    class Interval;

    /// A set of intervals.
    template <class T, class Lt=std::less<T>, class A=std::allocator<T> >
    class IntervalSet;

    /// Return true if the given point type is a lower bound type.
    inline bool isLowerBound(PointType type)
    {
        return ( type == PT_INFINITE_LOWER_BOUND  ||
                 type == PT_EXCLUSIVE_LOWER_BOUND ||
                 type == PT_INCLUSIVE_LOWER_BOUND );
    }

    /// Return true if the given point type is an upper bound type.
    inline bool isUpperBound(PointType type)
    {
        return ( type == PT_INFINITE_UPPER_BOUND  ||
                 type == PT_EXCLUSIVE_UPPER_BOUND ||
                 type == PT_INCLUSIVE_UPPER_BOUND );
    }

    /// Return true if the given point type is an infinite type.
    inline bool isInfinite(PointType type)
    {
        return ( type == PT_INFINITE_LOWER_BOUND ||
                 type == PT_INFINITE_UPPER_BOUND );
    }

    /// Return true if the given point type is a finite type.
    inline bool isFinite(PointType type)
    {
        return !isInfinite(type);
    }

    /// Return true if the given point type is an inclusive bound
    /// type.
    inline bool isInclusive(PointType type)
    {
        return ( type == PT_INCLUSIVE_LOWER_BOUND ||
                 type == PT_INCLUSIVE_UPPER_BOUND );
    }

    /// Return true if the given point type is an exclusive bound
    /// type.
    inline bool isExclusive(PointType type)
    {
        return ( type == PT_EXCLUSIVE_LOWER_BOUND ||
                 type == PT_EXCLUSIVE_UPPER_BOUND );
    }

} // namespace warp


//----------------------------------------------------------------------------
// IntervalPoint
//----------------------------------------------------------------------------
template <class T>
class warp::IntervalPoint
{
    typedef IntervalPoint<T> my_t;

public:
    typedef T value_t;

private:
    value_t value;
    PointType type;

public:
    IntervalPoint() : value(T()), type(PT_POINT) {}

    explicit IntervalPoint(value_t const & v, PointType type=PT_POINT) :
        value(v), type(type) {}

    value_t const & getValue() const { return value; }
    PointType getType() const { return type; }

    /// Get the adjacent complement of this interval boundary point.
    /// The adjacent complement is only defined for finite boundary
    /// types.  Adjacent complements are defined as follows:
    ///    x]  <-->  (x
    ///    x)  <-->  [x
    my_t getAdjacentComplement() const
    {
        using namespace ex;
        switch(type)
        {
            case PT_EXCLUSIVE_UPPER_BOUND:
                return my_t(value, PT_INCLUSIVE_LOWER_BOUND);
            case PT_EXCLUSIVE_LOWER_BOUND:
                return my_t(value, PT_INCLUSIVE_UPPER_BOUND);
            case PT_INCLUSIVE_UPPER_BOUND:
                return my_t(value, PT_EXCLUSIVE_LOWER_BOUND);
            case PT_INCLUSIVE_LOWER_BOUND:
                return my_t(value, PT_EXCLUSIVE_UPPER_BOUND);
            default:
                raise<ValueError>("undefined adjacent complement");
        }
    }
    
    // Type classification
    bool isLowerBound() const { return warp::isLowerBound(type); }
    bool isUpperBound() const { return warp::isUpperBound(type); }
    bool isInfinite()   const { return warp::isInfinite(type); }
    bool isFinite()     const { return warp::isFinite(type); }
    bool isInclusive()  const { return warp::isInclusive(type); }
    bool isExclusive()  const { return warp::isExclusive(type); }

    // Convenience operators using default IntervalPointOrder
    inline bool operator<(my_t const & o)  const; // After IntervalPointOrder
    inline bool operator>(my_t const & o)  const { return o < *this; }
    inline bool operator<=(my_t const & o) const { return !(o < *this); }
    inline bool operator>=(my_t const & o) const { return !(*this < o); }
    inline bool operator==(my_t const & o) const
    {
        return !((*this < o) || (o < *this));
    }
    inline bool operator!=(my_t const & o) const { return !(*this == o); }
};

namespace warp
{
    template <class T>
    std::ostream & operator<<(std::ostream & o, IntervalPoint<T> const & pt)
    {
        switch(pt.getType())
        {
            case PT_INFINITE_LOWER_BOUND:  return o << "<<";
            case PT_EXCLUSIVE_UPPER_BOUND: return o << pt.getValue() << ')';
            case PT_INCLUSIVE_LOWER_BOUND: return o << '[' << pt.getValue();
            case PT_POINT:                 return o << pt.getValue();
            case PT_INCLUSIVE_UPPER_BOUND: return o << pt.getValue() << ']';
            case PT_EXCLUSIVE_LOWER_BOUND: return o << '(' << pt.getValue();
            case PT_INFINITE_UPPER_BOUND:  return o << ">>";
            default:                       return o << "<?>";
        }
    }
}


//----------------------------------------------------------------------------
// IntervalPointOrder
//----------------------------------------------------------------------------
template <class Lt>
class warp::IntervalPointOrder
{
    Lt valueLt;

public:
    explicit IntervalPointOrder(Lt const & lt=Lt()) : valueLt(lt) {}

    /// Less-than ordering
    template <class A, class B>
    bool isLess(A const & aValue, PointType aType,
                B const & bValue, PointType bType) const
    {
        // First do special handling for infinite points
        if(warp::isInfinite(aType))
        {
            // A is infinite.  Special handling:
            if(warp::isLowerBound(aType))
            {
                // A is an infinite lower bound.  It is less than
                // anything except another infinite lower bound.
                return warp::isFinite(bType) || warp::isUpperBound(bType);
            }
            else
            {
                // Is is an infinite upper bound.  It is not less
                // than anything.
                return false;
            }
        }
        else if(warp::isInfinite(bType))
        {
            // B is infinite, but A is finite.  If B is an upper
            // bound, than any finite A must be less than it.  If it
            // is a lower bound, then no finite A can be less than it.
            return warp::isUpperBound(bType);
        }

        // At this point both A and B are finite bounds with
        // decision point values

        // Order first by value
        else if(valueLt(aValue, bValue))
            return true;
        else if(valueLt(bValue, aValue))
            return false;

        // Values are equal, now order by type
        return aType < bType;
    }

    /// Less-than ordering between two IntervalPoints
    template <class A, class B>
    bool operator()(IntervalPoint<A> const & a, IntervalPoint<B> const & b) const
    {
        return isLess(a.getValue(), a.getType(),
                      b.getValue(), b.getType());
    }

    /// Less-than ordering between a point value and an IntervalPoint
    template <class A, class B>
    bool operator()(A const & aValue, IntervalPoint<B> const & b) const
    {
        return isLess(aValue, PT_POINT,
                      b.getValue(), b.getType());
    }

    /// Less-than ordering between an IntervalPoint and a point value
    template <class A, class B>
    bool operator()(IntervalPoint<A> const & a, B const & bValue) const
    {
        return isLess(a.getValue(), a.getType(),
                      bValue, PT_POINT);
    }
};

namespace warp {

    template <class T>
    bool IntervalPoint<T>::operator<(IntervalPoint<T> const & o) const
    {
        // Use default ordering
        return IntervalPointOrder< std::less<T> >()(*this, o);
    }

}


//----------------------------------------------------------------------------
// Interval
//----------------------------------------------------------------------------
template <class T>
class warp::Interval
{
public:
    typedef Interval<T> my_t;
    typedef T value_t;
    typedef IntervalPoint<value_t> point_t;

private:
    point_t lowerBound;
    point_t upperBound;

public:
    /// Default constructor produces an empty interval that contains
    /// nothing.
    Interval() :
        lowerBound(point_t(T(), PT_EXCLUSIVE_LOWER_BOUND)),
        upperBound(point_t(T(), PT_EXCLUSIVE_UPPER_BOUND))
    {
    }

    /// Construct an interval from two points.
    Interval(point_t const & lowerBound, point_t const & upperBound) :
        lowerBound(lowerBound), upperBound(upperBound)
    {
        using namespace ex;
        if(!lowerBound.isLowerBound())
            raise<ValueError>("invalid lower bound");
        if(!upperBound.isUpperBound())
            raise<ValueError>("invalid upper bound");
    }

    /// Set the lower bound value and type.  The default type is
    /// BT_INCLUSIVE for an inclusive lower bound.  If the type is
    /// BT_INFINITE, the value doesn't matter.
    my_t & setLowerBound(T const & bound, BoundType type=BT_INCLUSIVE)
    {
        lowerBound = point_t(
            bound,
            type == BT_INCLUSIVE ? PT_INCLUSIVE_LOWER_BOUND :
            type == BT_EXCLUSIVE ? PT_EXCLUSIVE_LOWER_BOUND :
            PT_INFINITE_LOWER_BOUND);
        return *this;
    }

    /// Set lower bound from a point.  The point must be a lower
    /// bound.
    my_t & setLowerBound(point_t const & p)
    {
        using namespace ex;
        if(!p.isLowerBound())
            raise<ValueError>("invalid lower bound");
        lowerBound = p;
        return *this;
    }

    /// Unset the lower bound.  This is equivalent to setting an
    /// infinite lower bound.
    my_t & unsetLowerBound()
    {
        lowerBound = point_t(T(), PT_INFINITE_LOWER_BOUND);
        return *this;
    }

    /// Set the upper bound value and type.  The default type is
    /// BT_EXCLUSIVE for an exclusive upper bound.  If the type is
    /// BT_INFINITE, the value doesn't matter.
    my_t & setUpperBound(T const & bound, BoundType type=BT_EXCLUSIVE)
    {
        upperBound = point_t(
            bound,
            type == BT_EXCLUSIVE ? PT_EXCLUSIVE_UPPER_BOUND :
            type == BT_INCLUSIVE ? PT_INCLUSIVE_UPPER_BOUND :
            PT_INFINITE_UPPER_BOUND);
        return *this;
    }

    /// Set upper bound from a point.  The point must be an upper
    /// bound.
    my_t & setUpperBound(point_t const & p)
    {
        using namespace ex;
        if(!p.isUpperBound())
            raise<ValueError>("invalid upper bound");
        upperBound = p;
        return *this;
    }

    /// Unset the upper bound.  This is equivalent to setting an
    /// infinite upper bound.
    my_t & unsetUpperBound()
    {
        upperBound = point_t(T(), PT_INFINITE_UPPER_BOUND);
        return *this;
    }

    /// Set the interval to contain a single point.  This is
    /// equivalent to calling setLowerBound and setUpperBound with the
    /// same value and an inclusive type.
    my_t & setPoint(T const & x)
    {
        return setLowerBound(x, BT_INCLUSIVE)
            .setUpperBound(x, BT_INCLUSIVE);
    }

    /// Set the interval to contain everything.  This makes both the
    /// upper and lower bound infinite.
    my_t & setInfinite()
    {
        return unsetLowerBound().unsetUpperBound();
    }

    /// Set the interval to contain nothing.  This makes both the
    /// upper and lower bound exclusive around the same point.
    my_t & setEmpty()
    {
        return setLowerBound(T(), BT_EXCLUSIVE)
            .setUpperBound(T(), BT_EXCLUSIVE);
    }

    /// Get the lower bound point.
    point_t const & getLowerBound() const { return lowerBound; }

    /// Get the upper bound point.
    point_t const & getUpperBound() const { return upperBound; }

    /// Return true if the interval contains everything.
    bool isInfinite() const
    {
        return lowerBound.isInfinite() && upperBound.isInfinite();
    }

    /// Return true if the interval contains nothing, as defined by
    /// the given less-than ordering on point values.
    template <class Lt>
    bool isEmpty(Lt const & lt) const
    {
        IntervalPointOrder<Lt> plt(lt);
        return plt(upperBound, lowerBound);
    }

    /// Return true if the interval contains the given value, as
    /// defined by the given less-than ordering on point values.
    template <class TT, class Lt>
    bool contains(TT const & x, Lt const & lt) const
    {
        IntervalPointOrder<Lt> plt(lt);
        return plt(lowerBound, x) && plt(x, upperBound);
    }

    /// Return true if the interval contains (or equals) the given
    /// interval, as defined by the given less-than ordering on point
    /// values.  Empty intervals contain nothing.  Non-empty intervals
    /// contain any empty interval.
    template <class TT, class Lt>
    bool contains(Interval<TT> const & o, Lt const & lt) const
    {
        IntervalPointOrder<Lt> plt(lt);

        // Empty intervals contain nothing
        if(plt(upperBound, lowerBound))
            return false;

        // Non-empty intervals contain any empty interval.
        if(plt(o.getUpperBound(), o.getLowerBound()))
            return true;

        // We contain the interval iff:
        //   my.lb <= o.lb && o.ub <= my.ub
        return ( !plt(o.getLowerBound(), lowerBound) &&
                 !plt(upperBound, o.getUpperBound()) );
    }

    /// Return true if the interval overlaps the given interval, as
    /// defined by the given less-than ordering on point values.
    /// Empty intervals overlap nothing.
    template <class TT, class Lt>
    bool overlaps(Interval<TT> const & o, Lt const & lt) const
    {
        IntervalPointOrder<Lt> plt(lt);
        
        // Empty intervals overlap nothing
        if(plt(upperBound, lowerBound) ||
           plt(o.getUpperBound(), o.getLowerBound()))
        {
            return false;
        }

        // Are the intervals separated?
        if(plt(upperBound, o.getLowerBound()) ||
           plt(o.getUpperBound(), lowerBound))
        {
            return false;
        }

        // Must overlap
        return true;
    }

    /// Clip this interval to the given interval using the supplied
    /// less-than ordering on point values.  If the intervals do not
    /// overlap, the result will be empty.
    template <class Lt>
    my_t & clip(my_t const & o, Lt const & lt)
    {
        IntervalPointOrder<Lt> plt;
        
        if(plt(lowerBound, o.lowerBound))
            lowerBound = o.lowerBound;
        if(plt(o.upperBound, upperBound))
            upperBound = o.upperBound;

        return *this;
    }
    
    

    // Point-ordering methods using default ordering

    /// Return true if the interval contains nothing, as defined by
    /// the default less-than ordering on point values.
    bool isEmpty() const
    {
        return isEmpty(std::less<T>());
    }

    /// Return true if the interval contains the given value, as
    /// defined by the default less-than ordering on point values.
    template <class TT>
    bool contains(TT const & x) const
    {
        return contains(x, std::less<T>());
    }

    /// Return true if the interval contains (or equals) the given
    /// interval, as defined by the default less-than ordering on
    /// point values.  Empty intervals contain nothing.  Non-empty
    /// intervals contain any empty interval.
    template <class TT>
    bool contains(Interval<TT> const & o) const
    {
        return contains(o, std::less<T>());
    }

    /// Return true if the interval overlaps the given interval, as
    /// defined by the default less-than ordering on point values.
    /// Empty intervals overlap nothing.
    template <class TT>
    bool overlaps(Interval<TT> const & o) const
    {
        return overlaps(o, std::less<T>());
    }

    /// Clip this interval to the given interval using the default
    /// less-than ordering on point values.  If the intervals do not
    /// overlap, the result will be empty.
    my_t & clip(my_t const & o)
    {
        return clip(o, std::less<T>());
    }
};

namespace warp
{
    template <class T>
    std::ostream & operator<<(std::ostream & o, Interval<T> const & i)
    {
        return o << i.getLowerBound() << ' ' << i.getUpperBound();
    }

    /// Make an interval.  The default interval type is an inclusive
    /// lower bound and an exclusive upper bound.
    template <class T>
    Interval<T> makeInterval(T const & lb, T const & ub,
                             bool lInclusive = true,
                             bool uInclusive = false)
    {
        return Interval<T>(
            IntervalPoint<T>(
                lb,
                lInclusive ? PT_INCLUSIVE_LOWER_BOUND :
                PT_EXCLUSIVE_LOWER_BOUND),
            IntervalPoint<T>(
                ub,
                uInclusive ? PT_INCLUSIVE_UPPER_BOUND :
                PT_EXCLUSIVE_UPPER_BOUND));
    }

    /// Return the open interval with the given lower bound.  The
    /// default interval type is an inclusive lower bound.
    template <class T>
    Interval<T> makeLowerBound(T const & lb, bool lInclusive=true)
    {
        return Interval<T>(
            IntervalPoint<T>(
                lb,
                lInclusive ? PT_INCLUSIVE_LOWER_BOUND :
                PT_EXCLUSIVE_LOWER_BOUND),
            IntervalPoint<T>(T(), PT_INFINITE_UPPER_BOUND));
    }

    /// Return the open interval with the given upper bound.  The
    /// default interval type is an exclusive upper bound.
    template <class T>
    Interval<T> makeUpperBound(T const & ub, bool uInclusive=false)
    {
        return Interval<T>(
            IntervalPoint<T>(T(), PT_INFINITE_LOWER_BOUND),
            IntervalPoint<T>(
                ub,
                uInclusive ? PT_INCLUSIVE_UPPER_BOUND :
                PT_EXCLUSIVE_UPPER_BOUND));
    }

    /// Return an unbounded interval.
    template <class T>
    Interval<T> makeUnboundedInterval()
    {
        return Interval<T>(
            IntervalPoint<T>(T(), PT_INFINITE_LOWER_BOUND),
            IntervalPoint<T>(T(), PT_INFINITE_UPPER_BOUND));
    }
}


//----------------------------------------------------------------------------
// IntervalSet
//----------------------------------------------------------------------------
template <class T, class Lt, class A>
class warp::IntervalSet
{
    // Invariants
    //
    //   1. The point set contains only lower bound and upper bound
    //      interval points.
    //   2. The point set contains no duplicates
    //   3. The point set is maintained in sorted order
    //      w.r.t. point_cmp_t.
    //   4. The point set contains 2K points, where K is the number of
    //      disjoint ranges in the interval set.
    //   5. An upper bound in the point set is always preceeded by a
    //      lower bound.  Likewise a lower bound is always followed by
    //      an upper bound.
    //   6. The range set representation is minimal*.
    //
    // * Minimal without special domain knowledge of the contained
    //   type.  For example, the set "[1 3] [3 5]" can be simplified
    //   to "[1 5]", but the set "[1 3] [4 5]" cannot be reduced
    //   unless we can infer there is no possible value
    //   between 3 and 4.
    
public:
    typedef IntervalSet<T,Lt,A> my_t;
    typedef T value_t;
    typedef IntervalPoint<value_t> point_t;
    typedef Interval<value_t> interval_t;

private:
    typedef Lt value_cmp_t;
    typedef A alloc_t;
    typedef IntervalPointOrder<value_cmp_t> point_cmp_t;
    typedef typename alloc_t::template rebind<point_t>::other point_alloc_t;
    typedef std::vector<point_t, point_alloc_t> vec_t;

    typedef typename vec_t::iterator iter_t;
    typedef typename vec_t::const_iterator citer_t;

public:
    typedef citer_t const_iterator;

private:
    vec_t points;
    value_cmp_t valueLt;
    point_cmp_t pointLt;

    citer_t upper_bound(point_t const & x) const
    {
        return std::upper_bound(
            points.begin(), points.end(),
            x, pointLt);
    }

    citer_t lower_bound(point_t const & x) const
    {
        return std::lower_bound(
            points.begin(), points.end(),
            x, pointLt);
    }

    iter_t upper_bound(point_t const & x) 
    {
        return std::upper_bound(
            points.begin(), points.end(),
            x, pointLt);
    }

    iter_t lower_bound(point_t const & x)
    {
        return std::lower_bound(
            points.begin(), points.end(),
            x, pointLt);
    }

public:
    IntervalSet() {}
    explicit IntervalSet(Lt const & lt, A const & alloc=A()) :
        points(point_cmp_t(lt), alloc),
        valueLt(lt),
        pointLt(lt)
    {}

    /// Get an iterator to the first endpoint in the interval set.
    /// Endpoints will always come in pairs: first a lower bound, then
    /// an upper bound.
    const_iterator begin() const { return points.begin(); }

    /// Get an iterator pointing to one past the last endpoint in the
    /// interval set.
    const_iterator end() const { return points.end(); }
    
    /// Return the number of endpoints in the interval set.  This will
    /// always be twice the number of disjoint ranges in the interval
    /// set.
    size_t size() const { return points.size(); }

    /// Return true if the interval set contains nothing.
    bool isEmpty() const { return points.empty(); }

    /// Return true if the interval set contains everything.
    bool isInfinite() const
    {
        const_iterator i = begin();
        return (  i != end() && i->isInfinite() &&
                ++i != end() && i->isInfinite() );
    }

    // True iff the set contains the value as defined by the given less-than
    // ordering on point values.
    template <class TT, class Lt2>  
    bool contains(TT const & x, Lt2 const & lt) const
    {
        IntervalPointOrder<Lt2> plt(lt);

        // Find the insertion point for 'x'.
        citer_t it = std::lower_bound(points.begin(), points.end(), x, plt);

        // We're in the set iff the insertion point is in a valid range.
        return isInRange(it);
    }

    /// True iff the set contains the value.
    bool contains(value_t const & x) const
    {
        return contains(point_t(x), valueLt);
    }

    /// True iff the set contains the entire interval.
    bool contains(interval_t const & x) const
    {
        // Return false for empty query intervals
        if(!pointLt(x.getLowerBound(), x.getUpperBound()))
            return false;

        // The interval is contained without interruption iff the
        // insertion point for the lower bound and the upper bound are
        // the same, and that point is in a valid range.
        citer_t lo = upper_bound(x.getLowerBound());
        citer_t hi = lower_bound(x.getUpperBound());

        return (lo == hi && isInRange(lo));
    }

    /// True iff the set contains any of the interval.
    bool overlaps(interval_t const & x) const
    {
        // Return false for empty query intervals
        if(!pointLt(x.getLowerBound(), x.getUpperBound()))
            return false;

        // The interval overlaps with some valid range if it is either
        // contained completely within a valid range or there is some
        // endpoint between the insertion points for the lower and
        // upper bounds.
        citer_t lo = upper_bound(x.getLowerBound());
        citer_t hi = lower_bound(x.getUpperBound());

        return (lo != hi || isInRange(lo));
    }

    /// Update the interval set by setting it to the union of itself
    /// and the given interval.
    my_t & add(interval_t const & x)
    {
        // Reserve additional space now so that iterators won't
        // be invalidated by reallocation.
        points.reserve(points.size()+2);

        // Ignore empty update intervals
        if(!pointLt(x.getLowerBound(), x.getUpperBound()))
            return *this;

        // Insert lower bound -- 
        iter_t lower = lower_bound(x.getLowerBound());

        // Only need to insert anything if not duplicate
        if(lower == points.end() || pointLt(x.getLowerBound(), *lower)) {
            lower = points.insert(lower, x.getLowerBound());

            // Move to the least constrained lower bound
            lower = pickLowerBound(lower);
        }

        // Insert upper bound -- 
        iter_t upper = upper_bound(x.getUpperBound());

        // Don't insert duplicates
        if(upper == points.end() || pointLt(*(upper-1), x.getUpperBound()))
        {
            upper = points.insert(upper, x.getUpperBound());

            // Move to the least constrained upper bound
            upper = pickUpperBound(upper);
        }
        
        // Delete extraneous elements between the two endpoints
        if(++lower != upper)
            points.erase(lower, upper);

        return *this;
    }

    /// Clip the interval set to the given span.  Everything outside
    /// the span will be removed.
    my_t & clip(interval_t const & x)
    {
        // If the interval is empty, clear everything
        if(!pointLt(x.getLowerBound(), x.getUpperBound()))
        {
            points.clear();
            return *this;
        }

        // Find the lower bound of our clip region.  If is in the
        // middle of a valid range, we'll have to clip that range by
        // inserting a new lower bound.
        iter_t lower = lower_bound(x.getLowerBound());
        if(isInRange(lower))
            lower = points.insert(lower, x.getLowerBound());

        // Erase everything before the beginning of our clip region.
        points.erase(points.begin(), lower);

        // Find the upper bound of our clip region.  It is in the
        // middle of a valid range, we'll have to clip that range by
        // inserting a new upper bound.
        iter_t upper = upper_bound(x.getUpperBound());
        if(isInRange(upper))
        {
            upper = points.insert(upper, x.getUpperBound());
            // Move iterator ahead so we don't delete our newly
            // inserted upper bound.
            ++upper;
        }

        // Erase everything after the end of our clip region.
        points.erase(upper, points.end());

        return *this;
    }

    /// Update to the intersection of this and another interval set.
    my_t & intersect(my_t const & o)
    {
        vec_t merged;
        
        citer_t i    = points.begin();
        citer_t iEnd = points.end();
        citer_t j    = o.points.begin();
        citer_t jEnd = o.points.end();

        bool inI = false;
        bool inJ = false;

        while(i != iEnd && j != jEnd)
        {
            if(pointLt(*j, *i))
            {
                if(inI)
                    merged.push_back(*j);
                
                inJ = !inJ;
                ++j;
            }
            else
            {
                if(inJ)
                    merged.push_back(*i);

                inI = !inI;
                ++i;
            }
        }

        points.swap(merged);
        return *this;
    }

    /// Get the tightest interval containing the entire interval set.
    interval_t getHull() const
    {
        if(isEmpty())
            return interval_t().setEmpty();
        else
        {
            return interval_t(
                *points.begin(),
                *--points.end());
        }
    }

private:
    /// Return true iff the iterator is within an included range.
    /// This is the case if the iterator is pointing to an upper bound
    /// and the item below it is a lower bound.
    bool isInRange(citer_t const & it) const
    {
        // The iterator is in a valid range if it points to an upper
        // bound.  Class invariants require that if this is the case,
        // there is a valid lower bound below it.
        return it != points.end() && it->isUpperBound();
    }

    iter_t pickLowerBound(iter_t const & it)
    {
        assert(it != points.end());
        assert(it->isLowerBound());

        // If there is no lower neighbor, then we want to use the
        // current point as a lower bound.
        if(it == points.begin())
            return it;

        iter_t other = it;
        --other;

        if(other->isLowerBound())
        {
            // Two lower bounds in a row -- the lower one is less
            // constrained, so use that
            return other;
        }
        else if(it->isExclusive() && other->isExclusive())
        {
            // We have "a) (b" where a <= b.  Keep our bound.
            return it;
        }
        else if(!valueLt(other->getValue(), it->getValue()))
        {
            // Other is an upper bound, but it has the same value.
            // Sort order guarantees that we have either:
            //    x) [x
            // or
            //    x] (x
            // Either way, this is a meaningless range division, and
            // we can choose the lower bound below other.
            return --other;
        }
        else
        {
            // The given lower bound is good
            return it;
        }
    }

    iter_t pickUpperBound(iter_t const & it)
    {
        assert(it != points.end());
        assert(it->isUpperBound());

        iter_t other = it;
        ++other;

        // If there is no upper neighbor, then we want to use the
        // current point as an upper bound.
        if(other == points.end())
            return it;

        if(other->isUpperBound())
        {
            // Two upper bounds in a row -- the upper one is less
            // constrained, so use that
            return other;
        }
        else if(it->isExclusive() && other->isExclusive())
        {
            // We have "a) (b" where a <= b.  Keep our bound.
            return it;
        }
        else if(!valueLt(it->getValue(), other->getValue()))
        {
            // Other is a lower bound, but it has the same value.
            // Sort order guarantees that we have either:
            //    x) [x
            // or
            //    x] (x
            // Either way, this is a meaningless range division, and
            // we can choose the upper bound above other.
            return ++other;
        }
        else
        {
            // The given upper bound is good
            return it;
        }
    }
};

namespace warp
{
    template <class T, class Lt, class A>
    std::ostream & operator<<(std::ostream & o, IntervalSet<T,Lt,A> const & s)
    {
        bool first = true;
        for(typename IntervalSet<T,Lt,A>::const_iterator i = s.begin();
            i != s.end(); ++i)
        {
            if(!first)
                o << ' ';
            else
                first = false;

            o << *i;
        }
        return o;
    }
}



#endif // WARP_INTERVAL_H
