/** 
 * Convenient dereference routines for using boost's
 * shared_ptr objects with stl containers and
 * comparators ...
 *
 * @author Wang Lam
 * Separated from dqp/queryprocessor.cc Aug 2007
 */

#ifndef DEREFERENCE_H
#define DEREFERENCE_H

#include <boost/shared_ptr.hpp>
#include <boost/concept_check.hpp>
#include <boost/concept_archetype.hpp>
#include <algorithm>
#include <functional>

namespace warp {

template<class Comparator, class T>
struct DereferencedComparison :
        public std::binary_function<const boost::shared_ptr<T>, const boost::shared_ptr<T>, bool>
{
    DereferencedComparison(const Comparator &obj = Comparator()) : f(obj) {};
    Comparator f;
    bool operator()(const boost::shared_ptr<T> &a, const boost::shared_ptr<T> &b) const {

        boost::function_requires<
        boost::BinaryFunctionConcept<Comparator, bool, T, T> >();

        return f(*a,*b);
    }
};

template<class C>
DereferencedComparison<C, typename C::first_argument_type>
dereferencedComparison(const C &obj) {
    boost::function_requires< boost::BinaryFunctionConcept<C, bool,
        typename C::first_argument_type, typename C::first_argument_type> >();

    boost::function_requires< boost::BinaryFunctionConcept<C, bool,
        typename C::first_argument_type, typename C::first_argument_type> >();

    return DereferencedComparison<C, typename C::first_argument_type>(obj);
}


template<class T>
struct Dereference : public std::unary_function<const boost::shared_ptr<T>, T> {
    T &operator()(const boost::shared_ptr<T> &a) const {
        return *a;
    }
};

};

#endif // DEREFERENCE_H
