//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-27
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

#include <warp/string_range.h>
#include <warp/strutil.h>
#include <warp/string_interval.h>

#include <warp/parsing/quoted_str.h>
#include <warp/parsing/timestamp.h>

#include <kdi/scan_predicate.h>
#include <kdi/timestamp.h>
#include <boost/spirit.hpp>
#include <string>

using namespace kdi;
using namespace warp;
using namespace warp::parsing;
using namespace ex;
using namespace std;
using namespace boost::spirit;

namespace {
#if 0
}
#endif

//----------------------------------------------------------------------------
// Constants
//----------------------------------------------------------------------------
enum OperatorType
{
    OP_LT,
    OP_LTE,
    OP_GT,
    OP_GTE,
    OP_EQ,
    OP_STARTSWITH,
};


//----------------------------------------------------------------------------
// InequalityParser
//----------------------------------------------------------------------------
struct InequalityParser
{
    typedef OperatorType result_t;

    template <typename ScannerT>
    std::ptrdiff_t
    operator()(ScannerT const & scan, result_t & result) const
    {
        return
            (
                str_p("<=")  [ assign_a(result, OP_LTE) ] |
                ch_p('<')    [ assign_a(result, OP_LT)  ] |
                str_p(">=")  [ assign_a(result, OP_GTE) ] |
                ch_p('>')    [ assign_a(result, OP_GT)  ]
            )
            .parse(scan)
            .length();
    }
};

functor_parser<InequalityParser> const inequality_p;

//----------------------------------------------------------------------------
// StringIntervalParser
//----------------------------------------------------------------------------
class StringIntervalParser
{
public:
    typedef std::pair<std::string, warp::Interval<string> > result_t;
    
    template <typename ScannerT>
    std::ptrdiff_t
    operator()(ScannerT const & scan, result_t & r) const
    {
        // STRING_INTERVAL := 
        //    STRING_FIELD INEQUALITY STRING_LITERAL |
        //    STRING_LITERAL INEQUALITY STRING_FIELD INEQUALITY STRING_LITERAL |
        //    STRING_FIELD EQUALS STRING_LITERAL |
        //    STRING_LITERAL STARTSWITH STRING_LITERAL

        string s1,s2;
        OperatorType i1,i2;

        return
            (
                ( (str_p("row") | str_p("column"))[assign_a(r.first)] >>
                  inequality_p[assign_a(i1)] >>
                  quoted_str_p[assign_a(s1)]
                ) [ Assign1(r.second, i1, s1) ] |
                ( quoted_str_p[assign_a(s1)] >>
                  inequality_p[assign_a(i1)] >> 
                  (str_p("row") | str_p("column"))[assign_a(r.first)] >>
                  inequality_p[assign_a(i2)] >>
                  quoted_str_p[assign_a(s2)]
                ) [ Assign2(s1, i1, r.second, i2, s2) ] |
                ( (str_p("row") | str_p("column"))[assign_a(r.first)] >>
                  ch_p('=')[assign_a(i1, OP_EQ)] >>
                  quoted_str_p[assign_a(s1)]
                ) [ Assign1(r.second, i1, s1) ] |
                ( (str_p("row") | str_p("column"))[assign_a(r.first)] >>
                  str_p("~=")[assign_a(i1, OP_STARTSWITH)] >>
                  quoted_str_p[assign_a(s1)]
                ) [ Assign1(r.second, i1, s1) ]
            )
            .parse(scan)
            .length();
    }

private:
    struct Assign1
    {
        Interval<string> & ival;
        OperatorType const & op;
        string const & val;

        Assign1(Interval<string> & ival, OperatorType const & op, string const & val) :
            ival(ival), op(op), val(val)
        {            
        }
        
        void operator()(char const * begin, char const * end) const
        {
            switch(op)
            {
                case OP_GT:
                    ival.setLowerBound(val, BT_EXCLUSIVE).unsetUpperBound();
                    break;

                case OP_GTE:
                    ival.setLowerBound(val, BT_INCLUSIVE).unsetUpperBound();
                    break;

                case OP_LT:
                    ival.setUpperBound(val, BT_EXCLUSIVE).unsetLowerBound();
                    break;

                case OP_LTE:
                    ival.setUpperBound(val, BT_INCLUSIVE).unsetLowerBound();
                    break;

                case OP_EQ:
                    ival.setPoint(val);
                    break;

                case OP_STARTSWITH:
                    ival = makePrefixInterval(val);
                    break;

                default:
                    raise<ValueError>("unexpected operator: %s", StringRange(begin, end));
            }
        }
    };

    struct Assign2
    {
        Interval<string> & ival;
        OperatorType const & op1;
        OperatorType const & op2;
        string const & val1;
        string const & val2;

        Assign2(string const & val1, OperatorType const & op1,
                Interval<string> & ival,
                OperatorType const & op2, string const & val2) :
            ival(ival), op1(op1), op2(op2), val1(val1), val2(val2)
        {            
        }
        
        void operator()(char const * begin, char const * end) const
        {
            switch(op1)
            {
                case OP_GT:
                    if(op2 == OP_LT || op2 == OP_LTE)
                        raise<ValueError>("bad interval: %s", StringRange(begin, end));
                    ival.setUpperBound(val1, BT_EXCLUSIVE);
                    break;

                case OP_GTE:
                    if(op2 == OP_LT || op2 == OP_LTE)
                        raise<ValueError>("bad interval: %s", StringRange(begin, end));
                    ival.setUpperBound(val1, BT_INCLUSIVE);
                    break;

                case OP_LT:
                    if(op2 == OP_GT || op2 == OP_GTE)
                        raise<ValueError>("bad interval: %s", StringRange(begin, end));
                    ival.setLowerBound(val1, BT_EXCLUSIVE);
                    break;

                case OP_LTE:
                    if(op2 == OP_GT || op2 == OP_GTE)
                        raise<ValueError>("bad interval: %s", StringRange(begin, end));
                    ival.setLowerBound(val1, BT_INCLUSIVE);
                    break;

                default:
                    raise<ValueError>("unexpected operator: %s", StringRange(begin, end));
            }

            switch(op2)
            {
                case OP_GT:
                    ival.setLowerBound(val2, BT_EXCLUSIVE);
                    break;

                case OP_GTE:
                    ival.setLowerBound(val2, BT_INCLUSIVE);
                    break;

                case OP_LT:
                    ival.setUpperBound(val2, BT_EXCLUSIVE);
                    break;

                case OP_LTE:
                    ival.setUpperBound(val2, BT_INCLUSIVE);
                    break;

                default:
                    raise<ValueError>("unexpected operator: %s", StringRange(begin, end));
            }
        }
    };
};

functor_parser<StringIntervalParser> const string_interval_p;

//----------------------------------------------------------------------------
// TimeIntervalParser
//----------------------------------------------------------------------------
class TimeIntervalParser
{
public:
    typedef std::pair<std::string, warp::Interval<int64_t> > result_t;
    
    template <typename ScannerT>
    std::ptrdiff_t
    operator()(ScannerT const & scan, result_t & r) const
    {
        // TIME_INTERVAL := 
        //    TIME_FIELD INEQUALITY TIME_LITERAL |
        //    TIME_LITERAL INEQUALITY TIME_FIELD INEQUALITY TIME_LITERAL |
        //    TIME_FIELD EQUALS TIME_LITERAL |

        int64_t t1,t2;
        OperatorType i1,i2;

        return
            (
                ( str_p("time")[assign_a(r.first)] >>
                  inequality_p[assign_a(i1)] >>
                  timestamp_p[assign_a(t1)]
                ) [ Assign1(r.second, i1, t1) ] |
                ( timestamp_p[assign_a(t1)] >>
                  inequality_p[assign_a(i1)] >> 
                  str_p("time")[assign_a(r.first)] >>
                  inequality_p[assign_a(i2)] >>
                  timestamp_p[assign_a(t2)]
                ) [ Assign2(t1, i1, r.second, i2, t2) ] |
                ( str_p("time")[assign_a(r.first)] >>
                  ch_p('=')[assign_a(i1, OP_EQ)] >>
                  timestamp_p[assign_a(t1)]
                ) [ Assign1(r.second, i1, t1) ]
            )
            .parse(scan)
            .length();
    }

private:
    struct Assign1
    {
        Interval<int64_t> & ival;
        OperatorType const & op;
        int64_t const & val;

        Assign1(Interval<int64_t> & ival, OperatorType const & op, int64_t const & val) :
            ival(ival), op(op), val(val)
        {            
        }
        
        void operator()(char const * begin, char const * end) const
        {
            switch(op)
            {
                case OP_GT:
                    ival.setLowerBound(val, BT_EXCLUSIVE).unsetUpperBound();
                    break;

                case OP_GTE:
                    ival.setLowerBound(val, BT_INCLUSIVE).unsetUpperBound();
                    break;

                case OP_LT:
                    ival.setUpperBound(val, BT_EXCLUSIVE).unsetLowerBound();
                    break;

                case OP_LTE:
                    ival.setUpperBound(val, BT_INCLUSIVE).unsetLowerBound();
                    break;

                case OP_EQ:
                    ival.setPoint(val);
                    break;

                default:
                    raise<ValueError>("unexpected operator: %s", StringRange(begin, end));
            }
        }
    };

    struct Assign2
    {
        Interval<int64_t> & ival;
        OperatorType const & op1;
        OperatorType const & op2;
        int64_t const & val1;
        int64_t const & val2;

        Assign2(int64_t const & val1, OperatorType const & op1,
                Interval<int64_t> & ival,
                OperatorType const & op2, int64_t const & val2) :
            ival(ival), op1(op1), op2(op2), val1(val1), val2(val2)
        {            
        }
        
        void operator()(char const * begin, char const * end) const
        {
            switch(op1)
            {
                case OP_GT:
                    if(op2 == OP_LT || op2 == OP_LTE)
                        raise<ValueError>("bad interval: %s", StringRange(begin, end));
                    ival.setUpperBound(val1, BT_EXCLUSIVE);
                    break;

                case OP_GTE:
                    if(op2 == OP_LT || op2 == OP_LTE)
                        raise<ValueError>("bad interval: %s", StringRange(begin, end));
                    ival.setUpperBound(val1, BT_INCLUSIVE);
                    break;

                case OP_LT:
                    if(op2 == OP_GT || op2 == OP_GTE)
                        raise<ValueError>("bad interval: %s", StringRange(begin, end));
                    ival.setLowerBound(val1, BT_EXCLUSIVE);
                    break;

                case OP_LTE:
                    if(op2 == OP_GT || op2 == OP_GTE)
                        raise<ValueError>("bad interval: %s", StringRange(begin, end));
                    ival.setLowerBound(val1, BT_INCLUSIVE);
                    break;

                default:
                    raise<ValueError>("unexpected operator: %s", StringRange(begin, end));
            }

            switch(op2)
            {
                case OP_GT:
                    ival.setLowerBound(val2, BT_EXCLUSIVE);
                    break;

                case OP_GTE:
                    ival.setLowerBound(val2, BT_INCLUSIVE);
                    break;

                case OP_LT:
                    ival.setUpperBound(val2, BT_EXCLUSIVE);
                    break;

                case OP_LTE:
                    ival.setUpperBound(val2, BT_INCLUSIVE);
                    break;

                default:
                    raise<ValueError>("unexpected operator: %s", StringRange(begin, end));
            }
        }
    };
};

functor_parser<TimeIntervalParser> const time_interval_p;


//----------------------------------------------------------------------------
// IntervalSetParser
//----------------------------------------------------------------------------
template <class T, class P>
class IntervalSetParser
{
public:
    typedef std::pair<std::string, IntervalSet<T> > result_t;
    typedef std::pair<std::string, Interval<T> > interval_t;

    template <typename ScannerT>
    std::ptrdiff_t
    operator()(ScannerT const & scan, result_t & r) const
    {
        return
            (
                interval_p[Union(r)] % str_p("or")
            )
            .parse(scan)
            .length();
    }

private:
    P const interval_p;
    
    struct Union
    {
        result_t & result;
        
        Union(result_t & result) :
            result(result) {}

        void operator()(interval_t const & interval) const
        {
            if(result.first.empty())
                result.first = interval.first;
            else if(result.first != interval.first)
                raise<ValueError>("cannot form disjunction of different field predicates");
            
            result.second.add(interval.second);
        }
    };
};

functor_parser<IntervalSetParser<string, functor_parser<StringIntervalParser> > > const string_set_p;
functor_parser<IntervalSetParser<int64_t, functor_parser<TimeIntervalParser> > > const time_set_p;

//----------------------------------------------------------------------------
// ScanPredicateParser
//----------------------------------------------------------------------------
class ScanPredicateParser
{
public:
    typedef ScanPredicate result_t;

    template <typename ScannerT>
    std::ptrdiff_t
    operator()(ScannerT const & scan, result_t & r) const
    {
        int maxHistory;
        return
            (
                (
                    ( string_set_p[Update(r)] |
                      time_set_p[Update(r)] |
                      ( str_p("history") >>
                        ch_p('=') >>
                        int_p[assign_a(maxHistory)]
                      )[SetHistory(r, maxHistory)]
                    ) % str_p("and")
                ) | eps_p
            )
            .parse(scan)
            .length();
    }

public:
    struct Update
    {
        result_t & r;
        Update(result_t & r) : r(r) {}

        void operator()(pair<string, IntervalSet<string> > const & x) const
        {
            if(x.first == "row")
            {
                if(r.getRowPredicate())
                    raise<ValueError>("cannot specify row set conjunctions");
                r.setRowPredicate(x.second);
            }
            else if(x.first == "column")
            {
                if(r.getColumnPredicate())
                    raise<ValueError>("cannot specify column set conjunctions");
                r.setColumnPredicate(x.second);
            }
            else
            {
                raise<ValueError>("unexpected field: %s", x.first);
            }
        }

        void operator()(pair<string, IntervalSet<int64_t> > const & x) const
        {
            if(x.first == "time")
            {
                if(r.getTimePredicate())
                    raise<ValueError>("cannot specify time set conjunctions");
                r.setTimePredicate(x.second);
            }
            else
            {
                raise<ValueError>("unexpected field: %s", x.first);
            }
        }
    };

    struct SetHistory
    {
        result_t & r;
        int const & k;
        SetHistory(result_t & r, int const & k) : r(r), k(k) {}

        void operator()(char const *, char const *) const
        {
            if(r.getMaxHistory())
                raise<ValueError>("cannot specify history conjunctions");
            if(!k)
                raise<ValueError>("history should be non-zero");
            r.setMaxHistory(k);
        }
    };
};

functor_parser<ScanPredicateParser> const scan_predicate_p;

#if 0
{
#endif
} // anonymous namespace


//----------------------------------------------------------------------------
// ScanPredicate
//----------------------------------------------------------------------------
ScanPredicate::ScanPredicate(strref_t expr) :
    maxHistory(0)
{
    try {
        if(!parse(expr.begin(), expr.end(),
                  scan_predicate_p[assign_a(*this)] >> end_p,
                  space_p).full)
        {
            raise<ValueError>("malformed input");
        }
    }
    catch(std::exception const & ex) {
        raise<ValueError>("ScanPredicate parse failed (%s): %s",
                          ex.what(), reprString(expr));
    }
}

ScanPredicate ScanPredicate::clipRows(Interval<string> const & span) const
{
    if(span.isInfinite())
        return *this;

    ScanPredicate pred(*this);
    if(pred.getRowPredicate())
    {
        IntervalSet<string> rows(*pred.getRowPredicate());
        rows.clip(span);
        pred.setRowPredicate(rows);
    }
    else
    {
        pred.setRowPredicate(IntervalSet<string>().add(span));
    }

    return pred;
}

StringRange ScanPredicate::getColumnFamily(IntervalPoint<string> const & colPoint) const {
    if(colPoint.isInfinite())
        return StringRange();

    strref_t s = colPoint.getValue();
    char const *sep = find(s.begin(), s.end(), ':');

    if(colPoint.isLowerBound()) {
        if(sep < s.end()) {
            return StringRange(s.begin(), sep);
        } else {
            return StringRange();
        }
    } 

    if(colPoint.isUpperBound()) {
        if(sep == s.end()-1 && colPoint.isExclusive()) {
            return StringRange();
        } else if(sep < s.end()) {
            return StringRange(s.begin(), sep);
        } else if(colPoint.isExclusive()) {
            sep = find(s.begin(), s.end(), ';');
            if(sep == s.end()-1) { 
                return StringRange(s.begin(), sep);
            }
        } else {
            return StringRange();
        }
    }

    return colPoint.getValue();
}

bool ScanPredicate::getColumnFamilies(vector<warp::StringRange> &fams) const
{
    if(!columns) return false;

    IntervalSet<string>::const_iterator colIt = columns->begin();
    while(colIt != columns->end()) {
        strref_t lower = getColumnFamily(*colIt++);
        if(lower.empty()) {
            fams.clear();
            return false;
        }

        strref_t upper = getColumnFamily(*colIt++);
        if(upper != lower) {
            fams.clear();
            return false;
        }

        // Add to the column families if not already there
        if(find(fams.begin(), fams.end(), upper) == fams.end()) {
            std::cerr << "adding column family!! -- " << upper << std::endl;
            fams.push_back(upper);
        }
    }
    
    return true;
}

//----------------------------------------------------------------------------
// Output
//----------------------------------------------------------------------------
namespace {

    void outputValue(ostream & out, std::string const & val)
    {
        out << reprString(wrap(val));
    }

    void outputValue(ostream & out, int64_t val)
    {
        if(-10000 < val && val < 10000)
            out << '@' << val;
        else
            out << Timestamp::fromMicroseconds(val);
    }

    template <class T>
    bool outputInterval(ostream & out, string const & field,
                        IntervalPoint<T> const & lb,
                        IntervalPoint<T> const & ub)
    {
        if(ub.isInfinite())
        {
            if(lb.isInfinite())
                return false;
            
            out << field
                << (lb.isExclusive() ? " > " : " >= ");
            outputValue(out, lb.getValue());
            return true;
        }

        if(!lb.isInfinite())
        {
            if(lb.getValue() == ub.getValue() &&
               lb.isInclusive() && ub.isInclusive())
            {
                out << field << " = ";
                outputValue(out, lb.getValue());
                return true;
            }

            outputValue(out, lb.getValue());
            out << (lb.isExclusive() ? " < " : " <= ");
        }

        out << field
            << (ub.isExclusive() ? " < " : " <= ");
        outputValue(out, ub.getValue());

        return true;
    }

    template <class T>
    void outputIntervalSet(ostream & out, string const & field,
                           IntervalSet<T> const & iset)
    {
        if(iset.isEmpty())
        {
            // Empty set
            Interval<T> i;
            i.setEmpty();
            outputInterval(out, field, i.getLowerBound(), i.getUpperBound());
            return;
        }

        bool needOr = false;
        for(typename IntervalSet<T>::const_iterator lb = iset.begin();
            lb != iset.end(); ++lb, ++lb)
        {
            typename IntervalSet<T>::const_iterator ub = lb;
            ++ub;

            if(needOr)
                out << " or ";

            if(outputInterval(out, field, *lb, *ub))
                needOr = true;
        }
    }
}

ostream & kdi::operator<<(ostream & out, ScanPredicate const & pred)
{
    bool needAnd = false;

    if(pred.getRowPredicate() && !pred.getRowPredicate()->isInfinite())
    {
        outputIntervalSet(out, "row", *pred.getRowPredicate());
        needAnd = true;
    }

    if(pred.getColumnPredicate() && !pred.getColumnPredicate()->isInfinite())
    {
        if(needAnd)
            out << " and ";
        outputIntervalSet(out, "column", *pred.getColumnPredicate());
        needAnd = true;
    }

    if(pred.getTimePredicate() && !pred.getTimePredicate()->isInfinite())
    {
        if(needAnd)
            out << " and ";
        outputIntervalSet(out, "time", *pred.getTimePredicate());
        needAnd = true;
    }

    if(pred.getMaxHistory())
    {
        if(needAnd)
            out << " and ";
        out << "history = " << pred.getMaxHistory();
    }

    return out;
}
