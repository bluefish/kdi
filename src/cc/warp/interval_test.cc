//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-11
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/interval.h>
#include <iostream>
#include <sstream>
#include <string>

using namespace warp;
using namespace std;

template <class T>
istream & operator>>(istream & i, IntervalPoint<T> & p)
{
    T val = T();
    PointType type = PT_POINT;
    string s;
    i >> s;
    if(s == "<<")
        type = PT_INFINITE_LOWER_BOUND;
    else if(s == ">>")
        type = PT_INFINITE_UPPER_BOUND;
    else if(s == "[")
    {
        type = PT_INCLUSIVE_LOWER_BOUND;
        i >> val;
    }
    else if(s == "(")
    {
        type = PT_EXCLUSIVE_LOWER_BOUND;
        i >> val;
    }
    else
    {
        istringstream(s) >> val;
        i >> s;
        if(s == "]")
            type = PT_INCLUSIVE_UPPER_BOUND;
        else if(s == ")")
            type = PT_EXCLUSIVE_UPPER_BOUND;
    }

    p = IntervalPoint<T>(val, type);

    return i;
}

template <class T>
istream & operator>>(istream & i, Interval<T> & p)
{
    IntervalPoint<T> lb, ub;

    i >> lb >> ub;
    p = Interval<T>(lb, ub);

    return i;
}


int main(int ac, char ** av)
{
    IntervalSet<int> iset;
    
    while(!cin.eof())
    {
        string buf;
        cout << "What? " << flush;
        getline(cin, buf);
        istringstream line(buf);

        string cmd;
        line >> cmd;

        if(cmd == "add")
        {
            try {
                Interval<int> ival;
                line >> ival;
                cout << iset << "  UNION  " << ival;
                iset.add(ival);
                cout << "  =  " << iset << endl;
            }
            catch(exception const & ex) {
                cout << ex.what() << endl;
            }
        }
        else if(cmd == "contains")
        {
            try {
                IntervalPoint<int> lb;
                line >> lb;
                if(lb.getType() == PT_POINT)
                {
                    cout << lb << "  IN  " << iset << "  =  "
                         << iset.contains(lb.getValue()) << endl;
                }
                else
                {
                    IntervalPoint<int> ub;
                    line >> ub;
                    Interval<int> ival(lb,ub);
                    cout << ival << "  IN  " << iset << "  =  "
                         << iset.contains(ival) << endl;
                }
            }
            catch(exception const & ex) {
                cout << ex.what() << endl;
            }
        }
        else if(cmd == "overlaps")
        {
            try {
                Interval<int> ival;
                line >> ival;
                cout << ival << "  OVERLAPS  " << iset << "  =  "
                     << iset.overlaps(ival) << endl;
            }
            catch(exception const & ex) {
                cout << ex.what() << endl;
            }
        }
        else
        {
            cout << "Whatever." << endl;
        }
    }
}
