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

#include <kdi/scan_predicate.h>
#include <warp/options.h>
#include <ex/exception.h>
#include <iostream>
#include <sstream>
#include <string>

using namespace kdi;
using namespace warp;
using namespace std;
using namespace ex;

int main(int ac, char ** av)
{
    OptionParser op("%prog <predicate statement>");
    
    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    string predStr;
    if(!args.empty())
    {
        ostringstream oss;
        ArgumentList::const_iterator i = args.begin();
        oss << *i;
        for(++i; i != args.end(); ++i)
            oss << ' ' << *i;
        predStr = oss.str();
    }

    cout << "Expression: " << predStr << endl;
    try {
        ScanPredicate pred(predStr);
        cout << "Predicate: " << pred << endl;
    }
    catch(ValueError const & ex) {
        cout << ex << endl;
        return 1;
    }

    return 0;
}
