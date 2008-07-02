//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/scan_predicate.cc $
//
// Created 2008/02/27
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
