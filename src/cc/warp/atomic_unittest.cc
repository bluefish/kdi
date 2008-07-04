//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
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

#include <warp/atomic.h>
#include <unittest/main.h>

#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <iostream>

typedef boost::thread thread_t;

  
void threadOneFunc(warp::AtomicCounter* ac) {  
  for(int i= 0; i < 10000000; i++)
    ac->increment();
}
  
void threadTwoFunc(warp::AtomicCounter* ac) {  
  for(int i= 0; i < 10000000; i++)
    ac->decrementAndTest();
}
  

BOOST_AUTO_TEST_CASE(test_Atomic) 
{

  warp::AtomicCounter *ac = new warp::AtomicCounter(10);

  boost::thread_group threads;
  threads.create_thread(boost::bind(&threadOneFunc, ac));
  threads.create_thread(boost::bind(&threadTwoFunc, ac));
  threads.join_all();

  BOOST_CHECK_EQUAL(ac->get(), 10);

}

class AT {

public:
  
  volatile int i;
  
};

//THIS IS A NEGATIVE TEST TO SHOW HOW NOT USING ATOMICITY HURTS

/*void threadOneFunc1(AT* a) {  
  for(int i= 0; i < 100000000; i++) {
    a->i--;
    a->i++;
//     a->i--;
//     a->i++;
//     a->i--;
//     a->i++;
//     a->i--;
//     a->i++;
//     a->i--;
//     a->i++;
  }
}
 
void threadTwoFunc2(AT *a) {  
  for(int i= 0; i < 100000000; i++) {
    a->i++;
    a->i--;
//     a->i++;
//     a->i--;
//     a->i++;
//     a->i--;
//     a->i++;
//     a->i--;
//     a->i++;
//     a->i--;
  }
}


BOOST_AUTO_TEST_CASE(test_Atomic1) 
{

  AT * a = new AT();
  a->i = 10;

  boost::thread_group threads;
  threads.create_thread(boost::bind(&threadOneFunc1, a));
  threads.create_thread(boost::bind(&threadTwoFunc2, a));
  threads.join_all();

  BOOST_CHECK_EQUAL(a->i, 10);

}
*/
