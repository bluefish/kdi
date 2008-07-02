//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/atomic_unittest.cc#1 $
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "atomic.h"
#include "unittest/main.h"

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
