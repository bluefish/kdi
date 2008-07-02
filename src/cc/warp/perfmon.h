//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/perfmon.h#1 $
//
// Created 2006/11/14
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#error perfmon.h is not ready for use


/*
  Design notes:


  Would like to track:

    - Time series data vs. real time
      - input bytes read
      - output bytes written
      - cpu time used
      - memory allocation size
      - processed item counts
  
    - Events on real time
      - code phase transitions (read, sort, write)
      - signals

  Must be thread safe:

    - Reporting will probably need to happen in another thread
    - Would like to track data from several threads

  Output to single performance logging stream.  Use simple text format
  and leave processing and fancy interpretation to other processes.

  Human readable, but program required for interpretation

  Output format: (timestamp) (type) (message) (newline)
  
    - Timestamp is real time in milliseconds since start of process
    - Type defines how message should be interpreted
    - Message is some extra info depending on type

  Sample:

    0 TIME 2006/11/30 11:20:35 PST
    0 EVENT Program begins: wdscanon --foo --bar --baz
    0 VAR 0 file input kfs://dev008/blah/meh.PsPg (byte)
    0 VAR 0 stream input kfs://dev008/blah/meh.PsPg
    0 VAR 0 file output kfs://dev008/tmp/url-3.cdf (byte)
    0 VAR 0 stream output kfs://dev008/tmp/url-3.cdf
    0 VAR 117 CPU time (ms)
    5000 VAR 1245717 file input kfs://dev008/blah/meh.PsPg (byte)
    5000 VAR 99 stream input kfs://dev008/blah/meh.PsPg
    5000 VAR 402911 file output kfs://dev008/tmp/url-3.cdf (byte)
    5000 VAR 431 stream output kfs://dev008/tmp/url-3.cdf
    5000 VAR 4511 CPU time (ms)
    7500 EVENT Sort begins
    16900 EVENT Sort ends

  Message types:

    TIME:
      Global time to sync with process local timestamp, in YYYY/MM/DD
      HH:MM:SS TZ format (24-hour)

    EVENT:
      Some arbitrary string to describe the event

    VAR:
      Message relating to a tracked performance variable

      Format: int-value var-label

      where 'int-value' is the tracked integer value and 'var-label'
      is a descriptive name for the variable.  The label is typically
      a series of words organized hierarchically, with an optional
      unit type.  Examples:

        file input /some/path/foo.cdf (byte)
        file output kfs://dev008/some/other/path/bar.txt (byte)
        stream input /stream/path

      The hierarchical organization makes it relatively simple for
      analysis tools to aggregate results.  For example, it could
      aggregate all file stats, or all file input stats, or just file
      inputs on kfs.

      The optional unit describes the interpretation of the value.  If
      omitted, the value is assumed to be a unitless count.

   Components:

     PerformanceLog
       addItem(item)
       removeItem(item)

       logMessage(type, message):
          lock
          logMessageUnsync(type, message)
          unlock

       messageThread():
          logMessage("EVENT", "Tracker beginning")
          while not done:
             sleep(reportInterval)
             lock
             for x in tracker:
                msg = x.getMessage()
                if msg:
                   logMessageUnsync(x.getType(), msg)
             unlock
          logMessage("EVENT", "Tracker ends")

     TrackedItem -- an item monitored by a PerformanceLog

     PerformanceVariable -- labeled integer item that changes over time
     PerformanceInterval -- labeled interval with begin and end points

*/


#ifndef WARP_PERFMON_H
#define WARP_PERFMON_H





//----------------------------------------------------------------------------
// Some application
//----------------------------------------------------------------------------

//int main(int ac, char ** av)
//{
//    // At application init time, various performance monitoring
//    // services register init functions with central PerformanceLog.
//    //
//
//    string arg;
//    if(opt.get("perflog", arg))
//    {
//        // Set up performance logging
//        //   - create performance tracker
//        //   - open output log file
//        //   - start monitor thread
//        //   - install File hooks
//        //   - install Stream hooks
//        //   - install rusage monitor
//        
//        PerformanceLog::init(arg);
//    }
//
//
//
//    
//    for(;;)
//    {
//        
//    }
//    
//
//}



#endif // WARP_PERFMON_H
