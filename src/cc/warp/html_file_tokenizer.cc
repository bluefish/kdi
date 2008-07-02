//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/html_file_tokenizer.cc#1 $
//
// Created 2007/03/08
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "html_file_tokenizer.h"
#include "ex/exception.h"

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// HtmlTokenizer
//----------------------------------------------------------------------------
HtmlFileTokenizer::HtmlFileTokenizer(FilePtr const & input, size_t bufSize) :
    input(input),
    buf(bufSize),
    bytesIn(0),
    bytesOut(0),
    eof(false),
    firstTag(true)
{
    if(!input)
        raise<ValueError>("null file input");

    // Read some input
    size_t sz = buf.read(input);
    eof = (sz == 0);
    bytesIn += sz;
    buf.flip();
    
    // Set scanning range for tokenizer
    tok.setInput(buf.position(), buf.remaining());
}

bool HtmlFileTokenizer::get(HtmlToken & x)
{
    size_t sz;

    for(;;)
    {
        // Save tokenizer state
        lastValid = tok;

        // Try to get a token
        if(tok.get(x))
        {
            // Use token if we're sure it hasn't come from partial
            // information.
            if(eof || tok.remaining() || firstTag)
            {
                // Got a tag
                firstTag = false;
                return true;
            }

            // Tokenizer exhausted input and we're not at EOF.
            // Restore to old state and get more data.
            tok = lastValid;
        }

        // If there is no more input, there are no more tags
        if(eof)
            return false;

        // Position buffer at tokenizer's current position.
        sz = tok.position() - buf.position();
        bytesOut += sz;
        buf.position(buf.position() + sz);

        // Get more input
        buf.compact();
        sz = buf.read(input);
        eof = (sz == 0);
        bytesIn += sz;
        buf.flip();

        // Set scanning range for tokenizer
        tok.setInput(buf.position(), buf.remaining());

        // Reset first tag flag
        firstTag = true;
    }



    // -- while(!eof)
    // -- {
    // --     // Get more input
    // --     buf.compact();
    // --     size_t sz = buf.read(input);
    // --     eof = (sz == 0);
    // --     bytesIn += sz;
    // --     buf.flip();
    // -- 
    // --     // Set scanning range for tokenizer
    // --     tok.setInput(buf.position(), buf.remaining());
    // -- 
    // --     // Save tokenizer state
    // --     lastValid = tok;
    // -- 
    // --     // Get tokens
    // --     bool firstTag = true;
    // --     while(tok.get(x))
    // --     {
    // --         // Use token if we're sure it hasn't come from partial
    // --         // information.
    // --         if(eof || tok.remaining() || firstTag)
    // --         {
    // --             firstTag = false;
    // -- 
    // --             // Save tokenizer state
    // --             lastValid = tok;
    // -- 
    // --             // Got a tag
    // --             return true;
    // --         }
    // --         else
    // --         {
    // --             // Tokenizer exhausted input and we're not at EOF.
    // --             // Restore to old state and get more data.
    // --             tok = lastValid;
    // --             break;
    // --         }
    // --     }
    // -- 
    // --     // Position buffer at tokenizer's current position.
    // --     sz = tok.position() - buf.position();
    // --     bytesOut += sz;
    // --     buf.position(buf.position() + sz);
    // -- }
    // -- 
    // -- // Nothing left
    // -- return false;
}
