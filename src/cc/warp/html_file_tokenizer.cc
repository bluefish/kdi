//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-03-08
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

#include <warp/html_file_tokenizer.h>
#include <ex/exception.h>

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
