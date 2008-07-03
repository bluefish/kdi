//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-03-08
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

#ifndef WARP_HTML_FILE_TOKENIZER_H
#define WARP_HTML_FILE_TOKENIZER_H

#include "html_tokenizer.h"
#include "file.h"
#include "buffer.h"
#include <boost/utility.hpp>

namespace warp
{
    class HtmlFileTokenizer;
}

//----------------------------------------------------------------------------
// HtmlFileTokenizer
//----------------------------------------------------------------------------
class warp::HtmlFileTokenizer : public boost::noncopyable
{
    HtmlTokenizer lastValid;
    HtmlTokenizer tok;

    FilePtr input;
    Buffer buf;

    size_t bytesIn;
    size_t bytesOut;
    bool eof;
    bool firstTag;

public:
    explicit
    HtmlFileTokenizer(FilePtr const & input, size_t bufSize = 64 << 10);

    bool get(HtmlToken & x);

    size_t getBytesIn() const { return bytesIn; }
    size_t getBytesOut() const { return bytesOut; }
};

#endif // WARP_HTML_FILE_TOKENIZER_H
