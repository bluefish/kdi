//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/html_file_tokenizer.h#1 $
//
// Created 2007/03/08
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
