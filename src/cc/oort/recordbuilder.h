//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-12
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

#ifndef OORT_RECORDBUILDER_H
#define OORT_RECORDBUILDER_H

#include "record.h"
#include "warp/builder.h"

namespace oort
{
    class RecordBuilder;
}


//----------------------------------------------------------------------------
// RecordBuilder
//----------------------------------------------------------------------------
class oort::RecordBuilder : public warp::Builder
{
    // Usage:
    //
    // static void usage()
    // {
    //     RecordBuilder b;
    // 
    //     b.setType(SomeRecord::TYPECODE);
    //     b.setVersion(SomeRecord::VERSION);
    //     b.setFlags(0);
    //     
    //     Record r;
    //     for(int i = 0; i < 1000; ++i)
    //     {
    //         for(int j = 0; j <= i; ++j)
    //             b.append<uint16_t>(j);
    // 
    //         b.build(r, allocator);
    //         output.put(r);
    //     }
    // }
    // 
    // static void usage2()
    // {
    //     struct GraphNode
    //     {
    //         enum { ALIGNMENT = 4 };
    // 
    //         Offset<char> name;
    //         uint16_t nameLen;
    // 
    //         uint16_t nLinks;
    //         Offset<GraphNode> links[0]; // links[nLinks]
    //     };
    // 
    //     struct GraphRecord
    //     { 
    //         enum
    //         {
    //             TYPECODE  = MAKE_TYPE('G','r','a','f'),
    //             VERSION   = 0,
    //             FLAGS     = 0,
    //             ALIGNMENT = 4,
    //         };
    // 
    //         uint32_t nNodes;
    //         Offset<GraphNode> nodes[0]; // nodes[nNodes]
    //     };
    // 
    //     // Init builder
    //     RecordBuilder b;
    //     b.setHeader<GraphRecord>();
    //     
    //     // Write GraphRecord::nNodes
    //     b.append<uint32_t>(graph.size());
    // 
    //     // Write references to each GraphNode (and corresponding node data)
    //     std::vector<BuilderBlock *> refs(graph.size(), (BuilderBlock *)0);
    //     for(node_iterator ni = graph.begin(); ni != graph.end(); ++ni)
    //     {
    //         // Make reference target for this node if we haven't already
    //         int idx = getNodeIndex(*ni);
    //         assert(idx >= 0 && idx < graph.size());
    //         if(!refs[idx])
    //             refs[idx] = b.subblock(GraphNode::ALIGNMENT);
    // 
    //         // Ref refers to GraphNode target
    //         BuilderBlock * sub = refs[idx];
    // 
    //         // Write GraphRecord::nodes[idx]
    //         b.appendOffset(sub);
    // 
    //         // Write GraphNode::name and GraphNode::nameLen
    //         BuilderBlock * nameBlock = b.subblock(1);
    //         std::string const & name = ni->getNodeName();
    //         nameBlock->append(name.c_str(), name.length());
    //         sub->appendOffset(nameBlock);
    //         sub->append<uint16_t>(name.length());
    // 
    //         // Write GraphNode::nLinks
    //         sub->append<uint16_t>(ni->linkCount());
    // 
    //         // Write link target references
    //         for(node_iterator lni = ni->linksBegin(); lni != ni->linksEnd(); ++lni)
    //         {
    //             // Make reference target for link destination if we haven't already
    //             int linkIdx = getNodeIndex(*lni);
    //             assert(linkIdx >= 0 && linkIdx < graph.size());
    //             if(!refs[linkIdx])
    //                 refs[linkIdx] = b.subblock(GraphNode::ALIGNMENT);
    // 
    //             // Write GraphNode::links[linkIdx]
    //             sub->appendOffset(refs[linkIdx]);
    //         }
    //     }
    // 
    //     // Assemble record and output
    //     Record r;
    //     b.build(r, allocator);
    //     output.put(r);
    // }

    HeaderSpec::Fields hdrFields;

public:
    void setType(uint32_t type) { hdrFields.type = type; }
    void setVersion(uint32_t version) { hdrFields.version = version; }
    void setFlags(uint32_t flags) { hdrFields.flags = flags; }

    template <class RecordType>
    void setHeader()
    {
        hdrFields.type = RecordType::TYPECODE;
        hdrFields.version = RecordType::VERSION;
        hdrFields.flags = RecordType::FLAGS;
    }

    void exportTo(Record & r, Allocator * alloc)
    {
        assert(alloc);
        hdrFields.length = getFinalSize();
        char * dataPtr = alloc->alloc(r, hdrFields);
        warp::Builder::exportTo(dataPtr);
    }

    void build(Record & r, Allocator * alloc)
    {
        finalize();
        exportTo(r, alloc);
        reset();
    }
};


#endif // OORT_RECORDBUILDER_H
