#ifndef OORT_UNIQMGR_H
#define OORT_UNIQMGR_H

#include "record.h"
#include "typeregistry.h"
#include "warp/singleton.h"

#include <boost/utility.hpp>
#include <string>

namespace oort
{
    typedef void (UniqFunc)(Record &, Record const &, Record const &);

    class UniqManager;
    class Uniqifier;
}

//----------------------------------------------------------------------------
// UniqManager
//----------------------------------------------------------------------------
class oort::UniqManager : public TypeRegistry<UniqFunc *>,
                          public warp::Singleton<UniqManager>
{
    friend class warp::Singleton<UniqManager>;
    UniqManager() :
        TypeRegistry<UniqFunc *>("oort::UniqManager")
    {
    }
    
    template <class T> struct RegHelper
    {
        static void uniqueAddPolicy(
            Record & out, Record const & in1, Record const & in2)
        {
            out = in1.clone();
            *const_cast<T *>(out.cast<T>()) += *in2.cast<T>();
        }
    };

    static void uniqKeepFirst(
        Record & out, Record const & in1, Record const & in2)
    {
        out = in1;
    }

public:
    template <class T> void regUniqAdd()
    {
        registerType<T>(&RegHelper<T>::uniqueAddPolicy);
    }

    template <class T> void regUniqFirst()
    {
        registerType<T>(&uniqKeepFirst);
    }
};

//----------------------------------------------------------------------------
// Uniqifier
//----------------------------------------------------------------------------
class oort::Uniqifier
{
    mutable UniqFunc * uniqFunc;
    mutable uint32_t uniqType;
    mutable uint32_t uniqVer;
    
public:
    Uniqifier() : uniqFunc(0), uniqType(0), uniqVer(0) {}

    void operator()(Record & out, Record const & in1, Record const & in2)
    {
        uint32_t atype = in1.getType();
        uint32_t aver = in1.getVersion();
        uint32_t btype = in2.getType();
        uint32_t bver = in2.getVersion();

        assert(atype == btype && aver == bver);

        if(atype != btype)
            ex::raise<TypeError>("can't unique different types: "
                                 "typeA='%s', typeB='%s'",
                                 warp::typeString(atype).c_str(),
                                 warp::typeString(btype).c_str());
        else if(aver != bver)
            ex::raise<VersionError>("can't unique different versions of "
                                    "type '%s': verA=%u, verB=%u",
                                    warp::typeString(atype).c_str(),
                                    aver, bver);
        else if(!uniqFunc || uniqType != atype || uniqVer != aver)
        {
            uniqFunc = UniqManager::get().lookupType(atype, aver);
            uniqType = atype;
            uniqVer = aver;
        }
        
        (*uniqFunc)(out, in1, in2);
    }
};

#endif // OORT_UNIQMGR_H

