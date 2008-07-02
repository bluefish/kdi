
#ifndef FLUX_UNIQP_H
#define FLUX_UNIQP_H

#include "stream.h"
#include <vector>
#include <functional>
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // UniqKeepFirst
    //------------------------------------------------------------------------
    template <class T>
    struct UniqKeepFirstP
    {
        void operator()(T & out, std::vector<T> const & in)
        {
            assert(in.size() > 0);
            out = in[0];
        }
    };
    
    //------------------------------------------------------------------------
    // UniqKeepLast
    //------------------------------------------------------------------------
    template <class T>
    struct UniqKeepLastP
    {
        void operator()(T & out, std::vector<T> const & in)
        {
            assert(in.size() > 0);
            out = in[in.size()-1];
        }
    };

    //------------------------------------------------------------------------
    // Uniq
    //------------------------------------------------------------------------
    template <class T,
              class Lt=std::less<T>,
              class KeepPolicy=UniqKeepFirstP<T> >
    class UniqP : public Stream<T>
    {
    public:
        typedef UniqP<T,Lt,KeepPolicy> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        T emptyT;
        T backup; // very small allocation benefit in making this a class member
        base_handle_t stream;
        bool isOutput;
        std::vector<T> holder;
        Lt lt;
        KeepPolicy keepPolicy;
        uint32_t maxMergeSize;

    public:
        explicit UniqP(Lt const & lt = Lt(),
                       KeepPolicy const & keepPolicy = KeepPolicy(),
                       uint32_t maxMergeSize = 1000) :
            isOutput(false),
            lt(lt),
            keepPolicy(keepPolicy),
            maxMergeSize(maxMergeSize)
        {
        }

        ~UniqP()
        {
            if(isOutput && !holder.empty())
            {
                assert(stream);
                stream->put(holder[0]);
            }
        }

        void pipeFrom(base_handle_t const & input)
        {
            if(!holder.empty())
            {
                if(isOutput)
                {
                    assert(stream);
                    stream->put(holder[0]);
                }
                holder.clear();
            }
            isOutput = false;
            stream = input;
        }

        void pipeTo(base_handle_t const & output)
        {
            if(!holder.empty())
            {
                if(isOutput)
                {
                    assert(stream);
                    stream->put(holder[0]);
                }
                holder.clear();
            }
            isOutput = true;
            stream = output;
        }

        bool get(T & x)
        {
            assert(stream && !isOutput);
            
            for(;;)
            {
                holder.push_back(emptyT);

                // holder.empty() only happens on first/last reads
                // Maybe we can move it (to initialization?)
                // And then terminate in a different manner
                if(holder.empty())
                {
                    if(!stream->get(holder[holder.size()-1]))
                        return false;
                }
                else if(stream->get(holder[holder.size()-1]))
                {
                    // If we assume a sort order, we could save a compare
                    if(lt(holder[0], holder[holder.size()-1]) || 
                       lt(holder[holder.size()-1], holder[0]))
                    {
                        backup = holder.back();
                        holder.pop_back();
                            
                        if(holder.size() == 1)
                            x = holder[0];
                        else
                            keepPolicy(x,holder);
                            
                        holder.clear();
                        holder.push_back(backup);
                        return true;
                    }
                    else if(holder.size() > maxMergeSize)
                    {
                        keepPolicy(backup,holder);
                            
                        holder.clear();
                        holder.push_back(backup);
                    }
                }
                else
                {
                    if(holder.size() == 1)
                        x = holder[0];
                    else
                        keepPolicy(x,holder);
                        
                    holder.clear();
                    return true;
                }
            }
        }

        void put(T const & x)
        {
            assert(stream && isOutput);
            
            // holder.empty() only happens on first put
            // maybe we can move this to initialization
            if(holder.empty())
                holder.push_back(x);
            // If we assume a sort order, we could save a compare
            else if(lt(holder[0], x) || lt(x, holder[0]))
            {
                T toOut;
                if(holder.size() == 1)
                    toOut = holder[0];
                else
                    keepPolicy(toOut, holder);

                stream->put(toOut);

                holder.clear();
                holder.push_back(x);
            }
            else
                holder.push_back(x);
        }

        void flush()
        {
            assert(stream && isOutput);

            if(holder.size() > 0)
            {
                stream->put(holder[0]);
		holder.clear();
            }
            stream->flush();
        }
    };


    template <class T>
    typename UniqP<T>::handle_t makeUniqP()
    {
        typename UniqP<T>::handle_t s(new UniqP<T>());
        return s;
    }

    template <class T, class Lt>
    typename UniqP<T,Lt>::handle_t makeUniqP(Lt const & lt)
    {
        typename UniqP<T,Lt>::handle_t s(new UniqP<T,Lt>(lt));
        return s;
    }

    template <class T, class Lt, class Kp>
    typename UniqP<T,Lt,Kp>::handle_t makeUniqP(Lt const & lt, Kp const & kp)
    {
        typename UniqP<T,Lt,Kp>::handle_t s(new UniqP<T,Lt,Kp>(lt,kp));
        return s;
    }

    template <class T, class Lt, class Kp>
    typename UniqP<T,Lt,Kp>::handle_t makeUniqP(Lt const & lt, Kp const & kp, int32_t maxMergeSize)
    {
        typename UniqP<T,Lt,Kp>::handle_t s(new UniqP<T,Lt,Kp>(lt,kp,maxMergeSize));
        return s;
    }

}

#endif // FLUX_UNIQP_H
