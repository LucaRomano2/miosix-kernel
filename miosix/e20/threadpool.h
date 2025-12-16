#pragma once

#include <list>
#include <functional>
#include <miosix.h>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <future>
#include "callback.h"
#include "config/miosix_settings.h"

using namespace miosix;

namespace threadpool {

class future_base
{
public:
    virtual void execute() = 0;
    virtual ~future_base() = default;
};

template<typename R>
class future : public future_base
{
public:
    void execute() override
    {
        Lock<KernelMutex> l(m);

        return_value = task();
        is_finished = true;
        cv.signal();
    }

    R get()
    {
        Lock<KernelMutex> l(m);
        while(!is_finished) cv.wait(l);
        return return_value;
    }

    future(std::function<R()> task) : task(std::move(task)), is_finished(false) {}

private:
    std::function<R()> task;
    R return_value;
    KernelMutex m;
    ConditionVariable cv;
    bool is_finished;
};

class PriorityEventQueue
{
public:

    void post(future_base* fut, Priority priority);

    /**
     * Post an event to the queue. This function never blocks.
     * 
     * \param event function function to be called in the thread that calls
     * run() or runOne(). Bind can be used to bind parameters to the function.
     * \throws std::bad_alloc if there is not enough heap memory
     */
    void post(future_base* fut);

    /**
     * This function blocks waiting for events being posted, and when available
     * it calls the event function. To return from this event loop an event
     * function must throw an exception.
     * 
     * \throws any exception that is thrown by the event functions
     */
    void run(int prio);

    void startRun();

    static PriorityEventQueue& instance();

    PriorityEventQueue(const PriorityEventQueue&) = delete;
    PriorityEventQueue& operator= (const PriorityEventQueue&) = delete;

private:
    std::list<future_base*> events[NUM_PRIORITIES]; ///< Event queue
    mutable KernelMutex m[NUM_PRIORITIES]; ///< Mutex for synchronisation
    ConditionVariable cv[NUM_PRIORITIES]; ///< Condition variable for synchronisation
    int num_core=2;
    PriorityEventQueue() {
        startRun();
    }
};

template<class F, class... Args>
threadpool::future<typename std::result_of<F(Args...)>::type>* async(F&& f, Args&&... args)
{
    typedef typename std::result_of<F(Args...)>::type return_type;
    std::function<return_type()> task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    future<return_type>* fut = new future<return_type>(std::move(task));
    future_base* fut_base = fut;
    PriorityEventQueue::instance().post(fut_base);
    return fut;
}

}