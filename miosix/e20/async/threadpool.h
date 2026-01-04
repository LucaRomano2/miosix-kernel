#pragma once

#include <list>
#include <functional>
#include <miosix.h>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <future>
#include "config/miosix_settings.h"

using namespace miosix;

namespace threadpool {

class Task
{
public:
    virtual void execute() = 0;
    virtual ~Task() = default;
};

class PriorityEventQueue
{
public:

    void post(Task* fut, Priority priority);

    void post(Task* fut);

    void child_run(int prio);

    void start_child_run();

    void run(int prio);

    void startRun();

    static PriorityEventQueue& instance();

    PriorityEventQueue(const PriorityEventQueue&) = delete;
    PriorityEventQueue& operator= (const PriorityEventQueue&) = delete;

private:
    std::list<Task*> events[NUM_PRIORITIES]; ///< Event queue
    mutable KernelMutex m[NUM_PRIORITIES]; ///< Mutex for synchronisation
    ConditionVariable cv[NUM_PRIORITIES]; ///< Condition variable for synchronisation
    int num_core=2;
    int in_execution[2];

    PriorityEventQueue() {
        startRun();
    }
};

template<typename R>
class Task_impl : public Task
{
public:
    void execute() override
    {
        Lock<KernelMutex> l(m);
        return_value = task();
        finished = true;
        cv.signal();
        if(!can_return) delete this;
    }

    R get()
    {
        Lock<KernelMutex> l(m);
        while(!finished) 
        {
            PriorityEventQueue::instance().start_child_run();
            cv.wait(l);
        }
        return return_value;
    }

    bool is_finished()
    {
        Lock<KernelMutex> l(m);
        return finished;
    }

    void cannot_finish()
    {
        can_return = false;
    }

    Task_impl(std::function<R()> task) : task(std::move(task)), finished(false), can_return(true) {}

private:
    std::function<R()> task;
    R return_value;
    KernelMutex m;
    ConditionVariable cv;
    bool finished;
    bool can_return;
};

template<typename R>
class future
{
public:
    R get()
    {
        r = task->get();
        return r;
    }

    future(Task_impl<R>* task)
    {
        this->task = task;
    }

    ~future()
    {
        if(task->is_finished()) delete task;
        else task->cannot_finish();
    }

private:
    Task_impl<R>* task;
    R r;
};

template<class F, class... Args>
threadpool::future<typename std::result_of<F(Args...)>::type> async(F&& f, Args&&... args)
{
    typedef typename std::result_of<F(Args...)>::type return_type;
    std::function<return_type()> t = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    Task_impl<return_type>* task_impl = new Task_impl<return_type>(std::move(t));
    Task* task = task_impl;
    future<return_type> fut = future<return_type>(task_impl);
    PriorityEventQueue::instance().post(task);
    return fut;
}

}