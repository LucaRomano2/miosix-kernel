#include "lazy.h"

using namespace miosix;

namespace lazy {

//
// Class LazyScheduler
//

void LazyScheduler::lazy_execute(future_base* fut, int prio)
{
    fut->execute();
    Thread* t;
    {
        Lock<KernelMutex> l(m[prio]);
        t = events[prio].front();
    }
    t->wakeup();
}

void LazyScheduler::post(future_base* fut, Priority priority)
{
    std::thread t(&LazyScheduler::lazy_execute, this, fut);
    t.detach();
    Thread* current_thread=Thread::getCurrentThread();
    {
        Lock<KernelMutex> l(m[priority.get()]);
        events[priority.get()].push_back(current_thread);
    }
    current_thread->wait();
    //cv[priority.get()].signal();
}

void LazyScheduler::post(future_base* fut)
{
    Thread* current_thread=Thread::getCurrentThread();
    post(fut, current_thread->getPriority());
}

void LazyScheduler::run(int prio)
{
    Thread* thread=Thread::getCurrentThread();
    thread->setPriority(Priority(prio));
    future_base* fut;
    for(;;)
    {
        Lock<KernelMutex> l(m[prio]);
        while(events[prio].empty()) cv[prio].wait(l);
        fut=events[prio].front();
        events[prio].pop_front();
        {
            Unlock<KernelMutex> u(l);
            fut->execute();
        }
    }
}

void LazyScheduler::startRun(){
    Thread* current_thread = Thread::getCurrentThread();
    for(int i=NUM_PRIORITIES-1;i>=0;i--)
    {
        current_thread->setPriority(Priority(i));
        for(int j=0;j<num_core;j++)
        {
            std::thread t(&LazyScheduler::run, this, i);
            t.detach();
        }
    }
}

LazyScheduler& LazyScheduler::instance()
{
    static LazyScheduler singleton;
    return singleton;
}

}