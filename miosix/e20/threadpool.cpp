#include "threadpool.h"

using namespace miosix;

namespace threadpool {

//
// Class PriorityEventQueue
//

void PriorityEventQueue::post(future_base* fut, Priority priority)
{
    Lock<KernelMutex> l(m[priority.get()]);
    events[priority.get()].push_back(fut);
    cv[priority.get()].signal();
}

void PriorityEventQueue::post(future_base* fut)
{
    Thread* current_thread=Thread::getCurrentThread();
    post(fut, current_thread->getPriority());
}

void PriorityEventQueue::run(int prio)
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

void PriorityEventQueue::startRun(){
    Thread* current_thread = Thread::getCurrentThread();
    for(int i=NUM_PRIORITIES-1;i>=0;i--)
    {
        current_thread->setPriority(Priority(i));
        for(int j=0;j<num_core;j++)
        {
            std::thread t(&PriorityEventQueue::run, this, i);
            t.detach();
        }
    }
}

PriorityEventQueue& PriorityEventQueue::instance()
{
    static PriorityEventQueue singleton;
    return singleton;
}

}