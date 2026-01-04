#include "threadpool.h"

using namespace miosix;

namespace threadpool {

//
// Class PriorityEventQueue
//

void PriorityEventQueue::post(Task* task, Priority priority)
{
    Lock<KernelMutex> l(m[priority.get()]);
    events[priority.get()].push_back(task);
    cv[priority.get()].signal();
}

void PriorityEventQueue::post(Task* task)
{
    Thread* current_thread=Thread::getCurrentThread();
    post(task, current_thread->getPriority());
}

void PriorityEventQueue::child_run(int prio)
{
    Thread* thread=Thread::getCurrentThread();
    thread->setPriority(Priority(prio));
    Task* t;
    in_execution[prio]++;
    while(in_execution[prio] - 1 < num_core)
    {
        Lock<KernelMutex> l(m[prio]);
        while(events[prio].empty()) cv[prio].wait(l);
        t = events[prio].front();
        events[prio].pop_front();
        {
            Unlock<KernelMutex> u(l);
            t->execute();
        }
    }    
}

void PriorityEventQueue::start_child_run()
{
    Thread* thread=Thread::getCurrentThread();
    std::thread(&PriorityEventQueue::child_run, this, thread->getPriority().get());    
}

void PriorityEventQueue::run(int prio)
{
    Thread* thread=Thread::getCurrentThread();
    thread->setPriority(Priority(prio));
    Task* t;
    for(;;)
    {
        Lock<KernelMutex> l(m[prio]);
        while(events[prio].empty()) cv[prio].wait(l);
        t = events[prio].front();
        events[prio].pop_front();
        {
            Unlock<KernelMutex> u(l);
            t->execute();
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
            in_execution[i]++;
        }
    }
}

PriorityEventQueue& PriorityEventQueue::instance()
{
    static PriorityEventQueue singleton;
    return singleton;
}

}