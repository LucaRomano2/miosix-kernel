#include "lazy.h"

using namespace miosix;

namespace lazy {

//
// Class PriorityEventQueue
//

void PriorityEventQueue::post(Task* task, Priority priority)
{
    Lock<KernelMutex> l(m[priority.get()]);
    Thread* current_thread=Thread::getCurrentThread();
    if(scheduler_threads.count(current_thread))
    {
        --in_execution[priority.get()];
        to_terminate.insert(current_thread);
    }
    if(in_execution[priority.get()].load() + 1 >= num_core)
    {
        Task* task_restart = new TaskRestart(current_thread); 
        events[priority.get()].push_back(task);
        events[priority.get()].push_back(task_restart);
        cv[priority.get()].signal();
        Unlock<KernelMutex> u(l);
        Thread::wait();
    }else{
        events[priority.get()].push_back(task);
        cv[priority.get()].signal();
    }
}

void PriorityEventQueue::post(Task* task)
{
    Thread* current_thread=Thread::getCurrentThread();
    post(task, current_thread->getPriority());
}

void PriorityEventQueue::run(int prio)
{
    Thread* thread=Thread::getCurrentThread();
    thread->setPriority(Priority(prio));
    scheduler_threads.insert(thread);
    Task* t;
    while(!to_terminate.count(thread))
    {
        Lock<KernelMutex> l(m[prio]);
        while(events[prio].empty()) cv[prio].wait(l);
        t = events[prio].front();
        events[prio].pop_front();
        Unlock<KernelMutex> u(l);
        ++in_execution[prio];
        t->execute();
        --in_execution[prio];
    }
    to_terminate.erase(thread);
}

void PriorityEventQueue::startRun(){
    Thread* current_thread = Thread::getCurrentThread();
    for(int i=1-1;i>=0;i--)
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