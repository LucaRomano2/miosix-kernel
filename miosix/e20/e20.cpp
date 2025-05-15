/***************************************************************************
 *   Copyright (C) 2012, 2013, 2014, 2015, 2016 by Terraneo Federico       *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 *   This program is distributed in the hope that it will be useful,       *
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of        *
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
 *   GNU General Public License for more details.                          *
 *                                                                         *
 *   As a special exception, if other files instantiate templates or use   *
 *   macros or inline functions from this file, or you compile this file   *
 *   and link it with other works to produce a work based on this file,    *
 *   this file does not by itself cause the resulting work to be covered   *
 *   by the GNU General Public License. However the source code for this   *
 *   file must still be made available in accordance with the GNU General  *
 *   Public License. This exception does not invalidate any other reasons  *
 *   why a work based on this file might be covered by the GNU General     *
 *   Public License.                                                       *
 *                                                                         *
 *   You should have received a copy of the GNU General Public License     *
 *   along with this program; if not, see <http://www.gnu.org/licenses/>   *
 ***************************************************************************/

#include "e20.h"

using namespace std;

namespace miosix {

//
// Class EventQueue
//

void EventQueue::post(function<void ()> event)
{
    Lock<KernelMutex> l(m);
    events.push_back(event);
    cv.signal();
}

void EventQueue::run()
{
    Lock<KernelMutex> l(m);
    for(;;)
    {
        while(events.empty()) cv.wait(l);
        function<void ()> f=events.front();
        events.pop_front();
        {
            Unlock<KernelMutex> u(l);
            f();
        }
    }
}

void EventQueue::runOne()
{
    function<void ()> f;
    {
        Lock<KernelMutex> l(m);
        if(events.empty()) return;
        f=events.front();
        events.pop_front();
    }
    f();
}

//
// Class PriorityEventQueueBasic
//

void PriorityEventQueueBasic::post(function<void ()> event, Priority priority)
{
    Lock<KernelMutex> l(m[priority.get()]);
    events[priority.get()].push_back(event);
    cv[priority.get()].signal();
}

void PriorityEventQueueBasic::post(function<void ()> event)
{
    Thread* current_thread=Thread::getCurrentThread();
    post(event, current_thread->getPriority().get());
}

void PriorityEventQueueBasic::run()
{
    Thread* thread=Thread::getCurrentThread();
    int prio=thread->getPriority().get();
    for(;;)
    {
        Lock<KernelMutex> l(m[prio]);
        while(events[prio].empty()) cv[prio].wait(l);
        function<void ()> f=events[prio].front();
        events[prio].pop_front();
        {
            Unlock<KernelMutex> u(l);
            f();
        }
    }
}

void PriorityEventQueueBasic::runOne()
{
    Thread* current_thread=Thread::getCurrentThread();
    int prio=current_thread->getPriority().get();
    function<void ()> f;
    {
        Lock<KernelMutex> l(m[prio]);
        if(events[prio].empty()) return;
        f=events[prio].front();
        events[prio].pop_front();
    }
    f();
}

void PriorityEventQueueBasic::startRun(){
    Thread* current_thread = Thread::getCurrentThread();
    for(int i=NUM_PRIORITIES-1;i>=0;i--)
    {
        current_thread->setPriority(Priority(i));
        for(int j=0;j<num_core;j++)
        {
            std::thread t(&PriorityEventQueueBasic::run, this);
        }
    }
}

//
// Class PriorityEventQueue
//

Priority PriorityEventQueue::getThreadPriority()
{
    Thread *thread = Thread::getCurrentThread();
    return thread->getPriority();
}

Priority PriorityEventQueue::getHighestPriority()
{
    for(int i=NUM_PRIORITIES-1;i>=0;i--)
    {
        if(events[i].size()!=0) return i;
    }
    return 0;
}

Thread* PriorityEventQueue::getLowerExecutingThread()
{
    for(int i=0;i<NUM_PRIORITIES;i++)
    {
        if(runningThreads[i].size()!=0)
        {
            return runningThreads[i].front();
        }
    }
    return nullptr;
}

Thread* PriorityEventQueue::getHigherWaitingThread()
{
    for(int i=0;i<NUM_PRIORITIES;i++)
    {
        if(waitingThreads[i].size()!=0)
        {
            return waitingThreads[i].front();
        }
    }
    return nullptr;
}

void PriorityEventQueue::post(void (*event)(void *), Priority priority)
{
    Lock<FastMutex> l(m);
    Thread* lower_executing_thread=getLowerExecutingThread();
    if(lower_executing_thread != nullptr && lower_executing_thread->getPriority()<priority)
    {
        lower_executing_thread->wait();
        runningThreads[lower_executing_thread->getPriority().get()].pop_front();
        waitingThreads[lower_executing_thread->getPriority().get()].push_back(lower_executing_thread);
        Thread* t = Thread::create(*event, 768, priority, NULL, Thread::JOINABLE);
        runningThreads[priority.get()].push_back(t);
    }
    else
    {
        events[priority.get()].push_back(event);
        cv.signal();
    }
}

void PriorityEventQueue::post(void (*event)(void *))
{
    Priority priority=getThreadPriority();
    post(event, priority);
}

// cambiare la priorità del thread che esegue la funzione
void PriorityEventQueue::run()
{
    Lock<FastMutex> l(m);
    for(;;)
    {
        while(empty() || numRunningThreads() == num_core) cv.wait(l);
        Thread* higher_waiting_thread=nullptr;
        if(getHigherWaitingThread()!=nullptr)
        {
            waitingThreads[higher_waiting_thread->getPriority().get()].pop_front();
            higher_waiting_thread->wakeup();
            runningThreads[higher_waiting_thread->getPriority().get()].push_back(higher_waiting_thread);
        }
        else
        {
            int highest_priority=getHighestPriority().get();
            void (*f)(void*)=events[highest_priority].front();
            events[highest_priority].pop_front();
            {
                Unlock<FastMutex> u(l);
                // salvare priorità corrente
                // prende la priorità di f
                Thread *current_thread = Thread::getCurrentThread();
                Priority current_priority = current_thread->getPriority();
                current_thread->setPriority(Priority(highest_priority));
                Thread* t = Thread::create(*f, 768, Priority(highest_priority), NULL, Thread::JOINABLE);
                runningThreads[highest_priority].push_back(t);
                current_thread->setPriority(current_priority);
                // ripristana la priorità salvata
                // anche in runOne()
            }
        }
    }
}

void PriorityEventQueue::runOne()
{
    int highest_priority;
    void (*f)(void*);
    {
        Lock<FastMutex> l(m);
        if(empty()) return;
        highest_priority=getHighestPriority().get();
        f=events[highest_priority].front();
        events[highest_priority].pop_front();
    }
    Thread *current_thread = Thread::getCurrentThread();
    Priority current_priority = current_thread->getPriority();
    current_thread->setPriority(Priority(highest_priority));
    Thread* t = Thread::create(*f, 768, 0, NULL, Thread::JOINABLE);
    runningThreads[highest_priority].push_back(t);
    current_thread->setPriority(current_priority);
}

} //namespace miosix
