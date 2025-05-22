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
// Class PriorityEventQueueOptimized
//

int PriorityEventQueueOptimized::getRunningEvents()
{
    int sum=0;
    for(int i=NUM_PRIORITIES;i>=0;i--)
    {
        sum+=running_events[i];
    }
    return sum;
}

int PriorityEventQueueOptimized::getLowerExecutingEventPriority()
{
    int core=0;
    for(int i=NUM_PRIORITIES;i>=0;i--)
    {
        if(core+running_events[i]>num_core) return i;
        else core+=running_events[i];
    }
    return -1;
}

bool PriorityEventQueueOptimized::canSchedule(int prio)
{
    if(getRunningEvents()<num_core || prio<getLowerExecutingEventPriority()) return true;
    return false;
}

bool PriorityEventQueueOptimized::setEvent(std::function<void ()>& f, int* prio)
{
    Lock<KernelMutex> l(m);
    if(*prio!=-1)
    {
        running_events[*prio]--;
    }
    for(int i=NUM_PRIORITIES-1;i>=0;i--)
    {
        if(events[i].size()!=0)
        {
            f=events[i].front();
            events[i].pop_front();
            running_events[i]++;
            *prio=i;
            return true;
        }
    }
    return false;
}

void PriorityEventQueueOptimized::post(function<void ()> event, Priority priority)
{
    Lock<KernelMutex> l(m);
    events[priority.get()].push_back(event);
    if(canSchedule(priority.get()))
    {
        Unlock<KernelMutex> l(m);
        std::thread t(&PriorityEventQueueOptimized::runThread, this);
    }
    cv.signal();
}

void PriorityEventQueueOptimized::post(function<void ()> event)
{
    Thread* current_thread=Thread::getCurrentThread();
    post(event, current_thread->getPriority().get());
}

void PriorityEventQueueOptimized::runThread()
{
    Thread* current_thread = Thread::getCurrentThread();
    std::function<void ()> f;
    int prio=-1;
    while(setEvent(f, &prio))
    {
        current_thread->setPriority(Priority(prio));
        f();
    }
}

void PriorityEventQueueOptimized::run()
{
    Thread* thread=Thread::getCurrentThread();
    int prio=thread->getPriority().get();
    for(;;)
    {
        Lock<KernelMutex> l(m);
        while(events[prio].empty()) cv.wait(l);
        function<void ()> f=events[prio].front();
        events[prio].pop_front();
        {
            Unlock<KernelMutex> u(l);
            f();
        }
    }
}

void PriorityEventQueueOptimized::runOne()
{
    Thread* current_thread=Thread::getCurrentThread();
    int prio=current_thread->getPriority().get();
    function<void ()> f;
    {
        Lock<KernelMutex> l(m);
        if(events[prio].empty()) return;
        f=events[prio].front();
        events[prio].pop_front();
    }
    f();
}

} //namespace miosix
