#pragma once

#include <list>
#include <functional>
#include <miosix.h>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <future>
#include <unordered_set>
#include "config/miosix_settings.h"

using namespace miosix;

namespace threadpool {

// Lock safe unordered set
template<typename T>
class UnorderedSet{
private:
    std::unordered_set<T> s;
    mutable KernelMutex m;

public:
    void insert(const T& val){
        Lock<KernelMutex> l(m);
        s.insert(val);
    }

    bool count(const T& val) const {
        Lock<KernelMutex> l(m);
        return s.count(val);
    }
    
    void erase(const T& val){
		Lock<KernelMutex> l(m);
		s.erase(val);
	}
};

class Task
{
public:
    virtual void execute() = 0;
    virtual bool can_return() = 0;
    virtual ~Task() {}
};

class PriorityEventQueue
{
public:

    void post(Task* task, Priority priority);

    void post(Task* task);

    void child_run(int prio);

    void start_child_run();

    void run(int prio);

    void startRun();

    bool is_scheduler();

    void decrease_in_execution()
    {
		in_execution[Thread::getCurrentThread()->getPriority().get()]--;
	}
    
    void increase_in_execution()
    {
		in_execution[Thread::getCurrentThread()->getPriority().get()]++;
	}

    static PriorityEventQueue& instance();

    PriorityEventQueue(const PriorityEventQueue&) = delete;
    PriorityEventQueue& operator= (const PriorityEventQueue&) = delete;

private:
    std::list<Task*> events[NUM_PRIORITIES]; ///< Event queue
    mutable KernelMutex m[NUM_PRIORITIES]; ///< Mutex for synchronisation
    ConditionVariable cv[NUM_PRIORITIES]; ///< Condition variable for synchronisation
    UnorderedSet<Thread*> scheduler_threads; 
    int num_core=2;
    std::atomic<int> in_execution[NUM_PRIORITIES];

    PriorityEventQueue() {}
};

template<typename T>
class Task_ : public Task
{
public:
    void execute() override
    {
        T temp = task();
        Lock<KernelMutex> l(m);
        return_value = temp;
        finished = true;
        cv.broadcast();
    }

    T get()
    {
        Lock<KernelMutex> l(m);
        while(!finished) 
        {
            if(PriorityEventQueue::instance().is_scheduler())
            {
                PriorityEventQueue::instance().start_child_run();
				PriorityEventQueue::instance().decrease_in_execution();
				cv.wait(l);
				PriorityEventQueue::instance().increase_in_execution();
			}else{
				cv.wait(l);
            }
        }
        return return_value;
    }

    bool can_return() override
    {
		return ret;
	}
	
	void cannot_return()
	{
		ret = false;
	}

    bool is_finished()
    {
        Lock<KernelMutex> l(m);
        return finished;
    }

    Task_(std::function<T()> task) : task(std::move(task)), finished(false), ret(true) {}

private:
    std::function<T()> task;
    T return_value;
    KernelMutex m;
    ConditionVariable cv;
    bool finished;
    std::atomic<bool> ret;
};

template<typename T>
class future
{
public:
    T get()
    {
        return task->get();
    }

    future(Task_<T>* task)
    {
        this->task = task;
    }

    future(future&& other) noexcept
    {
        this->task = other.task;
        other.task = nullptr;    
    }

    future& operator=(future&& other)=delete;
    future(future& other)=delete;
    future& operator=(future& other)=delete;

    ~future()
    {
        if(task){
			if(task->is_finished()) delete task;
			task->cannot_return();
		}
    }

private:
    Task_<T>* task;
};

template<class F, class... Args>
future<typename std::result_of<F(Args...)>::type> async(F&& f, Args&&... args)
{
    typedef typename std::result_of<F(Args...)>::type return_type;
    std::function<return_type()> t = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    Task_<return_type>* task_ = new Task_<return_type>(t);
    Task* task = task_;
    PriorityEventQueue::instance().post(task);
    return future<return_type>(task_);
}

inline void start_async()
{
    PriorityEventQueue::instance().startRun();
}

}