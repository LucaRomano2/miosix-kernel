#pragma once

#include <list>
#include <functional>
#include <miosix.h>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <future>
#include <unordered_set>
#include <atomic>
#include "config/miosix_settings.h"

using namespace miosix;

namespace lazy {

class Task
{
public:
    virtual void execute() = 0;
    virtual ~Task() = default;
};

template<typename T>
class TaskSchedule : public Task
{
public:
    void execute() override
    {
        Lock<KernelMutex> l(m);
        return_value = task();
        is_finished = true;
        cv.signal();
    }

    T get()
    {
        Lock<KernelMutex> l(m);
        while(!is_finished) cv.wait(l);
        return return_value;
    }

    TaskSchedule(std::function<T()> task) : task(std::move(task)), is_finished(false) {}

private:
    std::function<T()> task;
    T return_value;
    KernelMutex m;
    ConditionVariable cv;
    bool is_finished;
};

class TaskRestart : public Task
{
public:
    void execute() override
    {
        thread->wakeup();
    }

    TaskRestart(Thread* t) : thread(t) {}
private:
    Thread* thread;
};

template<typename T>
class future
{
public:
    T get()
    {
        return_value = task->get();
        return return_value;
    }

    future(TaskSchedule<T>* task)
    {
        this->task = task;
    }

    future(future&& other) noexcept
    {
        this->task = other.task;
        other.task = nullptr;    
    }

    future& operator=(future&& other) noexcept
    {
        if(this != &other)
        {
            task = other.task;
            other.task = nullptr;
        }
        return *this;
    }

    ~future()
    {
        if(task) delete task;
    }

private:
    TaskSchedule<T>* task;
    T return_value;
};

class PriorityEventQueue
{
public:

    void post(Task* task, Priority priority);

    void post(Task* task);

    void run(int prio);

    void startRun();

    static PriorityEventQueue& instance();

    PriorityEventQueue(const PriorityEventQueue&) = delete;
    PriorityEventQueue& operator= (const PriorityEventQueue&) = delete;

private:
    std::list<Task*> events[NUM_PRIORITIES]; ///< Event queue
    mutable KernelMutex m[NUM_PRIORITIES]; ///< Mutex for synchronisation
    ConditionVariable cv[NUM_PRIORITIES]; ///< Condition variable for synchronisation
    std::unordered_set<Thread*> scheduler_threads, to_terminate;
    int num_core=2;
    std::atomic<int> in_execution[2];

    PriorityEventQueue() {
        startRun();
    }
};

template<class F, class... Args>
future<typename std::result_of<F(Args...)>::type> async(F&& f, Args&&... args)
{
    typedef typename std::result_of<F(Args...)>::type return_type;
    std::function<return_type()> t = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    TaskSchedule<return_type>* task_schedule = new TaskSchedule<return_type>(std::move(t));
    Task* task = task_schedule;
    future<return_type> fut = future<return_type>(task_schedule);
    PriorityEventQueue::instance().post(task);
    return fut;
}

inline void start_async()
{
    PriorityEventQueue::instance();
}

}