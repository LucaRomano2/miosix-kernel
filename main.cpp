#include <miosix.h>
#include <iostream>
using namespace miosix;
int main()
{
    for(;;)
    {
        std::cout<<2<<"\n";
        ledOn();
        Thread::sleep(1000);
        ledOff();
        Thread::sleep(1000);
    }
}
