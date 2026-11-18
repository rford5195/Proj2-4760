#ifndef RESOURCE_H
#define RESOURCE_H

#include <sys/types.h>

const int MAX_PCB = 18;
const int RESOURCE_TYPES = 5;
const int RESOURCE_INSTANCES = 5;

const long OSS_MESSAGE_TYPE = 1;
const long long BILLION = 1000000000LL;

enum Command {
    DISPATCH = 1,
    GRANT = 2,
    TERMINATE = 3
};

struct Message {
    long mtype;
    pid_t pid;
    int command;
    int resourceAction;
};

struct SharedClock {
    unsigned int seconds,
        nanoseconds;
};

struct Resource {
    int total,
        available,
        allocated[MAX_PCB];
};

struct PCB {
    int occupied;
    pid_t pid;
    int localPid;

    unsigned int startSeconds,
        startNano;

    long long lifetimeNano;

    int blocked,
        requestedResource,
        resourcesAllocated[RESOURCE_TYPES],
        inReadyQueue;
};

#endif
