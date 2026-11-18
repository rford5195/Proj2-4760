#include <iostream>
#include <iomanip>
#include <string>
#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <signal.h>
#include <sys/msg.h>
#include <cstdio>
#include <cstdarg>
#include <random>
#include <ctime>
#include <deque>
#include <algorithm>

#include "resource.h"

using namespace std;

const long long TABLE_INTERVAL = 500000000LL,
    DEADLOCK_INTERVAL = 1000000000LL,
    CLOCK_INCREMENT = 10000000LL;

int shmId = -1,
    msgId = -1;

SharedClock* systemClock = nullptr;

PCB processTable[MAX_PCB]{};
Resource resourceTable[RESOURCE_TYPES]{};

deque<int> readyQueue;

FILE* logFile = nullptr;
int logLines = 0;
bool logLimitReached = false;

mt19937_64 randomEngine;

volatile sig_atomic_t terminateRequested = 0,
    stopLaunching = 0;

int totalLaunched = 0,
    totalFinished = 0,
    nextLocalPid = 0,
    totalRequests = 0,
    immediateGrants = 0,
    grantedSinceTable = 0,
    deadlockChecks = 0,
    deadlockTerminations = 0,
    maxProcesses = 5,
    simultaneousProcesses = 3;

double timeLimit = 2.5,
    launchInterval = 0.0;

long long maxLifetimeNano = 2500000000LL,
    launchIntervalNano = 0,
    nextLaunchTime = 0,
    nextTablePrint = TABLE_INTERVAL,
    nextDeadlockCheck = DEADLOCK_INTERVAL;

void printProcessTable();
void grantWaitingProcesses();
void releaseProcess(int index, bool notify);

void signalHandler(int signalNumber)
{
    if (signalNumber == SIGINT || signalNumber == SIGTERM)
        terminateRequested = 1;

    if (signalNumber == SIGALRM)
        stopLaunching = 1;
}

long long clockToNano()
{
    return static_cast<long long>(systemClock->seconds) * BILLION + systemClock->nanoseconds;
}

void advanceClock(long long amount)
{
    if (amount <= 0)
        return;

    long long total = clockToNano() + amount;

    systemClock->seconds = static_cast<unsigned int>(total / BILLION);
    systemClock->nanoseconds = static_cast<unsigned int>(total % BILLION);
}

void logMessage(const char* format, ...)
{
    va_list args;
    va_start(args, format);
    va_list screenArgs;
    va_copy(screenArgs, args);

    vfprintf(stdout, format, screenArgs);
    fflush(stdout);

    va_end(screenArgs);

    if (logFile != nullptr && !logLimitReached) {

        char buffer[8192];

        va_list fileArgs;
        va_copy(fileArgs, args);

        int count = vsnprintf(buffer, sizeof(buffer), format, fileArgs);

        va_end(fileArgs);

        if (count > 0) {
            int length = min(count, static_cast<int>(sizeof(buffer)) - 1),
                allowed = 0;

            for (int i = 0; i < length; i++) {
                if (buffer[i] == '\n') {
                    if (logLines >= 10000) {
                        logLimitReached = true;
                        break;
                    }

                    logLines++;
                }

                allowed++;
            }

            if (allowed > 0) {
                fwrite(buffer, 1, allowed, logFile);
                fflush(logFile);
            }

            if (logLines >= 10000)
                logLimitReached = true;
        }
    }

    va_end(args);
}

int activeChildren()
{
    int count = 0;

    for (int i = 0; i < MAX_PCB; i++) {
        if (processTable[i].occupied)
            count++;
    }

    return count;
}

int findFreePCB()
{
    for (int i = 0; i < MAX_PCB; i++) {
        if (!processTable[i].occupied)
            return i;
    }

    return -1;
}

void removeFromReadyQueue(int index)
{
    readyQueue.erase(remove(readyQueue.begin(), readyQueue.end(), index), readyQueue.end());
    processTable[index].inReadyQueue = 0;
}

void enqueueProcess(int index)
{
    PCB& pcb = processTable[index];

    if (!pcb.occupied || pcb.blocked || pcb.inReadyQueue)
        return;

    readyQueue.push_back(index);
    pcb.inReadyQueue = 1;
}

int selectProcess()
{
    while (!readyQueue.empty()) {

        int index = readyQueue.front();
        readyQueue.pop_front();

        if (index < 0 || index >= MAX_PCB)
            continue;

        PCB& pcb = processTable[index];

        pcb.inReadyQueue = 0;

        if (!pcb.occupied || pcb.blocked)
            continue;

        return index;
    }

    return -1;
}

bool sendCommand(int index, int command, int resource = -1)
{
    if (index < 0 || index >= MAX_PCB)
        return false;

    if (!processTable[index].occupied)
        return false;

    Message outgoing{};

    outgoing.mtype = processTable[index].pid;
    outgoing.pid = processTable[index].pid;
    outgoing.command = command;
    outgoing.resourceAction = resource;

    if (msgsnd(msgId, &outgoing, sizeof(Message) - sizeof(long), 0) == -1)
    {
        if (errno != EIDRM && errno != EINVAL)
            perror("OSS msgsnd");

        return false;
    }

    return true;
}

// Returns:  1 = message received
//           0 = abort (termination requested or queue removed / error)
//          -1 = the worker exited without sending a message (not yet reaped)
int receiveAction(Message& incoming, pid_t workerPid)
{
    while (true) {

        if (msgrcv(msgId, &incoming, sizeof(Message) - sizeof(long), OSS_MESSAGE_TYPE, IPC_NOWAIT) != -1)
            return 1;

        if (errno == EINTR) {
            if (terminateRequested)
                return 0;

            continue;
        }

        if (errno == EIDRM || errno == EINVAL)
            return 0;

        if (errno != ENOMSG) {
            perror("OSS msgrcv");
            return 0;
        }

        if (terminateRequested)
            return 0;

        // No message yet: peek (without reaping) to see whether the worker has already exited.
        siginfo_t info{};

        if (waitid(P_PID, static_cast<id_t>(workerPid), &info, WEXITED | WNOHANG | WNOWAIT) == 0 && info.si_pid != 0) {

            // The worker may have sent its message right before exiting, so check once more.
            if (msgrcv(msgId, &incoming, sizeof(Message) - sizeof(long), OSS_MESSAGE_TYPE, IPC_NOWAIT) != -1)
                return 1;

            return -1;
        }

        usleep(100);
    }
}

void initializeResources()
{
    for (int i = 0; i < RESOURCE_TYPES; i++) {
        resourceTable[i].total = RESOURCE_INSTANCES;
        resourceTable[i].available = RESOURCE_INSTANCES;

        for (int j = 0; j < MAX_PCB; j++)
            resourceTable[i].allocated[j] = 0;
    }
}

void removeProcessResources(int index)
{
    PCB& pcb = processTable[index];
    string released;

    for (int r = 0; r < RESOURCE_TYPES; r++)
    {
        int amount = pcb.resourcesAllocated[r];

        if (amount > 0)
        {
            resourceTable[r].available += amount;
            resourceTable[r].allocated[index] = 0;
            pcb.resourcesAllocated[r] = 0;

            if (!released.empty())
                released += ", ";

            released += "R" + to_string(r) + ":" + to_string(amount);
        }
    }

    logMessage("Resources released are as follows: %s\n", released.empty() ? "None" : released.c_str());
}

void grantWaitingProcesses()
{
    for (int i = 0; i < MAX_PCB; i++)
    {
        PCB& pcb = processTable[i];

        if (!pcb.occupied || !pcb.blocked)
            continue;

        int resource = pcb.requestedResource;

        if (resource < 0 || resource >= RESOURCE_TYPES)
            continue;

        if (resourceTable[resource].available <= 0)
            continue;

        resourceTable[resource].available--;
        resourceTable[resource].allocated[i]++;
        pcb.resourcesAllocated[resource]++;
        pcb.blocked = 0;
        pcb.requestedResource = -1;

        logMessage("OSS: Granting previously blocked request R%d to P%d at time %u:%u\n", resource, pcb.localPid, systemClock->seconds, systemClock->nanoseconds);

        if (!sendCommand(i, GRANT, resource))
        {
            logMessage("OSS: Could not notify P%d of its grant.\n", pcb.localPid);
            resourceTable[resource].available++;
            resourceTable[resource].allocated[i]--;
            pcb.resourcesAllocated[resource]--;
            pcb.blocked = 1;
            pcb.requestedResource = resource;
            continue;
        }

        grantedSinceTable++;

        if (grantedSinceTable >= 20)
        {
            printProcessTable();
            grantedSinceTable = 0;
        }

        enqueueProcess(i);
    }
}

void releaseProcess(int index, bool notify)
{
    if (index < 0 || index >= MAX_PCB)
        return;

    if (!processTable[index].occupied)
        return;

    PCB& pcb = processTable[index];

    pid_t pid = pcb.pid;
    int localPid = pcb.localPid;

    removeFromReadyQueue(index);

    if (notify)
    {
        if (!sendCommand(index, TERMINATE))
            kill(pid, SIGTERM);
    }

    removeProcessResources(index);

    pcb.blocked = 0;
    pcb.requestedResource = -1;

    int status;

    while (waitpid(pid, &status, 0) == -1)
    {
        if (errno == EINTR)
            continue;

        if (errno != ECHILD)
            perror("OSS waitpid");

        break;
    }

    logMessage("OSS: Process P%d (PID %d) has terminated.\n", localPid, static_cast<int>(pid));

    memset(&pcb, 0, sizeof(PCB));

    totalFinished++;

    grantWaitingProcesses();
}

bool launchWorker()
{
    int index = findFreePCB();

    if (index == -1)
        return false;

    long long lifetime = 0;

    if (maxLifetimeNano > 0)
    {
        uniform_int_distribution<long long> distribution(1, maxLifetimeNano);
        lifetime = distribution(randomEngine);
    }

    unsigned int startSeconds = systemClock->seconds,
        startNano = systemClock->nanoseconds;

    pid_t pid = fork();

    if (pid == -1) {
        perror("fork");
        return false;
    }

    if (pid == 0) {

        string lifetimeString = to_string(lifetime),
            secondsString = to_string(startSeconds),
            nanoString = to_string(startNano);

        execl("./worker", "./worker", lifetimeString.c_str(), secondsString.c_str(), nanoString.c_str(), nullptr);
        perror("execl");
        _exit(EXIT_FAILURE);
    }

    PCB& pcb = processTable[index];

    memset(&pcb, 0, sizeof(PCB));

    pcb.occupied = 1;
    pcb.pid = pid;
    pcb.localPid = nextLocalPid++;
    pcb.startSeconds = startSeconds;
    pcb.startNano = startNano;
    pcb.lifetimeNano = lifetime;
    pcb.blocked = 0;
    pcb.requestedResource = -1;
    pcb.inReadyQueue = 0;

    enqueueProcess(index);

    totalLaunched++;

    logMessage("OSS: Generating P%d with PID %d, lifetime %lld ns at time %u:%u\n", pcb.localPid, static_cast<int>(pid), lifetime, startSeconds, startNano);
    return true;
}

void processRequest(int index, int resource)
{
    PCB& pcb = processTable[index];

    totalRequests++;

    logMessage("OSS has detected Process P%d requesting R%d at time %u:%u\n", pcb.localPid, resource, systemClock->seconds, systemClock->nanoseconds);

    if (resourceTable[resource].available > 0)
    {
        resourceTable[resource].available--;
        resourceTable[resource].allocated[index]++;
        pcb.resourcesAllocated[resource]++;

        logMessage("OSS granting P%d request R%d at time %u:%u\n", pcb.localPid, resource, systemClock->seconds, systemClock->nanoseconds);

        if (!sendCommand(index, GRANT, resource))
        {
            logMessage("OSS: Could not notify P%d of its grant.\n", pcb.localPid);

            // Undo the grant; leave the process blocked on this resource.
            resourceTable[resource].available++;
            resourceTable[resource].allocated[index]--;
            pcb.resourcesAllocated[resource]--;
            pcb.blocked = 1;
            pcb.requestedResource = resource;
            return;
        }

        immediateGrants++;
        enqueueProcess(index);

        grantedSinceTable++;

        if (grantedSinceTable >= 20)
        {
            printProcessTable();
            grantedSinceTable = 0;
        }
    }
    else {

        pcb.blocked = 1;
        pcb.requestedResource = resource;

        logMessage("OSS: P%d cannot obtain R%d and is now blocked at time %u:%u\n", pcb.localPid, resource, systemClock->seconds, systemClock->nanoseconds);
    }
}

void processRelease(int index, int resource)
{
    PCB& pcb = processTable[index];

    if (pcb.resourcesAllocated[resource] <= 0)
    {
        logMessage("OSS: P%d attempted to release unallocated R%d.\n", pcb.localPid, resource);
        enqueueProcess(index);
        return;
    }

    pcb.resourcesAllocated[resource]--;
    resourceTable[resource].allocated[index]--;
    resourceTable[resource].available++;

    logMessage("OSS has acknowledged Process P%d releasing R%d at time %u:%u\n", pcb.localPid, resource, systemClock->seconds, systemClock->nanoseconds);
    enqueueProcess(index);
    grantWaitingProcesses();
}

void finishProcess(int index)
{
    PCB& pcb = processTable[index];

    pid_t pid = pcb.pid;

    removeFromReadyQueue(index);
    removeProcessResources(index);

    while (waitpid(pid, nullptr, 0) == -1)
    {
        if (errno == EINTR)
            continue;

        if (errno != ECHILD)
            perror("OSS waitpid");

        break;
    }

    memset(&pcb, 0, sizeof(PCB));

    totalFinished++;
    grantWaitingProcesses();
}

void dispatchProcess(int index)
{
    PCB& pcb = processTable[index];

    if (!pcb.occupied || pcb.blocked)
        return;

    if (!sendCommand(index, DISPATCH))
        return;

    Message incoming{};

    int result = receiveAction(incoming, pcb.pid);

    if (result == 0)
        return;

    if (result == -1) {

        logMessage("OSS: P%d exited unexpectedly. Cleaning up.\n", pcb.localPid);
        finishProcess(index);
        return;
    }

    if (incoming.pid != pcb.pid) {

        logMessage("OSS: Unexpected response. Expected PID %d, received PID %d.\n", static_cast<int>(pcb.pid), static_cast<int>(incoming.pid));
        enqueueProcess(index);
        return;
    }

    if (incoming.resourceAction == 0)
    {
        logMessage("OSS: P%d has finished and is terminating.\n", pcb.localPid);
        finishProcess(index);
        return;
    }

    if (incoming.resourceAction > 0 && incoming.resourceAction <= RESOURCE_TYPES)
    {
        int resource = incoming.resourceAction - 1;
        processRequest(index, resource);
        return;
    }

    if (incoming.resourceAction < 0 && incoming.resourceAction >= -RESOURCE_TYPES)
    {
        int resource = -incoming.resourceAction - 1;
        processRelease(index, resource);
        return;
    }

    logMessage("OSS: Invalid resource action from P%d.\n", pcb.localPid);
    enqueueProcess(index);
}

void printProcessTable()
{
    logMessage("\n");
    logMessage("============================================================\n");
    logMessage("OSS Process Table at %u:%u\n", systemClock->seconds, systemClock->nanoseconds);
    logMessage("============================================================\n");
    logMessage("%-8s %-10s %-10s %-10s %-12s\n", "Entry", "PID", "LocalPID", "Blocked", "Requested");

    for (int i = 0; i < MAX_PCB; i++)
    {
        PCB& pcb = processTable[i];

        if (!pcb.occupied)
            continue;

        string requested = "-";

        if (pcb.blocked)
            requested = "R" + to_string(pcb.requestedResource);

        logMessage("%-8d %-10d %-10d %-10d %-12s\n", i, static_cast<int>(pcb.pid), pcb.localPid, pcb.blocked, requested.c_str());
    }

    logMessage("\nBlocked Processes:\n");
    bool foundBlocked = false;

    for (int i = 0; i < MAX_PCB; i++) {

        PCB& pcb = processTable[i];

        if (pcb.occupied && pcb.blocked)
        {
            logMessage("P%d: Waiting on R%d\n", pcb.localPid, pcb.requestedResource);
            foundBlocked = true;
        }
    }

    if (!foundBlocked)
        logMessage("None\n");

    logMessage("\nCurrent System Resources:\n");
    logMessage("%-12s %-10s %-12s\n", "Resource", "Total", "Available");

    for (int r = 0; r < RESOURCE_TYPES; r++)
        logMessage("R%-11d %-10d %-12d\n", r, resourceTable[r].total, resourceTable[r].available);

    logMessage("\nResources Allocated:\n");
    logMessage("%-10s", "Process");

    for (int r = 0; r < RESOURCE_TYPES; r++)
    {
        string resourceName = "R" + to_string(r);
        logMessage("%-7s", resourceName.c_str());
    }

    logMessage("\n");

    for (int i = 0; i < MAX_PCB; i++)
    {
        PCB& pcb = processTable[i];

        if (!pcb.occupied)
            continue;

        string processName = "P" + to_string(pcb.localPid);
        logMessage("%-10s", processName.c_str());

        for (int r = 0; r < RESOURCE_TYPES; r++)
            logMessage("%-7d", pcb.resourcesAllocated[r]);

        logMessage("\n");
    }

    logMessage("============================================================\n\n");
}

// Returns true if a deadlock was found and a victim was terminated.
bool detectDeadlock()
{
    deadlockChecks++;

    logMessage("\nOSS running deadlock detection at time %u:%u\n", systemClock->seconds, systemClock->nanoseconds);

    int work[RESOURCE_TYPES];
    bool finish[MAX_PCB] = {false};

    for (int r = 0; r < RESOURCE_TYPES; r++)
        work[r] = resourceTable[r].available;

    for (int i = 0; i < MAX_PCB; i++)
    {
        if (!processTable[i].occupied)
        {
            finish[i] = true;
            continue;
        }

        int allocatedCount = 0;

        for (int r = 0; r < RESOURCE_TYPES; r++)
            allocatedCount += processTable[i].resourcesAllocated[r];

        finish[i] = (allocatedCount == 0);
    }

    bool progress = true;

    while (progress)
    {
        progress = false;

        for (int i = 0; i < MAX_PCB; i++)
        {
            PCB& pcb = processTable[i];

            if (!pcb.occupied || finish[i])
                continue;

            int request = pcb.blocked ? pcb.requestedResource : -1;
            bool canFinish = false;

            if (request == -1)
                canFinish = true;
            else if (request >= 0 && request < RESOURCE_TYPES)
            {
                if (work[request] >= 1)
                    canFinish = true;
            }

            if (!canFinish)
                continue;

            for (int r = 0; r < RESOURCE_TYPES; r++)
                work[r] += pcb.resourcesAllocated[r];

            finish[i] = true;
            progress = true;
        }
    }

    int victim = -1;
    string involved;

    for (int i = 0; i < MAX_PCB; i++)
    {
        if (!processTable[i].occupied || finish[i])
            continue;

        if (!involved.empty())
            involved += ", ";

        involved += "P" + to_string(processTable[i].localPid);

        if (victim == -1 || processTable[i].localPid < processTable[victim].localPid)
            victim = i;
    }

    if (victim == -1)
    {
        logMessage("No deadlock detected.\n\n");
        return false;
    }

    logMessage("Processes %s deadlocked\n", involved.c_str());
    logMessage("Attempting to resolve deadlock...\n");
    logMessage("Killing process P%d:\n", processTable[victim].localPid);

    deadlockTerminations++;
    releaseProcess(victim, true);

    return true;
}

void updatePeriodicEvents()
{
    long long currentTime = clockToNano();

    if (currentTime >= nextTablePrint)
    {
        printProcessTable();

        while (nextTablePrint <= currentTime)
            nextTablePrint += TABLE_INTERVAL;
    }

    if (currentTime >= nextDeadlockCheck)
    {
        while (detectDeadlock()) { }

        currentTime = clockToNano();

        while (nextDeadlockCheck <= currentTime)
            nextDeadlockCheck += DEADLOCK_INTERVAL;
    }
}

void killChildren()
{
    for (int i = 0; i < MAX_PCB; i++)
    {
        if (processTable[i].occupied && processTable[i].pid > 0)
            kill(processTable[i].pid, SIGTERM);
    }

    for (int i = 0; i < MAX_PCB; i++)
    {
        if (!processTable[i].occupied)
            continue;

        pid_t pid = processTable[i].pid;

        while (waitpid(pid, nullptr, 0) == -1)
        {
            if (errno == EINTR)
                continue;

            break;
        }

        memset(&processTable[i], 0, sizeof(PCB));
    }

    readyQueue.clear();
}

void cleanup()
{
    if (systemClock != nullptr)
    {
        shmdt(systemClock);
        systemClock = nullptr;
    }

    if (shmId != -1)
    {
        shmctl(shmId, IPC_RMID, nullptr);
        shmId = -1;
    }

    if (msgId != -1)
    {
        msgctl(msgId, IPC_RMID, nullptr);
        msgId = -1;
    }

    if (logFile != nullptr)
    {
        fclose(logFile);
        logFile = nullptr;
    }
}

void printFinalReport()
{
    logMessage("============================================\n");
    logMessage("             OSS FINAL REPORT\n");
    logMessage("============================================\n");

    logMessage("Total processes launched: %d\n", totalLaunched);
    logMessage("Total processes finished: %d\n", totalFinished);
    logMessage("Total resource requests: %d\n", totalRequests);
    logMessage("Immediate resource grants: %d\n", immediateGrants);
    logMessage("Deadlock detection runs: %d\n", deadlockChecks);
    logMessage("Processes terminated to resolve deadlock: %d\n", deadlockTerminations);

    double grantPercentage = 0.0;

    if (totalRequests > 0)
        grantPercentage = (static_cast<double>(immediateGrants) / totalRequests) * 100.0;

    logMessage("Percentage of requests granted immediately: %.2f%%\n", grantPercentage);
    logMessage("Percentage of requests not granted immediately: %.2f%%\n", totalRequests > 0 ? 100.0 - grantPercentage : 0.0);
    logMessage("Final simulated time: %u:%u\n", systemClock->seconds, systemClock->nanoseconds);

    logMessage("============================================\n");
}

bool parseArguments(int argc, char* argv[])
{
    string logFileName = "oss.log";

    int option;

    while ((option = getopt(argc, argv, "hn:s:t:i:f:")) != -1)
    {

        switch (option) {

            case 'h':
                cout << "Usage: ./oss [-h] [-n proc] [-s simul] [-t timeLimit] [-i launchInterval] [-f logfile]\n";
                cout << "-n Maximum number of processes\n";
                cout << "-s Maximum simultaneous processes\n";
                cout << "-t Maximum worker lifetime in seconds\n";
                cout << "-i Process launch interval in seconds\n";
                cout << "-f Log file name\n";

                return false;

            case 'n':
                maxProcesses = atoi(optarg);
                break;

            case 's':
                simultaneousProcesses = atoi(optarg);
                break;

            case 't':
                timeLimit = atof(optarg);
                break;

            case 'i':
                launchInterval = atof(optarg);
                break;

            case 'f':
                logFileName = optarg;
                break;

            default:
                cerr << "Invalid arguments. Use ./oss -h\n";
                return false;
        }
    }

    if (maxProcesses <= 0 || simultaneousProcesses <= 0 || timeLimit < 0 || launchInterval < 0)
    {
        cerr << "Invalid argument values.\n";
        return false;
    }

    if (simultaneousProcesses > MAX_PCB)
        simultaneousProcesses = MAX_PCB;

    if (timeLimit > 0)
        maxLifetimeNano = static_cast<long long>(timeLimit * BILLION);
    else
        maxLifetimeNano = 0;

    launchIntervalNano = static_cast<long long>(launchInterval * BILLION);
    logFile = fopen(logFileName.c_str(), "w");

    if (logFile == nullptr)
    {
        perror("fopen");
        return false;
    }

    return true;
}

bool initializeSignals()
{
    struct sigaction sa{};

    sa.sa_handler = signalHandler;

    sigemptyset(&sa.sa_mask);

    sa.sa_flags = 0;

    if (sigaction(SIGINT, &sa, nullptr) == -1)
        return false;

    if (sigaction(SIGTERM, &sa, nullptr) == -1)
        return false;

    if (sigaction(SIGALRM, &sa, nullptr) == -1)
        return false;

    alarm(5);

    return true;
}

bool initializeIPC()
{
    msgId = msgget(IPC_PRIVATE, IPC_CREAT | 0600);

    if (msgId == -1)
    {
        perror("msgget");
        return false;
    }

    shmId = shmget(IPC_PRIVATE, sizeof(SharedClock), IPC_CREAT | 0600);

    if (shmId == -1)
    {
        perror("shmget");
        return false;
    }

    systemClock = static_cast<SharedClock*>(shmat(shmId, nullptr, 0));

    if (systemClock == reinterpret_cast<SharedClock*>(-1))
    {
        perror("shmat");
        systemClock = nullptr;
        return false;
    }

    systemClock->seconds = 0;
    systemClock->nanoseconds = 0;

    string messageString = to_string(msgId),
        sharedString = to_string(shmId);

    setenv("OSS_MSG_ID", messageString.c_str(), 1);
    setenv("OSS_SHM_ID", sharedString.c_str(), 1);

    return true;
}

int main(int argc, char* argv[])
{
    if (!parseArguments(argc, argv))
    {
        cleanup();
        return 0;
    }

    randomEngine.seed(static_cast<unsigned long long>(time(nullptr)) ^ static_cast<unsigned long long>(getpid()));

    memset(processTable, 0, sizeof(processTable));

    initializeResources();

    if (!initializeSignals())
    {
        perror("sigaction");
        cleanup();
        return 1;
    }

    if (!initializeIPC())
    {
        cleanup();
        return 1;
    }

    logMessage("\nOSS starting.\n");
    logMessage("OSS PID: %d\n", static_cast<int>(getpid()));
    logMessage("Maximum processes: %d\n", maxProcesses);
    logMessage("Maximum simultaneous processes: %d\n", simultaneousProcesses);
    logMessage("Maximum worker lifetime: %.2f seconds\n", timeLimit);
    logMessage("Launch interval: %.2f seconds\n", launchInterval);
    logMessage("Resource classes: %d\n", RESOURCE_TYPES);
    logMessage("Instances per resource: %d\n\n", RESOURCE_INSTANCES);

    nextLaunchTime = 0;

    while (!terminateRequested && (totalLaunched < maxProcesses || activeChildren() > 0))
    {
        long long currentTime;

        if (stopLaunching)
            maxProcesses = totalLaunched;

        grantWaitingProcesses();
        currentTime = clockToNano();

        if (!stopLaunching && totalLaunched < maxProcesses && activeChildren() < simultaneousProcesses && activeChildren() < MAX_PCB && currentTime >= nextLaunchTime)
        {
            if (launchWorker())
                nextLaunchTime = clockToNano() + launchIntervalNano;
        }

        updatePeriodicEvents();

        if (terminateRequested)
            break;

        int selected = selectProcess();

        if (selected != -1)
            dispatchProcess(selected);

        advanceClock(CLOCK_INCREMENT);

        updatePeriodicEvents();
    }

    if (terminateRequested)
    {
        logMessage("\nOSS: Termination signal received. Cleaning up workers.\n");
        killChildren();
    }

    printFinalReport();
    cleanup();

    return 0;
}
