
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
#include <cmath>
#include <deque>
#include <algorithm>

using namespace std;

const int MAX_PCB = 18;
const long long BILLION = 1000000000LL,
    TABLE_INTERVAL_NS = 500000000LL,
    SCHEDULING_COST = 1000LL,
    IO_BLOCK_TIME = 200000000LL;

const long long QUANTA[3] = {25000000LL, 50000000LL, 100000000LL};

enum {
    TERMINATED = 0,
    QUANTUM_USED = 1,
    IO_BLOCKED = 2
};

struct SharedClock {
    unsigned int seconds,
        nanoseconds;
};

struct PCB {
    int occupied;
    pid_t pid;
    int localPid;

    int startSeconds,
        startNano;

    long long serviceTime;

    int eventWaitSec,
        eventWaitNano,
        blocked,
        queueLevel;
};

struct Message {
    long mtype;
    pid_t pid;
    long long quantum,
        used;
    int status;
};

int shmId = -1,
    msgId = -1;

SharedClock* systemClock = nullptr;

PCB processTable[MAX_PCB];

deque<int> readyQueues[3];
FILE* logFile = nullptr;
mt19937_64 randomEngine;
volatile sig_atomic_t terminateRequested = 0;

int totalLaunched = 0,
    totalFinished = 0,
    nextLocalPid = 1;

int logLines = 0;
bool logLimitReached = false;

long long totalCpuTime = 0,
    totalNonCpuTime = 0,
    totalIoBlocks = 0,
    nextLaunchTime = 0,
    nextTablePrint = TABLE_INTERVAL_NS,
    maxLifetimeNano = 2500000000LL,
    launchIntervalNano = 0;

int maxProcesses = 5,
    simultaneousProcesses = 3;

void signalHandler(int signalNumber)
{
    if (signalNumber == SIGINT || signalNumber == SIGALRM)
        terminateRequested = 1;
}

long long clockToNano()
{
    return static_cast<long long>(systemClock->seconds) * BILLION + systemClock->nanoseconds;
}

void normalizeClock()
{
    while (systemClock->nanoseconds >= BILLION) {
        systemClock->seconds++;
        systemClock->nanoseconds -= BILLION;
    }
}

void advanceClock(long long amount, bool cpuTime)
{
    if (amount <= 0)
        return;

    systemClock->nanoseconds += static_cast<unsigned int>(amount);
    normalizeClock();

    if (cpuTime)
        totalCpuTime += amount;
    else
        totalNonCpuTime += amount;
}

void addTime(long long amount, int& seconds, int& nano)
{
    long long result = static_cast<long long>(seconds) * BILLION + nano + amount;
    seconds = static_cast<int>(result / BILLION);
    nano = static_cast<int>(result % BILLION);
}

long long toTotalNano(int seconds, int nano)
{
    return static_cast<long long>(seconds) * BILLION + nano;
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
        va_list fileArgs;
        va_copy(fileArgs, args);

        char buffer[4096];

        int count = vsnprintf(buffer, sizeof(buffer), format, fileArgs);

        va_end(fileArgs);

        if (count > 0) {
            for (int i = 0; i < count && i < (int)sizeof(buffer); i++) {
                if (buffer[i] == '\n')
                    logLines++;
            }

            if (logLines <= 10000)
                fputs(buffer, logFile);
            else {
                logLimitReached = true;
                fprintf(stdout, "OSS: Log file reached 10000 lines.\n");
            }

            fflush(logFile);
        }
    }

    va_end(args);
}

int findFreePCB()
{
    for (int i = 0; i < MAX_PCB; i++) {
        if (!processTable[i].occupied)
            return i;
    }

    return -1;
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

void printQueues()
{
    logMessage("OSS: ");

    for (int q = 0; q < 3; q++) {
        logMessage("q%d[", q);

        for (size_t i = 0; i < readyQueues[q].size(); i++) {
            logMessage("%d", processTable[readyQueues[q][i]].localPid);

            if (i + 1 < readyQueues[q].size())
                logMessage(",");
        }

        logMessage("]");
    }

    logMessage(",blocked[");

    bool first = true;

    for (int i = 0; i < MAX_PCB; i++) {
        if (processTable[i].occupied && processTable[i].blocked) {
            if (!first)
                logMessage(",");

            logMessage("%d", processTable[i].localPid);
            first = false;
        }
    }

    logMessage("]\n");
}

void printProcessTable()
{
    logMessage("\nOSS PID:%d SysClockS:%u SysClockNano:%u\n", static_cast<int>(getpid()), systemClock->seconds, systemClock->nanoseconds);
    logMessage("%-6s %-9s %-9s %-9s %-12s %-12s %-12s %-9s\n", "Entry", "Occupied", "PID", "LocalPID", "StartS", "StartN", "ServiceNS", "Queue");

    for (int i = 0; i < MAX_PCB; i++)
        logMessage("%-6d %-9d %-9d %-9d %-12d %-12d %-12lld %-9d\n", i, processTable[i].occupied, static_cast<int>(processTable[i].pid), processTable[i].localPid, processTable[i].startSeconds, processTable[i].startNano, processTable[i].serviceTime, processTable[i].queueLevel);

    printQueues();
    logMessage("\n");
}

void cleanup()
{
    if (systemClock != nullptr) {
        shmdt(systemClock);
        systemClock = nullptr;
    }

    if (shmId != -1) {
        shmctl(shmId, IPC_RMID, nullptr);
        shmId = -1;
    }

    if (msgId != -1) {
        msgctl(msgId, IPC_RMID, nullptr);
        msgId = -1;
    }

    if (logFile != nullptr) {
        fclose(logFile);
        logFile = nullptr;
    }
}

void killChildren()
{
    for (int i = 0; i < MAX_PCB; i++) {
        if (processTable[i].occupied && processTable[i].pid > 0)
            kill(processTable[i].pid, SIGTERM);
    }

    for (int i = 0; i < MAX_PCB; i++) {
        if (processTable[i].occupied && processTable[i].pid > 0) {
            while (waitpid(processTable[i].pid, nullptr, 0) == -1) {
                if (errno == EINTR)
                    continue;

                break;
            }

            memset(&processTable[i], 0, sizeof(PCB));
        }
    }
}

void printFinalReport()
{
    long long totalTime = clockToNano();

    double utilization = 0.0;

    if (totalTime > 0)
        utilization = (static_cast<double>(totalCpuTime) / static_cast<double>(totalTime)) * 100.0;

    logMessage("\n========================================\n");
    logMessage("OSS FINAL REPORT\n");
    logMessage("========================================\n");
    logMessage("Total processes launched: %d\n", totalLaunched);
    logMessage("Total processes finished: %d\n", totalFinished);
    logMessage("Total CPU time: %lld nanoseconds\n", totalCpuTime);
    logMessage("Total non-CPU time: %lld nanoseconds\n", totalNonCpuTime);
    logMessage("Total I/O blocks: %lld\n", totalIoBlocks);
    logMessage("Total simulated time: %lld seconds %lld nanoseconds\n", totalTime / BILLION,  totalTime % BILLION);
    logMessage("CPU utilization: %.2f%%\n", utilization);
    logMessage("========================================\n");
}

bool launchWorker()
{
    int index = findFreePCB();

    if (index == -1)
        return false;

    uniform_int_distribution<long long> burstDistribution(1, maxLifetimeNano > 0 ? maxLifetimeNano : 1);

    long long burstTime = burstDistribution(randomEngine);

    int startSeconds = systemClock->seconds,
        startNano = systemClock->nanoseconds;

    pid_t pid = fork();

    if (pid < 0) {
        perror("fork");
        return false;
    }

    if (pid == 0) {
        string burstString = to_string(burstTime);

        execl("./worker", "./worker", burstString.c_str(), nullptr);
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

    pcb.serviceTime = 0;
    pcb.queueLevel = 0;
    pcb.blocked = 0;

    readyQueues[0].push_back(index);
    totalLaunched++;
    logMessage("OSS: Generating process with PID %d (simulated PID %d), burst %lld ns and putting it in q0 at time %d:%d\n", static_cast<int>(pid), pcb.localPid, burstTime, startSeconds, startNano);
    return true;
}

void checkBlockedProcesses()
{
    long long currentTime = clockToNano();

    for (int i = 0; i < MAX_PCB; i++) {
        PCB& pcb = processTable[i];

        if (!pcb.occupied || !pcb.blocked)
            continue;

        long long eventTime = toTotalNano(pcb.eventWaitSec, pcb.eventWaitNano);

        if (currentTime >= eventTime) {
            pcb.blocked = 0;
            pcb.queueLevel = 0;

            advanceClock(SCHEDULING_COST, false);
            readyQueues[0].push_back(i);
            logMessage("OSS: Process %d has unblocked and returned to q0 at time %u:%u\n", pcb.localPid, systemClock->seconds, systemClock->nanoseconds);
        }
    }
}

int selectProcess()
{
    for (int q = 0; q < 3; q++) {
        if (!readyQueues[q].empty()) {
            int index = readyQueues[q].front();
            readyQueues[q].pop_front();
            return index;
        }
    }

    return -1;
}

void updateProcessTableTiming()
{
    long long currentTime = clockToNano();

    if (currentTime >= nextTablePrint) {
        printProcessTable();

        while (nextTablePrint <= currentTime)
            nextTablePrint += TABLE_INTERVAL_NS;
    }
}

void advanceIdleClock()
{
    long long currentTime = clockToNano(),
        nextEvent = -1;

    if (totalLaunched < maxProcesses && activeChildren() < simultaneousProcesses)
        nextEvent = nextLaunchTime;

    for (int i = 0; i < MAX_PCB; i++) {
        if (processTable[i].occupied && processTable[i].blocked) {
            long long eventTime = toTotalNano(processTable[i].eventWaitSec, processTable[i].eventWaitNano);

            if (nextEvent == -1 || eventTime < nextEvent)
                nextEvent = eventTime;
        }
    }

    if (nextEvent > currentTime) {
        long long difference = nextEvent - currentTime;
        advanceClock(difference, false);
    }
    else
        advanceClock(SCHEDULING_COST, false);
}

bool dispatchProcess(int index)
{
    PCB& pcb = processTable[index];

    int queue = pcb.queueLevel;
    long long quantum = QUANTA[queue];

    advanceClock(SCHEDULING_COST, false);
    logMessage("OSS: Dispatching process PID %d from q%d at time %u:%u\n", pcb.localPid, queue, systemClock->seconds, systemClock->nanoseconds);

    Message outgoing {};

    outgoing.mtype = pcb.pid;
    outgoing.pid = pcb.pid;
    outgoing.quantum = quantum;

    if (msgsnd(msgId, &outgoing, sizeof(Message) - sizeof(long), 0) == -1) {
        if (errno != EINTR)
            perror("OSS msgsnd");

        return false;
    }

    Message incoming {};

    while (msgrcv(msgId, &incoming, sizeof(Message) - sizeof(long), 1, 0) == -1) {
        if (errno == EINTR) {
            if (terminateRequested)
                return false;

            continue;
        }

        if (errno == EIDRM || errno == EINVAL)
            return false;

        perror("OSS msgrcv");
        return false;
    }

    if (incoming.pid != pcb.pid) {
        logMessage("OSS: ERROR - Unexpected worker response.\n");
        return false;
    }

    long long used = incoming.used;

    if (used < 0)
        used = 0;

    if (used > quantum)
        used = quantum;

    advanceClock(used, true);
    pcb.serviceTime += used;
    logMessage("OSS: Receiving that process PID %d ran for %lld nanoseconds\n", pcb.localPid, used);

    if (incoming.status == TERMINATED) {
        logMessage("OSS: Process %d terminated after using %lld ns\n", pcb.localPid, pcb.serviceTime);
        int status;

        while (waitpid(pcb.pid, &status, 0) == -1) {
            if (errno == EINTR) {
                if (terminateRequested)
                    return false;

                continue;
            }

            if (errno != ECHILD)
                perror("waitpid");

            break;
        }

        memset(&pcb, 0, sizeof(PCB));
        totalFinished++;
        return true;
    }

    if (incoming.status == IO_BLOCKED) {
        pcb.blocked = 1;
        addTime(IO_BLOCK_TIME, pcb.eventWaitSec, pcb.eventWaitNano);
        totalIoBlocks++;
        logMessage("OSS: Process %d used part of its quantum and is being put into the blocked queue\n", pcb.localPid);
        return true;
    }

    if (incoming.status == QUANTUM_USED) {
        if (queue < 2) {
            pcb.queueLevel = queue + 1;
            logMessage("OSS: Process %d used its full quantum. Demoting to q%d\n", pcb.localPid, pcb.queueLevel);
        }
        else 
            logMessage("OSS: Process %d used its full quantum. Returning to q2\n", pcb.localPid);

        readyQueues[pcb.queueLevel].push_back(index);
    }

    return true;
}

int main(int argc, char* argv[])
{
    double timeLimit = 2.5,
        launchInterval = 0.0;

    string logFileName = "oss.log";

    int option;

    while ((option = getopt(argc, argv, "hn:s:t:i:f:")) != -1) {
        switch (option) {
            case 'h':
                cout << "Usage: ./oss [-h] [-n proc] [-s simul] [-t timelimit] [-i interval] [-f logfile]\n";
                return 0;

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
                return 1;
        }
    }

    if (maxProcesses <= 0 || simultaneousProcesses <= 0 || timeLimit < 0 || launchInterval < 0) {
        cerr << "Invalid argument values.\n";
        return 1;
    }

    if (simultaneousProcesses > MAX_PCB)
        simultaneousProcesses = MAX_PCB;

    maxLifetimeNano = static_cast<long long>(timeLimit * BILLION);
    launchIntervalNano = static_cast<long long>(launchInterval * BILLION);
    randomEngine.seed(static_cast<unsigned long long>(time(nullptr)) ^ static_cast<unsigned long long>(getpid()));

    logFile = fopen(logFileName.c_str(), "w");

    if (!logFile) {
        perror("fopen");
        return 1;
    }

    memset(processTable, 0, sizeof(processTable));

    struct sigaction sa {};

    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGINT, &sa, nullptr) == -1 || sigaction(SIGALRM, &sa, nullptr) == -1) {
        perror("sigaction");
        cleanup();
        return 1;
    }

    alarm(3);
    msgId = msgget(IPC_PRIVATE, IPC_CREAT | 0600);

    if (msgId == -1) {
        perror("msgget");
        cleanup();
        return 1;
    }

    shmId = shmget(IPC_PRIVATE, sizeof(SharedClock), IPC_CREAT | 0600);

    if (shmId == -1) {
        perror("shmget");
        cleanup();
        return 1;
    }

    systemClock = static_cast<SharedClock*>(shmat(shmId, nullptr, 0));

    if (systemClock == reinterpret_cast<SharedClock*>(-1)) {
        perror("shmat");
        systemClock = nullptr;
        cleanup();
        return 1;
    }

    systemClock->seconds = 0;
    systemClock->nanoseconds = 0;

    string msgString = to_string(msgId),
        shmString = to_string(shmId);

    setenv("OSS_MSG_ID", msgString.c_str(), 1);
    setenv("OSS_SHM_ID", shmString.c_str(), 1);

    nextLaunchTime = 0;
    cout << "OSS starting, PID: " << getpid() << " PPID: " << getppid() << '\n';
    cout << "Called with:\n" << "-n " << maxProcesses << '\n' << "-s " << simultaneousProcesses << '\n' << "-t " << timeLimit << '\n' << "-i " << launchInterval << '\n';

    logMessage("OSS: Starting MLFQ scheduler.\n");

    while (!terminateRequested && (totalLaunched < maxProcesses || activeChildren() > 0)) {

        long long currentTime = clockToNano();
        checkBlockedProcesses();
        currentTime = clockToNano();

        while (totalLaunched < maxProcesses && activeChildren() < simultaneousProcesses && currentTime >= nextLaunchTime) {
            if (!launchWorker())
                break;

            nextLaunchTime = clockToNano() + launchIntervalNano;
            currentTime = clockToNano();

            if (launchIntervalNano > 0)
                break;
        }

        updateProcessTableTiming();
        printQueues();

        int selected = selectProcess();

        if (selected != -1) {
            if (!dispatchProcess(selected) && !terminateRequested) {
                logMessage("OSS: Communication error.\n");
                break;
            }
        }
        else 
            advanceIdleClock();

        updateProcessTableTiming();
    }

    if (terminateRequested) {
        logMessage("OSS: Termination signal received.\n");
        killChildren();
        totalFinished = totalLaunched;
    }

    printFinalReport();
    cleanup();
    return 0;
}
