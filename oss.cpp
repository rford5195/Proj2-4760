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

using namespace std;

const int MAX_PCB = 20;

const long long BILLION = 1000000000LL,
    TABLE_INTERVAL_NS = 500000000LL,
    CLOCK_INCREMENT_NS = 250000000LL;

struct SharedClock {
    int seconds,
    nanoseconds;
};

struct PCB {
    int occupied;
    pid_t pid;

    int startSeconds,
    startNano,
    endingTimeSeconds,
    endingTimeNano,
    messagesSent;
};

struct Message {
    long mtype;
    pid_t pid;
    int status;
};

int shmId = -1,
    msgId = -1,
    nextWorkerIndex = 0;
SharedClock* systemClock = nullptr;

PCB processTable[MAX_PCB];
FILE* logFile = nullptr;

long long totalMessagesSent = 0,
    maxLifetimeNano = 7000000000LL,
    launchIntervalNano = 0,
    lastLaunchTime = 0;

mt19937_64 randomEngine;

volatile sig_atomic_t terminateRequested = 0;

int totalLaunched = 0,
totalFinished = 0;

long long totalRuntimeNano = 0;

void signalHandler(int signalNumber)
{
    if (signalNumber == SIGINT || signalNumber == SIGALRM)
        terminateRequested = 1;

}

long long toTotalNano(int seconds, int nanoseconds)
{
    return static_cast<long long>(seconds) * 1'000'000'000LL + nanoseconds;
}

void normalizeTime(int& seconds, int& nanoseconds)
{
    while (nanoseconds >= 1'000'000'000) {
        seconds++;
        nanoseconds -= 1'000'000'000;
    }

    while (nanoseconds < 0) {
        seconds--;
        nanoseconds += 1'000'000'000;
    }
}

void addTime(int startSeconds, int startNano, int addSeconds, int addNano, int& resultSeconds, int& resultNano)
{
    resultSeconds = startSeconds + addSeconds;
    resultNano = startNano + addNano;
    normalizeTime(resultSeconds, resultNano);
}

int findFreePCB()
{
    for (int i = 0; i < MAX_PCB; i++) {
        if (!processTable[i].occupied)
            return i;
    }

    return -1;
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

    if (logFile != nullptr) {
        va_list fileArgs;
        va_copy(fileArgs, args);
        vfprintf(logFile, format, fileArgs);
        fflush(logFile);
        va_end(fileArgs);
    }

    va_end(args);
}

int findPCB(pid_t pid)
{
    for (int i = 0; i < MAX_PCB; i++) {
        if (processTable[i].occupied && processTable[i].pid == pid)
            return i;
    }

    return -1;
}

void printProcessTable()
{
    logMessage("\nOSS PID:%d SysClockS: %d SysclockNano: %d\n",static_cast<int>(getpid()),systemClock->seconds,systemClock->nanoseconds);
    logMessage("Process Table:\n");
    logMessage("%-7s %-10s %-10s %-10s %-12s %-12s %-12s %-14s\n","Entry", "Occupied", "PID", "StartS", "StartN","EndingTS", "EndingTN", "MessagesSent");

    for (int i = 0; i < MAX_PCB; i++) 
        logMessage("%-7d %-10d %-10d %-10d %-12d %-12d %-12d %-14d\n",i,processTable[i].occupied,static_cast<int>(processTable[i].pid),processTable[i].startSeconds,processTable[i].startNano,processTable[i].endingTimeSeconds,processTable[i].endingTimeNano,processTable[i].messagesSent);

    logMessage("\n");
}

void killChildren()
{
    for (int i = 0; i < MAX_PCB; i++) {
        if (processTable[i].occupied && processTable[i].pid > 0) 
            kill(processTable[i].pid, SIGTERM);
    }

    while (waitpid(-1, nullptr, WNOHANG) > 0) {}
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

void printFinalReport()
{

    logMessage("\nOSS PID:%d Terminating\n", static_cast<int>(getpid()));
    logMessage("%d workers were launched and terminated\n",  totalLaunched);
    logMessage("Total messages sent: %lld\n", totalMessagesSent);

    long long totalSeconds = totalRuntimeNano / BILLION,
        totalNano = totalRuntimeNano % BILLION;

    logMessage("Workers ran for a combined time of %lld seconds %lld nanoseconds.\n", totalSeconds, totalNano);

}

bool launchWorker()
{
    int pcbIndex = findFreePCB();

    if (pcbIndex == -1)
        return false;

    uniform_int_distribution<long long> lifetimeDist(BILLION, maxLifetimeNano);

    long long lifetimeNano = lifetimeDist(randomEngine);

    int workerSeconds = static_cast<int>(lifetimeNano / BILLION),
        workerNano = static_cast<int>(lifetimeNano % BILLION);

    int startSeconds = systemClock->seconds,
        startNano = systemClock->nanoseconds,
        endingSeconds, 
        endingNano;

    addTime(startSeconds, startNano, workerSeconds, workerNano, endingSeconds, endingNano);

    pid_t pid = fork();

    if (pid < 0) {
        perror("fork");
        return false;
    }

    if (pid == 0) {
        string secondsString = to_string(workerSeconds),
            nanoString = to_string(workerNano);

        execl("./worker", "./worker", secondsString.c_str(), nanoString.c_str(), nullptr);

        perror("execl");
        _exit(EXIT_FAILURE);
    }

    processTable[pcbIndex].occupied = 1;
    processTable[pcbIndex].pid = pid;
    processTable[pcbIndex].startSeconds = startSeconds;
    processTable[pcbIndex].startNano = startNano;
    processTable[pcbIndex].endingTimeSeconds = endingSeconds;
    processTable[pcbIndex].endingTimeNano = endingNano;
    processTable[pcbIndex].messagesSent = 0;

    totalLaunched++;

    logMessage("OSS: Launched worker at PCB %d, PID %d\n", pcbIndex, static_cast<int>(pid));
    printProcessTable();

    return true;
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

void incrementClock()
{
    int count = activeChildren();
    long long increment = 250000000LL;

    if (count > 0)
        increment /= count;

    systemClock->nanoseconds += static_cast<int>(increment);

    normalizeTime(systemClock->seconds, systemClock->nanoseconds);
}

bool communicateWithWorker(int pcbIndex)
{
    Message outgoing {};
    outgoing.mtype = processTable[pcbIndex].pid;
    outgoing.pid = getpid();
    outgoing.status = 1;

    pid_t workerPid = processTable[pcbIndex].pid;

    logMessage("OSS: Sending message to worker %d PID %d at time %d:%d\n", pcbIndex, static_cast<int>(workerPid), systemClock->seconds, systemClock->nanoseconds);

    if (msgsnd(msgId, &outgoing, sizeof(Message) - sizeof(long), 0) == -1) {
        if (errno != EINTR)
            perror("msgsnd");

        return false;
    }

    processTable[pcbIndex].messagesSent++;
    totalMessagesSent++;

    Message incoming {};

    while ((totalLaunched < maxProcesses || totalFinished < totalLaunched) && !terminateRequested) {

        int active = activeChildren();

        if (active > 0)
            incrementClock();

        long long currentTime = toTotalNano(systemClock->seconds, systemClock->nanoseconds);

        if (currentTime >= nextTablePrint) {
            printProcessTable();

            while (nextTablePrint <= currentTime) {
                nextTablePrint += TABLE_INTERVAL_NS;
            }
        }

        if (active > 0) {
            int selected = -1;

            for (int i = 0; i < MAX_PCB; i++) {
                int candidate = (nextWorkerIndex + i) % MAX_PCB;

                if (processTable[candidate].occupied) {
                    selected = candidate;
                    nextWorkerIndex = (candidate + 1) % MAX_PCB;
                    break;
                }
            }

            if (selected != -1) {
                if (!communicateWithWorker(selected) && !terminateRequested)
                    logMessage("OSS: Communication failed for PCB %d\n", selected);
            }
        }

        if (terminateRequested)
            break;

        active = activeChildren();

        currentTime = toTotalNano(systemClock->seconds, systemClock->nanoseconds);

        if (totalLaunched < maxProcesses && active < simultaneousProcesses && currentTime - lastLaunchTime >= launchIntervalNano) {
            if (launchWorker(timeLimit))
                lastLaunchTime = currentTime;
        }
    }

    if (terminateRequested) {
        logMessage("OSS received termination signal.\n");
        killChildren();

        for (int i = 0; i < MAX_PCB; i++) {
            if (processTable[i].occupied && processTable[i].pid > 0) {
                while (waitpid(processTable[i].pid, nullptr, 0) == -1) {
                    if (errno == EINTR)
                        continue;

                    if (errno != ECHILD) 
                        perror("waitpid");

                    break;
                }

                memset(&processTable[i], 0, sizeof(PCB));
            }
        }

        totalFinished = totalLaunched;
    }

    printFinalReport();
    cleanup();

    return 0;
}
