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

using namespace std;

const int MAX_PCB = 20,
    CLOCK_INCREMENT_NS = 10'000'000,
    TABLE_INTERVAL_NS = 500'000'000;

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
    endingTimeNano;
};

int shmId = -1;
SharedClock* systemClock = nullptr;

PCB processTable[MAX_PCB];

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

// ------------------------------------------------------------
// Find PCB associated with a PID
// ------------------------------------------------------------
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
    cout << "\nOSS PID:" << getpid() << " SysClockS: " << systemClock->seconds << " SysclockNano: " << systemClock->nanoseconds << endl;
    cout << "Process Table:" << endl;
    cout << left << setw(7) << "Entry" << setw(10) << "Occupied" << setw(10) << "PID" << setw(10) << "StartS" << setw(12) << "StartN" << setw(15) << "EndingTimeS" << setw(18) << "EndingTimeNano" << endl;

    for (int i = 0; i < MAX_PCB; i++)
        cout << left << setw(7) << i << setw(10) << processTable[i].occupied << setw(10) << processTable[i].pid << setw(10) << processTable[i].startSeconds << setw(12) << processTable[i].startNano << setw(15) << processTable[i].endingTimeSeconds << setw(18) << processTable[i].endingTimeNano << endl;

    cout << endl;
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
}

void printFinalReport()
{
    cout << "\nOSS PID:" << getpid() << " Terminating" << endl;
    cout << totalLaunched << " workers were launched and terminated" << endl;

    long long totalSeconds = totalRuntimeNano / 1'000'000'000LL,
    totalNano = totalRuntimeNano % 1'000'000'000LL;

    cout << "Workers ran for a combined time of " << totalSeconds << " seconds " << totalNano << " nanoseconds." << endl;
}

bool launchWorker(double workerLifetime)
{
    int pcbIndex = findFreePCB();

    if (pcbIndex == -1)
        return false;

    int workerSeconds = static_cast<int>(workerLifetime),
    workerNano = static_cast<int>((workerLifetime - workerSeconds) * 1'000'000'000.0);

    normalizeTime(workerSeconds, workerNano);

    int startSeconds = systemClock->seconds,
    startNano = systemClock->nanoseconds;

    int endingSeconds, endingNano;

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

    totalLaunched++;

    return true;
}

void checkChildren()
{
    while (true) {

        int status = 0;

        pid_t pid = waitpid(-1, &status, WNOHANG);

        if (pid == 0)
            break;

        if (pid == -1) {
            if (errno == ECHILD) 
                break;

            if (errno == EINTR)
                continue;

            perror("waitpid");
            break;
        }

        int pcbIndex = findPCB(pid);

        if (pcbIndex != -1) {

            int endSeconds = systemClock->seconds,
            endNano = systemClock->nanoseconds,
            startSeconds = processTable[pcbIndex].startSeconds,
            startNano = processTable[pcbIndex].startNano;

            long long startTotal = toTotalNano(startSeconds, startNano),
            endTotal = toTotalNano(endSeconds, endNano);

            if (endTotal >= startTotal)
                totalRuntimeNano += endTotal - startTotal;

            processTable[pcbIndex].occupied = 0;
            processTable[pcbIndex].pid = 0;
            processTable[pcbIndex].startSeconds = 0;
            processTable[pcbIndex].startNano = 0;
            processTable[pcbIndex].endingTimeSeconds = 0;
            processTable[pcbIndex].endingTimeNano = 0;

            totalFinished++;
        }
    }
}

void incrementClock()
{
    systemClock->nanoseconds += CLOCK_INCREMENT_NS;

    normalizeTime(systemClock->seconds, systemClock->nanoseconds);
}

int main(int argc, char* argv[])
{
    int maxProcesses = 5,
    simultaneousProcesses = 3;

    double timeLimit = 5.0,
    launchInterval = 0.0;

    int option;

    while ((option = getopt(argc, argv, "hn:s:t:i:")) != -1) {
        switch (option) {
            case 'h':
                cout << "Usage: ./oss " << "[-h] " << "[-n proc] " << "[-s simul] " << "[-t timelimit] " << "[-i interval]" << endl;
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

            default:
                cerr << "Usage: ./oss " << "[-h] " << "[-n proc] " << "[-s simul] " << "[-t timelimit] " << "[-i interval]" << endl;
                return 1;
        }
    }

    if (maxProcesses <= 0) {
        cerr << "Error: -n must be greater than 0." << endl;
        return 1;
    }

    if (simultaneousProcesses <= 0) {
        cerr << "Error: -s must be greater than 0." << endl;
        return 1;
    }

    if (simultaneousProcesses > MAX_PCB)
        simultaneousProcesses = MAX_PCB;

    if (timeLimit <= 0) {
        cerr << "Error: -t must be greater than 0." << endl;
        return 1;
    }

    if (launchInterval < 0) {
        cerr << "Error: -i cannot be negative." << endl;
        return 1;
    }

    memset(processTable, 0, sizeof(processTable));

    struct sigaction sa {};
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGINT, &sa, nullptr) == -1) {
        perror("sigaction");
        return 1;
    }

    if (sigaction(SIGALRM, &sa, nullptr) == -1) {
        perror("sigaction");
        return 1;
    }

    alarm(60);

    shmId = shmget(IPC_PRIVATE, sizeof(SharedClock), IPC_CREAT | 0666);

    if (shmId == -1) {
        perror("shmget");
        return 1;
    }

    systemClock = static_cast<SharedClock*>(shmat(shmId, nullptr, 0));

    if (systemClock == reinterpret_cast<SharedClock*>(-1)) {

        perror("shmat");
        systemClock = nullptr;
        shmctl(shmId, IPC_RMID, nullptr);
        shmId = -1;
        return 1;
    }

    systemClock->seconds = 0;
    systemClock->nanoseconds = 0;

    string shmIdString = to_string(shmId);

    if (setenv("OSS_SHM_ID", shmIdString.c_str(), 1) == -1) {
        perror("setenv");
        cleanup();
        return 1;
    }

    cout << "OSS starting, PID:" << getpid() << " PPID:" << getppid() << endl;
    cout << "Called with:" << endl;
    cout << "-n " << maxProcesses << endl;
    cout << "-s " << simultaneousProcesses << endl;
    cout << "-t " << timeLimit << endl;
    cout << "-i " << launchInterval << endl;
    cout << endl;

    long long launchIntervalNano = static_cast<long long>( launchInterval * 1'000'000'000.0 ),
    lastLaunchTime = -launchIntervalNano,
    nextTablePrint = TABLE_INTERVAL_NS;

    while ((totalLaunched < maxProcesses || totalFinished < totalLaunched) && !terminateRequested) {
        incrementClock();

        long long currentTime = toTotalNano(systemClock->seconds, systemClock->nanoseconds);

        if (currentTime >= nextTablePrint) {

            printProcessTable();

            while (nextTablePrint <= currentTime)
                nextTablePrint += TABLE_INTERVAL_NS;
        }

        checkChildren();
        currentTime = toTotalNano(systemClock->seconds, systemClock->nanoseconds);

        int activeChildren = totalLaunched - totalFinished;

        if (totalLaunched < maxProcesses && activeChildren < simultaneousProcesses && currentTime - lastLaunchTime >= launchIntervalNano) {
            if (launchWorker(timeLimit)) 
                lastLaunchTime = currentTime;
        }
    }

    if (terminateRequested) {
        cerr << "\nOSS received termination signal." << endl;
        killChildren();
    }

    while (waitpid(-1, nullptr, WNOHANG) > 0) {}

    printFinalReport();
    cleanup();

    return 0;
}
