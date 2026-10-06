#include <iostream>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <signal.h>

using namespace std;

struct SharedClock {
    int seconds,
    nanoseconds;
};

int shmId = -1;
SharedClock* systemClock = nullptr;

volatile sig_atomic_t terminateRequested = 0;

void signalHandler(int signalNumber)
{
    if (signalNumber == SIGTERM || signalNumber == SIGINT)
        terminateRequested = 1;
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

int compareTime(int seconds1, int nano1, int seconds2, int nano2)
{
    if (seconds1 < seconds2)
        return -1;

    if (seconds1 > seconds2)
        return 1;

    if (nano1 < nano2)
        return -1;

    if (nano1 > nano2)
        return 1;

    return 0;
}

void cleanup()
{
    if (systemClock != nullptr) {
        shmdt(systemClock);
        systemClock = nullptr;
    }
}

void printWorkerStatus(int termSeconds, int termNano, const string& message)
{
    cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << endl;
    cout << "SysClockS: " << systemClock->seconds << " SysclockNano: " << systemClock->nanoseconds << " TermTimeS: " << termSeconds << " TermTimeNano: " << termNano << endl;
    cout << message << endl;
}

int main(int argc, char* argv[])
{
    if (argc != 3) {
        cerr << "Usage: ./worker seconds nanoseconds" << endl;
        return 1;
    }

    int intervalSeconds = atoi(argv[1]),
    intervalNano = atoi(argv[2]);

    if (intervalSeconds < 0 || intervalNano < 0) {
        cerr << "Error: worker interval cannot be negative."<< endl;
        return 1;
    }

    normalizeTime(intervalSeconds, intervalNano);

    cout << "Worker starting, PID:" << getpid() << " PPID:" << getppid() << endl;
    cout << "Called with:" << endl;
    cout << "Interval: " << intervalSeconds << " seconds, " << intervalNano << " nanoseconds" << endl;

    struct sigaction sa {};

    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGTERM, &sa, nullptr) == -1) {
        perror("sigaction");
        return 1;
    }

    if (sigaction(SIGINT, &sa, nullptr) == -1) {
        perror("sigaction");
        return 1;
    }

    const char* shmEnvironment = getenv("OSS_SHM_ID");

    if (shmEnvironment == nullptr) {
        cerr << "Error: OSS_SHM_ID was not set." << endl;
        return 1;
    }

    shmId = atoi(shmEnvironment);
    systemClock = static_cast<SharedClock*>(shmat(shmId, nullptr, 0));

    if (systemClock == reinterpret_cast<SharedClock*>(-1)) {
        perror("shmat");
        systemClock = nullptr;
        return 1;
    }

    int startSeconds = systemClock->seconds,
    startNano = systemClock->nanoseconds,
    termSeconds = startSeconds + intervalSeconds,
    termNano = startNano + intervalNano;

    normalizeTime(termSeconds, termNano);
    printWorkerStatus(termSeconds, termNano, "--Just Starting");

    int lastReportedSecond = startSeconds;

    while (!terminateRequested) {

        int currentSeconds = systemClock->seconds,
        currentNano = systemClock->nanoseconds;

        if (compareTime(currentSeconds, currentNano, termSeconds, termNano) >= 0) {
            printWorkerStatus(termSeconds, termNano, "--Terminating");
            break;
        }

        if (currentSeconds != lastReportedSecond) {
            int elapsedSeconds = currentSeconds - startSeconds;
            printWorkerStatus(termSeconds, termNano, "--" + to_string(elapsedSeconds) + " seconds have passed since starting");
            lastReportedSecond = currentSeconds;
        }
    }

    cleanup();

    return 0;
}
