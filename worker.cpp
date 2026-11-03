
#include <iostream>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <signal.h>
#include <sys/msg.h>
#include <random>
#include <ctime>

using namespace std;

const long OSS_MESSAGE_TYPE = 1;
const long long BILLION = 1000000000LL;

enum {
    TERMINATED = 0,
    QUANTUM_USED = 1,
    IO_BLOCKED = 2
};

struct SharedClock {
    unsigned int seconds,
        nanoseconds;
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

volatile sig_atomic_t terminateRequested = 0;

void signalHandler(int signalNumber)
{
    if (signalNumber == SIGTERM || signalNumber == SIGINT)
        terminateRequested = 1;
}

void cleanup()
{
    if (systemClock != nullptr) {
        shmdt(systemClock);
        systemClock = nullptr;
    }
}

int main(int argc, char* argv[])
{
    if (argc != 2) {
        cerr << "Usage: ./worker totalBurstNanoseconds\n";
        return 1;
    }

    long long remainingTime = atoll(argv[1]);

    if (remainingTime < 0) {
        cerr << "Invalid CPU burst.\n";
        return 1;
    }

    struct sigaction sa {};

    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGTERM, &sa, nullptr) == -1 || sigaction(SIGINT, &sa, nullptr) == -1) {
        perror("sigaction");
        return 1;
    }

    const char* shmEnvironment = getenv("OSS_SHM_ID");
    const char* msgEnvironment = getenv("OSS_MSG_ID");

    if (!shmEnvironment || !msgEnvironment) {
        cerr << "OSS shared memory or message queue ID missing.\n";
        return 1;
    }

    shmId = atoi(shmEnvironment);
    msgId = atoi(msgEnvironment);

    systemClock = static_cast<SharedClock*>(shmat(shmId, nullptr, 0));

    if (systemClock == reinterpret_cast<SharedClock*>(-1)) {
        perror("shmat");
        systemClock = nullptr;
        return 1;
    }

    mt19937_64 randomEngine(static_cast<unsigned long long>(getpid()) ^ static_cast<unsigned long long>(time(nullptr)));

    uniform_int_distribution<int> ioChance(1, 100);

    cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << " Starting CPU burst: " << remainingTime << " nanoseconds\n";

    while (!terminateRequested) {
        Message incoming {};

        while (msgrcv(msgId, &incoming, sizeof(Message) - sizeof(long), static_cast<long>(getpid()), 0) == -1) {
            if (errno == EINTR) {
                if (terminateRequested)
                    break;

                continue;
            }

            if (errno == EIDRM || errno == EINVAL) {
                terminateRequested = 1;
                break;
            }

            perror("worker msgrcv");
            terminateRequested = 1;
            break;
        }

        if (terminateRequested)
            break;

        long long quantum = incoming.quantum;

        if (quantum <= 0)
            continue;

        Message outgoing {};

        outgoing.mtype = OSS_MESSAGE_TYPE;
        outgoing.pid = getpid();

        if (remainingTime <= quantum) {
            outgoing.used = remainingTime;
            outgoing.status = TERMINATED;
            cout << "WORKER PID:" << getpid() << " Terminating after using " << outgoing.used << " nanoseconds\n";
            remainingTime = 0;
        }
        else {
            int chance = ioChance(randomEngine);

            if (chance <= 20) {
                uniform_int_distribution<long long> ioTime(1, quantum - 1);
                outgoing.used = ioTime(randomEngine);
                outgoing.status = IO_BLOCKED;

                cout << "WORKER PID:" << getpid() << " Becoming blocked after using " << outgoing.used << " nanoseconds\n";
            }
            else {
                outgoing.used = quantum;
                outgoing.status = QUANTUM_USED;
                cout << "WORKER PID:" << getpid() << " Used full quantum of " << quantum << " nanoseconds\n";
            }
        }

        remainingTime -= outgoing.used;

        while (msgsnd(msgId, &outgoing, sizeof(Message) - sizeof(long), 0) == -1) {

            if (errno == EINTR) {
                if (terminateRequested)
                    break;

                continue;
            }

            if (errno == EIDRM || errno == EINVAL) {
                terminateRequested = 1;
                break;
            }

            perror("worker msgsnd");
            terminateRequested = 1;
            break;
        }

        if (outgoing.status == TERMINATED)
            break;
    }

    cleanup();

    return 0;
}
