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

using namespace std;

const long OSS_MESSAGE_TYPE = 1;

struct SharedClock {
    int seconds,
    nanoseconds;
};

struct Message {
    long mtype;
    pid_t pid;
    int status;
};

int shmId = -1;
int msgId = -1;
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
    cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << " SysClockS: " << systemClock->seconds << " SysclockNano: " << systemClock->nanoseconds << endl;
    cout << "TermTimeS: " << termSeconds << " TermTimeNano: " << termNano << endl;
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

    if (shmEnvironment == nullptr || msgEnvironment == nullptr) {
        cerr << "Error: OSS_SHM_ID or OSS_MSG_ID was not set." << endl;
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

    int startSeconds = systemClock->seconds,
        startNano = systemClock->nanoseconds,
        termSeconds = startSeconds + intervalSeconds,
        termNano = startNano + intervalNano;

    normalizeTime(termSeconds, termNano);

    printWorkerStatus(termSeconds,termNano,"--Just Starting");

    int messagesReceived = 0;
    bool done = false;

    while (!done && !terminateRequested) {
        Message incoming {};

        while (msgrcv( msgId, &incoming, sizeof(Message) - sizeof(long), static_cast<long>(getpid()), 0) == -1) {
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

        messagesReceived++;

        int currentSeconds = systemClock->seconds,
            currentNano = systemClock->nanoseconds;

        bool timeExpired = compareTime(currentSeconds, currentNano, termSeconds, termNano) >= 0;

        Message outgoing {};
        outgoing.mtype = OSS_MESSAGE_TYPE;
        outgoing.pid = getpid();
        outgoing.status = timeExpired ? 0 : 1;

        if (timeExpired) {
            printWorkerStatus(termSeconds, termNano, "--Terminating after sending message back to OSS after " + to_string(messagesReceived) + " received messages.");
            done = true;
        } 
        else {
            string messageText;
            
            if (messagesReceived == 1)
                messageText = "--1 message received from OSS";
            else 
                messageText = "--" + to_string(messagesReceived) + " messages received from OSS";

            printWorkerStatus(termSeconds, termNano, messageText);
        }

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
    }

    cleanup();
    return 0;
}
