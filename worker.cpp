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

#include "resource.h"

using namespace std;

int shmId = -1,
    msgId = -1;

SharedClock* systemClock = nullptr;

volatile sig_atomic_t terminateRequested = 0;

int resourcesHeld[RESOURCE_TYPES] = {0};

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

long long clockToNano()
{
    return static_cast<long long>(systemClock->seconds) * BILLION + systemClock->nanoseconds;
}

void sendAction(int action)
{
    Message outgoing{};

    outgoing.mtype = OSS_MESSAGE_TYPE;
    outgoing.pid = getpid();
    outgoing.command = 0;
    outgoing.resourceAction = action;

    while (msgsnd(msgId, &outgoing, sizeof(Message) - sizeof(long), 0) == -1) {

        if (errno == EINTR) {
            if (terminateRequested)
                return;
            continue;
        }

        if (errno == EIDRM || errno == EINVAL) {
            terminateRequested = 1;
            return;
        }

        perror("worker msgsnd");
        terminateRequested = 1;
        return;
    }
}

bool receiveMessage(Message& incoming)
{
    while (msgrcv(msgId, &incoming, sizeof(Message) - sizeof(long), static_cast<long>(getpid()), 0) == -1) {

        if (errno == EINTR) {
            if (terminateRequested)
                return false;
            continue;
        }

        if (errno == EIDRM || errno == EINVAL) {
            terminateRequested = 1;
            return false;
        }

        perror("worker msgrcv");
        terminateRequested = 1;
        return false;
    }

    return true;
}

int chooseResourceToRequest(mt19937_64& engine)
{
    int candidates[RESOURCE_TYPES];
    int count = 0;

    for (int i = 0; i < RESOURCE_TYPES; i++) {
        if (resourcesHeld[i] < RESOURCE_INSTANCES)
            candidates[count++] = i;
    }

    if (count == 0)
        return -1;

    uniform_int_distribution<int> distribution(0, count - 1);

    return candidates[distribution(engine)];
}

int chooseResourceToRelease(mt19937_64& engine)
{
    int candidates[RESOURCE_TYPES],
        count = 0;

    for (int i = 0; i < RESOURCE_TYPES; i++) {
        if (resourcesHeld[i] > 0)
            candidates[count++] = i;
    }

    if (count == 0)
        return -1;

    uniform_int_distribution<int> distribution(0, count - 1);

    return candidates[distribution(engine)];
}

int main(int argc, char* argv[])
{
    if (argc != 4) {
        cerr << "Usage: ./worker lifetime startSeconds startNano\n";
        return 1;
    }

    long long lifetime = atoll(argv[1]);

    unsigned int startSeconds = static_cast<unsigned int>(atoi(argv[2])),
        startNano = static_cast<unsigned int>(atoi(argv[3]));

    struct sigaction sa{};

    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);

    const char* shmEnvironment = getenv("OSS_SHM_ID");
    const char* msgEnvironment = getenv("OSS_MSG_ID");

    if (!shmEnvironment || !msgEnvironment) {
        cerr << "Missing IPC environment variables.\n";
        return 1;
    }

    shmId = atoi(shmEnvironment);
    msgId = atoi(msgEnvironment);

    systemClock = static_cast<SharedClock*>(shmat(shmId, nullptr, 0));

    if (systemClock == reinterpret_cast<SharedClock*>(-1)) {
        perror("worker shmat");
        systemClock = nullptr;
        return 1;
    }

    mt19937_64 engine(static_cast<unsigned long long>(getpid()) ^ static_cast<unsigned long long>(time(nullptr)));

    uniform_int_distribution<int> actionChance(1, 100);

    cout << "WORKER " << getpid() << " starting. Lifetime: " << lifetime << " ns\n";

    while (!terminateRequested) {

        Message incoming{};

        if (!receiveMessage(incoming))
            break;

        if (incoming.command == TERMINATE) {
            cout << "WORKER " << getpid() << " received termination.\n";
            break;
        }

        if (incoming.command == GRANT) {

            int resource = incoming.resourceAction;

            if (resource >= 0 && resource < RESOURCE_TYPES) {
                resourcesHeld[resource]++;
                cout << "WORKER " << getpid() << " received R" << resource << ". Total held: " << resourcesHeld[resource] << "\n";
            }

            continue;
        }

        if (incoming.command != DISPATCH)
            continue;

        long long currentTime = clockToNano(),
            startTime = static_cast<long long>(startSeconds) * BILLION + startNano;

        if (currentTime - startTime >= lifetime) {
            cout << "WORKER " << getpid() << " lifetime expired.\n";
            sendAction(0);
            break;
        }

        int chance = actionChance(engine),
            action = 0;

        if (chance <= 70) {
            int resource = chooseResourceToRequest(engine);

            if (resource != -1) {
                action = resource + 1;

                cout << "WORKER " << getpid() << " requesting R" << resource << "\n";
            }
        }

        if (action == 0 && chance > 70) {
            int resource = chooseResourceToRelease(engine);

            if (resource != -1) {
                action = -(resource + 1);
                resourcesHeld[resource]--;
                cout << "WORKER " << getpid() << " releasing R" << resource << "\n";
            }
        }

        if (action == 0) {
            int resource = chooseResourceToRequest(engine);

            if (resource != -1)
                action = resource + 1;
            else {
                resource = chooseResourceToRelease(engine);

                if (resource != -1) {
                    action = -(resource + 1);
                    resourcesHeld[resource]--;
                }
            }
        }

        if (action == 0)
            continue;

        sendAction(action);
    }

    cleanup();

    return 0;
}
