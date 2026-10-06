CXX = g++

CXXFLAGS = -Wall -Wextra -std=c++17 -g

TARGETS = oss worker

all: $(TARGETS)

oss: oss.cpp
	$(CXX) $(CXXFLAGS) -o oss oss.cpp

worker: worker.cpp
	$(CXX) $(CXXFLAGS) -o worker worker.cpp

clean:
	rm -f $(TARGETS) *.o

.PHONY: all clean
