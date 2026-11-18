CXX = g++
CXXFLAGS = -Wall -Wextra -std=c++17 -g

all: oss worker

oss: oss.cpp resource.h
	$(CXX) $(CXXFLAGS) -o oss oss.cpp

worker: worker.cpp resource.h
	$(CXX) $(CXXFLAGS) -o worker worker.cpp

clean:
	rm -f oss worker *.o oss.log

.PHONY: all clean
