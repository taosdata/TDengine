#ifndef __TEST_TIMESTAMP__
#define __TEST_TIMESTAMP__

#include<iostream>
#include<vector>
#include<string>

// #define MAX_DATA_SIZE 1000000

class TestTimestamp{
public:
    int period; // In second.
    double mu;
    double sigma;
    double missing_rate;
    int data_size;
    std::vector<unsigned long> data;
    char * compressed;
    unsigned long * decompressed;

    TestTimestamp(int period = 5, double mu = 0, double sigma = 1, int data_size = 10000, double missing_rate = 0.05);
    TestTimestamp(std::string fname);
    ~TestTimestamp();

    void generateData();
    void writeData(std::string fname);
    double test(int times = 10, bool display = false);
    double statistic(int times, bool display = true);
    bool isSuccessful();
    void showSummary(double c_rate);
};

#endif
