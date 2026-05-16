#ifndef __TEST_BOOL__
#define __TEST_BOOL__

#include<iostream>
#include<vector>
#include<string>

// #define MAX_DATA_SIZE 1000000

class TestBool{
public:
    double rate; // Probability to generate 1.
    int data_size;
    std::vector<char> data;
    char * compressed;
    char * decompressed;

    TestBool(double rate = 0.5, int data_size = 10000);
    TestBool(std::string fname);
    ~TestBool();

    void generateData();
    // void writeData(std::string fname);
    void writeData(std::string fname);
    double test(int times = 10, bool display = false);
    double statistic(int times = 500, bool display = true);
    bool isSuccessful();
    void showSummary(double c_rate);
};

#endif
