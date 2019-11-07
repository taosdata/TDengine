#ifndef __TEST_STRING__
#define __TEST_STRING__

#include <vector>
// TODO: debug the string compression.

class TestString{
public:
    int data_size;
    std::vector<char> data;
    char * compressed;
    char * decompressed;

    TestString(int data_size = 10000);
    ~TestString();

    void generateData();
    double test(bool display = true);
    double statistic(int times, bool display = true);
    bool isSuccessful();
    void showSummary(double c_rate);

};

#endif
