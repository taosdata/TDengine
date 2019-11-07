#ifndef __TEST_INT__
#define __TEST_INT__

#include <vector>
#include <string>

#define MOD1 0
#define MOD2 1

template <typename T>
class TestInt{
public:
    int a,b,c;
    int sigma;
    int model;
    std::vector<T> data;
    int data_size;
    char * compressed;
    T * decompressed;

    TestInt(int a, int b, int c, int sigma, int model = 0, int data_size = 5000);
    TestInt(std::string fname);
    ~TestInt();

    void generateData();
    void writeData(std::string fname);
    double test(int times = 10, bool display = false);
    double statistic(int times = 100, bool display = true);
    bool isSuccessful();
    void showSummary(double c_rate);
};
#endif
