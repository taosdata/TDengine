#ifndef __TEST_FLOAT__
#define __TEST_FLOAT__

#include <vector>
#include <string>

#define PI 3.141592454
#define MOD1 0
#define MOD2 1
#define MOD3 2

class TestFloat{
public:

    int model;
    std::vector<float> data;
    int data_size;
    char * compressed;
    float * decompressed;
    float A,T,b;
    float mu, sigma;
    float start_value;

    TestFloat(float A, float T, float b, float mu, float sigma, int model, int data_size = 5000);
    TestFloat(float start_value, float mu, float sigma, int data_size = 5000);
    TestFloat(std::string fname);
    ~TestFloat();

    // Singular and rectangular wave generator
    void generateData();
    // Random walk generator
    // void generateData(float start_value, float mu, float sigma);
    void writeData(std::string fname);
    float test(int times, bool display = false);
    float statistic(int times = 10, bool display = true);
    bool isSuccessful();
    void showSummary(float c_rate);

};

#endif
