#ifndef __TEST_DOUBLE__
#define __TEST_DOUBLE__

#include <vector>
#include <string>

#define PI 3.141592454
#define MOD1 0
#define MOD2 1
#define MOD3 2

class TestDouble{
public:

    int model;
    std::vector<double> data;
    int data_size;
    char * compressed;
    double * decompressed;
    double A,T,b;
    double mu, sigma;
    double start_value;

    TestDouble(double A, double T, double b, double mu, double sigma, int model, int data_size = 5000);
    TestDouble(double start_value, double mu, double sigma, int data_size = 5000);
    TestDouble(std::string fname);
    ~TestDouble();

    // Singular and rectangular wave generator
    void generateData();
    // Random walk generator
    // void generateData(double start_value, double mu, double sigma);
    void writeData(std::string fname);
    double test(int times = 10, bool display = false);
    double statistic(int times = 100, bool display = true);
    bool isSuccessful();
    void showSummary(double c_rate);

};

#endif
