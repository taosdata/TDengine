#include <iostream>
#include <random>
#include <cstring>
#include <iomanip>

#include "taosdef.h"
#include "TestString.h"
#include "tsCompression.h"

TestString::TestString(int data_size) {
    this->data_size = data_size;
    compressed = new char[data_size];
    decompressed = new char[data_size];
}

TestString::~TestString() {
    delete [] compressed;
    delete [] decompressed;
}

void TestString::generateData() {
    std::srand((unsigned)time(0));
    for (int i = 0; i < data_size; i++) {
        data.push_back(std::rand() % 95 + 32);
    }
}

double TestString::test(bool display) {
    data.clear();
    std::memset(compressed, 0, data_size);
    std::memset(decompressed, 0, data_size);

    generateData();

    int nbytes = tsCompressString(data.data(), data_size, compressed);

    tsDecompressString(compressed, data_size, decompressed);
    if (isSuccessful()) {
        std::cout << "SUCCEED!" << std::endl;
    }
    else {
        std::cout << "FAIL!" << std::endl;
        exit(1);
    }

    if (display) {
        showSummary((double) nbytes / (sizeof(char) * data_size));
    }
    
    return (double) nbytes / (sizeof(char) * data_size);
}

double TestString::statistic(int times, bool display) {
    double result = 0;

    for (int i = 0; i < times; i++) {
        double c_rate = test(false);
        result = (result * i + c_rate) / (i+1);
    }

    if (display) {
        std::cout << "Experiment times: " << times << std::endl;
        showSummary(result);
    }

    return result;

}

bool TestString::isSuccessful(){
    for (int i = 0; i < data_size; i++) {
        if (data[i] != decompressed[i])
           return false; 
    }
    return true;
}

void TestString::showSummary(double c_rate) {
        std::cout << "=====================================================" << std::endl;
        std::cout << "Data size: " << data_size << " (" << std::fixed << std::setprecision(1);
        std::cout << (double)sizeof(char) * data_size / 1024 <<" K)" << std::endl;
        std::cout << std::endl;
        std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
        std::cout << c_rate;
        std::cout << std::endl;
        std::cout << std::endl;
}
