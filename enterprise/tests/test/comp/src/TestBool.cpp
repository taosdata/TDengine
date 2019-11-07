#include <iostream>
#include <random>
#include <fstream>
#include <cstring>
#include <iomanip>
#include <time.h>

#include "tsdb.h"
#include "TestBool.h"
#include "tsCompression.h"

TestBool::TestBool(double rate, int data_size) {
    this->rate = rate;
    this->data_size = data_size;
    compressed = new char[data_size + 1];
    decompressed = new char[data_size];
}

TestBool::TestBool(std::string fname) {
    std::ifstream ifile(fname, std::ios::in | std::ios::binary);
    ifile.unsetf(std::ios::skipws);
    std::streampos fileSize;

    // get the file size.
    ifile.seekg(0, std::ios::end);
    fileSize = ifile.tellg();
    ifile.seekg(0, std::ios::beg);
    data.clear();

    data_size = fileSize / sizeof(char);
    data.resize(data_size);

    ifile.read(reinterpret_cast<char*>(data.data()), data_size*sizeof(char));

    compressed = new char[data_size + 1];
    decompressed = new char[data_size];
}

TestBool::~TestBool() {
    delete [] compressed;
    delete [] decompressed;
}

void TestBool::generateData() {
    std::srand((unsigned)time(0));
    for (int i = 0; i < data_size; i++) {
        if ((double)std::rand() / RAND_MAX < rate) {
            data.push_back(1);
        }
        else {
            data.push_back(0);
        }
    }
}

void TestBool::writeData(std::string fname) {
    std::ofstream ofile(fname, std::ios::out | std::ios::binary);
    ofile.write((char *)data.data(), data.size());
    ofile.close();
}

double TestBool::test(int times, bool display) {

    clock_t c_begin, c_end;
    float c_time = 0;
    clock_t dc_begin, dc_end;
    float dc_time = 0;
    int nbytes = 0;
    for (int i = 0; i < times; i++) {
        c_begin = clock();
        nbytes = tsCompressBool(data.data(), data_size, compressed);
        c_end = clock();
        c_time = (i * c_time + (float)(c_end-c_begin)/CLOCKS_PER_SEC)/(i+1);

        dc_begin = clock();
        tsDecompressBool(compressed, data_size, decompressed);
        dc_end = clock();
        dc_time = (i * dc_time + (float)(dc_end-dc_begin)/CLOCKS_PER_SEC)/(i+1);
    }

    if (isSuccessful()) {
        // std::cout << "SUCCEED!" << std::endl;
    }
    else {
        std::cout << "FAIL!" << std::endl;
        exit(1);
    }

    if (display) {
        std::cout << "=============================" << std::endl;
        std::cout << "Data size: " << data_size << "(";
        std::cout << sizeof(char) * data_size / 1024 << "K)" << std::endl;
        std::cout << "Compression Time: " << std::setprecision(3);
        std::cout << c_time * 1000 << "ms" << std::endl;
        std::cout << "Decompression Time: " << std::setprecision(3);
        std::cout << dc_time * 1000 << "ms" << std::endl;
        std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
        std::cout << (double) nbytes / (sizeof(char) * data_size) << std::endl;

    }
    
    return (double) nbytes / (sizeof(char) * data_size);
}

double TestBool::statistic(int times, bool display) {

    double result = 0;

    for (int i = 0; i < times; i++) {
        data.clear();
        std::memset(compressed, 0, data_size);
        std::memset(decompressed, 0, data_size);

        generateData();
        double c_rate = test(false);
        result = (result * i + c_rate) / (i+1);
    }

    if (display) {
        std::cout << "Experiment times: " << times << std::endl;
        showSummary(result);
    }

    return result;

}

bool TestBool::isSuccessful(){
    for (int i = 0; i < data_size; i++) {
        if (data[i] != decompressed[i]){
            return false; 
        }
    }
    return true;
}

void TestBool::showSummary(double c_rate) {
        std::cout << "=====================================================" << std::endl;
        std::cout << "Data size: " << data_size << " (" << std::fixed << std::setprecision(1);
        std::cout << (double)sizeof(char) * data_size / 1024 <<" K)" << std::endl;
        std::cout << "Rate (Probability getting true): " << std::fixed << std::setprecision(2) << rate << std::endl;
        std::cout << std::endl;
        std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
        std::cout << c_rate;
        std::cout << std::endl;
        std::cout << std::endl;
}
