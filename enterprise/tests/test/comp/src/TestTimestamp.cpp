#include <iostream>
#include <ctime>
#include <vector>
#include <random>
#include <iomanip>
#include <cstdlib>
#include <cstring>
#include <string>
#include <fstream>
#include <time.h>
#include <sys/time.h>

#include "tsdb.h"
#include "TestTimestamp.h"
#include "tsCompression.h"

TestTimestamp::TestTimestamp(int period, double mu, double sigma, int data_size,
        double missing_rate){
    this->period = period;
    this->mu = mu;
    this->sigma = sigma;
    this->data_size = data_size;
    this->missing_rate = missing_rate;
    compressed = new char[sizeof(unsigned long)*data_size + 1];
    decompressed = new unsigned long[data_size];
}

TestTimestamp::TestTimestamp(std::string fname) {

    std::ifstream ifile(fname, std::ios::in | std::ios::binary);
    ifile.unsetf(std::ios::skipws);
    std::streampos fileSize;

    // get the file size.
    ifile.seekg(0, std::ios::end);
    fileSize = ifile.tellg();
    ifile.seekg(0, std::ios::beg);
    data.clear();

    data_size = fileSize / sizeof(long);
    data.resize(data_size);

    ifile.read(reinterpret_cast<char*>(data.data()), data_size*sizeof(long));

    compressed = new char[sizeof(unsigned long)*data_size + 1];
    decompressed = new unsigned long[data_size];
}

TestTimestamp::~TestTimestamp() {
    delete [] compressed;
    delete [] decompressed;
}

void TestTimestamp::generateData(){
    int counter = 0;

    // get current time in milliseconds.
    struct timeval tp;
    gettimeofday(&tp, NULL);
    unsigned long ms = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    data.push_back(ms);
    counter++;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<> nrand(mu,sigma);

    do {
        ms += (period + nrand(gen)) * 1000;
        // Decide if this data is sampled by our database.
        std::srand((unsigned)time(0));
        if ((double)std::rand() / RAND_MAX > missing_rate) {
            data.push_back(ms);
            counter++;
        }
    } while (counter < data_size);
}

void TestTimestamp::writeData(std::string fname) {
    std::ofstream ofile(fname, std::ios::out | std::ios::binary);
    ofile.write((char *)data.data(), sizeof(long) * data.size());
    ofile.close();
}

double TestTimestamp::test(int times, bool display) {
    // compress the data.
    clock_t c_begin, c_end;
    float c_time = 0;
    clock_t dc_begin, dc_end;
    float dc_time = 0;
    int nbytes = 0;


    for (int i = 0; i < times; i++) {
        c_begin = clock();
        nbytes = tsCompressTimestamp(data.data(), data_size, compressed);
        c_end = clock();
        c_time = (i * c_time + (float)(c_end-c_begin)/CLOCKS_PER_SEC)/(i+1);

        dc_begin = clock();
        tsDecompressTimestamp(compressed, data_size, decompressed);
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

    if(display) {
        std::cout << "=============================" << std::endl;
        std::cout << "Data size: " << data_size << "(";
        std::cout << sizeof(long) * data_size / 1024 << "K)" << std::endl;
        std::cout << "Compression Time: " << std::setprecision(3);
        std::cout << c_time * 1000 << "ms" << std::endl;
        std::cout << "Decompression Time: " << std::setprecision(3);
        std::cout << dc_time * 1000 << "ms" << std::endl;
        std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
        std::cout << (double) nbytes / (sizeof(unsigned long) * data_size) << std::endl;
    }

    return (double) nbytes / (sizeof(unsigned long) * data_size);
}

double TestTimestamp::statistic(int times, bool display) {

    double result = 0;

    for (int i = 0; i < times; i++) {
        data.clear();
        std::memset(compressed, 0, data_size * sizeof(unsigned long));
        std::memset(decompressed, 0, data_size * sizeof(unsigned long));
        // generate data.
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

bool TestTimestamp::isSuccessful(){
    for (int i = 0; i < data_size; i++) {
        if (data[i] != decompressed[i])
           return false; 
    }
    return true;
}

void TestTimestamp::showSummary(double c_rate){
        std::cout << "=====================================================" << std::endl;
        std::cout << "Data size: " << data_size << " (" << std::fixed << std::setprecision(1);
        std::cout << (double)sizeof(unsigned long) * data_size / 1024 <<" K)" << std::endl;
        std::cout << "Period: " << period << "s" << std::endl; 
        std::cout << "mu: " << mu << "    " << "sigma: " << sigma << std::endl;
        std::cout << "Missing rate: " << std::fixed << std::setprecision(2) << missing_rate << std::endl;
        std::cout << std::endl;
        std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
        std::cout << c_rate;
        std::cout << std::endl;
        std::cout << std::endl;
}
