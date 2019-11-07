#include <iostream>
#include <cstdlib>
#include <iomanip>
#include <cstring>
#include <random>
#include <cmath>
#include <fstream>
#include <time.h>

#include "tsdb.h"
#include "tsCompression.h"
#include "TestFloat.h"

TestFloat::TestFloat(float A, float T, float b, float mu, float sigma, int model, int data_size) {
    this->A = A;
    this->T = T;
    this->b = b;
    this->mu = mu;
    this->sigma = sigma;
    this->model = model;
    this->data_size = data_size;
    if (model != MOD1 and model != MOD2) {
        std::cout << "Wrong model!" << std::endl;
    }
    compressed = new char[data_size * sizeof(float) + 1];
    decompressed = new float[data_size];
}

TestFloat::TestFloat(float start_value, float mu, float sigma, int data_size) {
    this->mu = mu;
    this->sigma = sigma;
    this->model = MOD3;
    this->data_size = data_size;
    compressed = new char[data_size * sizeof(float) + 1];
    decompressed = new float[data_size];
}

TestFloat::TestFloat(std::string fname) {
    std::ifstream ifile(fname, std::ios::in | std::ios::binary);
    ifile.unsetf(std::ios::skipws);
    std::streampos fileSize;

    // get the file size.
    ifile.seekg(0, std::ios::end);
    fileSize = ifile.tellg();
    ifile.seekg(0, std::ios::beg);
    data.clear();

    data_size = fileSize / sizeof(float);
    data.resize(data_size);

    ifile.read(reinterpret_cast<char*>(data.data()), data_size*sizeof(float));

    compressed = new char[data_size * sizeof(float) + 1];
    decompressed = new float[data_size];

}


TestFloat::~TestFloat() {
    delete [] compressed;
    delete [] decompressed;
}

void TestFloat::generateData() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<> nrand(mu,sigma);

    if (model == MOD1) {
        // sine wave generator.
        for(int i = 0; i < data_size; i++) {
            data.push_back(A * std::sin(2 * PI * i / T) + b + nrand(gen));
        }
    }
    else if(model == MOD2) {
        // rectangular wave generator.
        for (int i = 0; i < data_size; i++) {
            if (std::sin(2 * PI * i / T < 0)) {
                data.push_back(-A + b + nrand(gen));
            }
            else {
                data.push_back(A + b + nrand(gen));
            }
        }
    }
    else if (model == MOD3) {
        int counter = 0;
        data.push_back(start_value);
        counter++;

        std::random_device rd;
        std::mt19937 gen(rd());
        std::normal_distribution<> nrand(mu,sigma);

        for (;counter < data_size; counter++) {
            start_value += nrand(gen);
            data.push_back(start_value);
        }

    }
    else {
        std::cerr << "Wrong model type!" << std::endl;
        exit(1);
    }
}

void TestFloat::writeData(std::string fname) {
    std::ofstream ofile(fname, std::ios::out | std::ios::binary);
    ofile.write((char *)data.data(), sizeof(float) * data.size());
    ofile.close();
}

float TestFloat::test(int times, bool display) {
    // compress the data.
    clock_t c_begin, c_end;
    float c_time;
    clock_t dc_begin, dc_end;
    float dc_time;
    int nbytes = 0;

    for (int i = 0; i < times; i++){
        c_begin = clock();
        nbytes = tsCompressFloat(data.data(), data_size, compressed);
        c_end = clock();
        c_time = (i * c_time + (float)(c_end-c_begin)/CLOCKS_PER_SEC)/(i+1);
        // decompress the data.
        tsDecompressFloat(compressed, data_size, decompressed);
        dc_time = (i * dc_time + (float)(dc_end-dc_begin)/CLOCKS_PER_SEC)/(i+1);
    }

    if (not isSuccessful()) {
        std::cout << "FAIL!" << std::endl;
        exit(1);
    }

    if(display) {
        std::cout << "=============================" << std::endl;
        std::cout << "Data size: " << data_size << "(";
        std::cout << sizeof(float) * data_size / 1024 << "K)" << std::endl;
        std::cout << "Compression Time: " << std::setprecision(3);
        std::cout << c_time * 1000 << "ms" << std::endl;
        std::cout << "Decompression Time: " << std::setprecision(3);
        std::cout << dc_time * 1000 << "ms" << std::endl;
        std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
        std::cout << (double) nbytes / (sizeof(float) * data_size) << std::endl;

    }

    return (float) nbytes / (sizeof(float) * data_size);
}

float TestFloat::statistic(int times, bool display) {

    float result = 0;

    for (int i = 0; i < times; i++) {
        data.clear();
        std::memset(compressed, 0, data_size * sizeof(float));
        std::memset(decompressed, 0, data_size * sizeof(float));
        // generate data
        generateData();

        float c_rate = test(false);
        result = (result * i + c_rate) / (i+1);
    }

    if (display) {
        std::cout << "Experiment times: " << times << std::endl;
        showSummary(result);
    }

    return result;
}

bool TestFloat::isSuccessful() {
    for (int i = 0; i < data_size; i++) {
        if (data[i] != decompressed[i])
            return false; 
    }
    return true;
}

void TestFloat::showSummary(float c_rate){
    std::cout << "=====================================================" << std::endl;
    std::cout << "Data size: " << data_size << " (" << std::fixed << std::setprecision(1);
    std::cout << (float)sizeof(float) * data_size / 1024 <<" K)" << std::endl;
    if (model == MOD1 or model == MOD2) {
        std::cout << "Amplitude: " << A << "Period: " << T << "Baseline: " << b << std::endl;
    }
    else {
        std::cout << "Baseline: " << std::endl;
    }
    std::cout << "mu: " << mu << "    " << "sigma: " << sigma << std::endl;
    std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
    std::cout << c_rate;
    std::cout << std::endl;
    std::cout << std::endl;
}
