#include <iostream>
#include <cstdlib>
#include <iomanip>
#include <cstring>
#include <random>
#include <cmath>
#include <fstream>

#include "tsdb.h"
#include "tsCompression.h"
#include "TestDouble.h"

TestDouble::TestDouble(double A, double T, double b, double mu, double sigma, int model, int data_size) {
    this->A = A;
    this->T = T;
    this->b = b;
    this->mu = mu;
    this->sigma = sigma;
    this->model = model;
    this->data_size = data_size;
    if (model != MOD1 and model != MOD2) {
        std::cerr << "Wrong constructor!" << std::endl;
        std::exit(1);
    }

    compressed = new char[data_size * sizeof(double) + 1];
    decompressed = new double[data_size];
}

TestDouble::TestDouble(double start_value, double mu, double sigma, int data_size) {
    this->mu = mu;
    this->sigma = sigma;
    this->model = MOD3;
    this->data_size = data_size;
    this->model = MOD3;
    compressed = new char[data_size * sizeof(double) + 1];
    decompressed = new double[data_size];
}

TestDouble::TestDouble(std::string fname) {
    std::ifstream ifile(fname, std::ios::in | std::ios::binary);
    ifile.unsetf(std::ios::skipws);
    std::streampos fileSize;

    // get the file size.
    ifile.seekg(0, std::ios::end);
    fileSize = ifile.tellg();
    ifile.seekg(0, std::ios::beg);
    data.clear();

    data_size = fileSize / sizeof(double);
    data.resize(data_size);

    ifile.read(reinterpret_cast<char*>(data.data()), data_size*sizeof(double));

    compressed = new char[data_size * sizeof(double) + 1];
    decompressed = new double[data_size];
}


TestDouble::~TestDouble() {
    delete [] compressed;
    delete [] decompressed;
}

void TestDouble::generateData() {
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

        // std::random_device rd;
        // std::mt19937 gen(rd());
        // std::normal_distribution<> nrand(mu,sigma);

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

void TestDouble::writeData(std::string fname) {
    std::ofstream ofile(fname, std::ios::out | std::ios::binary);
    ofile.write((char *)data.data(), sizeof(double) * data.size());
    ofile.close();
}

double TestDouble::test(int times, bool display) {
    
    clock_t c_begin, c_end;
    float c_time = 0;
    clock_t dc_begin, dc_end;
    float dc_time = 0;
    int nbytes = 0;

    for (int i = 0; i < times; i++) {
        // compress the data.
        c_begin = clock();
        nbytes = tsCompressDouble(data.data(), data_size, compressed);
        c_end = clock();
        c_time = (i * c_time + (float)(c_end-c_begin)/CLOCKS_PER_SEC)/(i+1);
        // decompress the data.
        dc_begin = clock();
        tsDecompressDouble(compressed, data_size, decompressed);
        dc_end = clock();
        dc_time = (i * dc_time + (float)(dc_end-dc_begin)/CLOCKS_PER_SEC)/(i+1);
    }

    if (not isSuccessful()) {
        std::cout << "FAIL!" << std::endl;
        exit(1);
    }

    if(display) {
        std::cout << "=============================" << std::endl;
        std::cout << "Data size: " << data_size << "(";
        std::cout << sizeof(double) * data_size / 1024 << "K)" << std::endl;
        std::cout << "Compression Time: " << std::setprecision(3);
        std::cout << c_time * 1000 << "ms" << std::endl;
        std::cout << "Decompression Time: " << std::setprecision(3);
        std::cout << dc_time * 1000 << "ms" << std::endl;
        std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
        std::cout << (double) nbytes / (sizeof(double) * data_size) << std::endl;

    }

    return (double) nbytes / (sizeof(double) * data_size);
}

double TestDouble::statistic(int times, bool display) {

    double result = 0;

    for (int i = 0; i < times; i++) {
        data.clear();
        std::memset(compressed, 0, data_size * sizeof(double));
        std::memset(decompressed, 0, data_size * sizeof(double));
        // generate data
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

bool TestDouble::isSuccessful() {
    for (int i = 0; i < data_size; i++) {
        if (data[i] != decompressed[i])
            return false; 
    }
    return true;
}

void TestDouble::showSummary(double c_rate){
    std::cout << "=====================================================" << std::endl;
    std::cout << "Data size: " << data_size << " (" << std::fixed << std::setprecision(1);
    std::cout << (double)sizeof(double) * data_size / 1024 <<" K)" << std::endl;
    if (model == MOD1 or model == MOD2) {
        std::cout << "Amplitude: " << A << " Period: " << T << " Baseline: " << b << std::endl;
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
