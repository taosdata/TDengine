#include <iostream>
#include <cstdlib>
#include <iomanip>
#include <cstring>
#include <random>
#include <fstream>
#include <cmath>
#include <time.h>
#include <typeinfo>

#include "tsdb.h"
#include "TestInt.h"
#include "tsCompression.h"

template <typename T> TestInt<T>::TestInt(int a, int b, int c, int sigma, int model, int data_size) {
    this->a = a;
    this->b = b;
    this->c = c;
    this->sigma = sigma;
    this->model = model;
    this->data_size = data_size;
    compressed = new char[data_size * sizeof(T) + 1];
    decompressed = new T[data_size];
}

template <typename T> TestInt<T>::TestInt(std::string fname) {

    std::ifstream ifile(fname, std::ios::in | std::ios::binary);
    ifile.unsetf(std::ios::skipws);
    std::streampos fileSize;

    // get the file size.
    ifile.seekg(0, std::ios::end);
    fileSize = ifile.tellg();
    ifile.seekg(0, std::ios::beg);
    data.clear();

    data_size = fileSize / sizeof(T);
    data.resize(data_size);

    ifile.read(reinterpret_cast<char*>(data.data()), data_size*sizeof(T));

    compressed = new char[sizeof(T)*data_size + 1];
    decompressed = new T[data_size];

}

template <typename T> TestInt<T>::~TestInt() {
    delete [] compressed;
    delete [] decompressed;
}

template <typename T> void TestInt<T>::generateData() {
    std::srand((unsigned)time(0));

    if (model == MOD1) {
        for (int i = 0; i < data_size; i++) {
            if (sigma != 0)
                data.push_back(a * i + b + std::rand() % (2*sigma) - sigma );
            else
                data.push_back(a * i + b);
        }
    }
    else if (model == MOD2) {
        for (int i = 0; i < data_size; i++) {
            if (sigma != 0)
                data.push_back(a * i*i + b*i + c + std::rand() % (2*sigma) - sigma );
            else
                data.push_back(a * i*i + b*i + c);
        }
    }
    else{
        std::cerr << "Wrong model" << std::endl;
        exit(1);
    }
}

template <typename T> void TestInt<T>::writeData(std::string fname) {
    int bytes_per_ele = 0;
    if (typeid(T) == typeid(long)){
        bytes_per_ele = sizeof(long);
    }
    else if (typeid(T) == typeid(int)){
        bytes_per_ele = sizeof(int);
    }
    else if (typeid(T) == typeid(short)){
        bytes_per_ele = sizeof(short);
    }
    else if (typeid(T) == typeid(char)) {
        bytes_per_ele = sizeof(char);
    }
    else {
        std::cerr << "Wrong data type." << std::endl;
    }

    std::ofstream ofile(fname, std::ios::out | std::ios::binary);
    ofile.write((char *)data.data(), bytes_per_ele * data_size);
    ofile.close();
}

template <typename T> double TestInt<T>::test(int times, bool display) {
    // compress the data.
    clock_t c_begin, c_end;
    float c_time = 0;
    clock_t dc_begin, dc_end;
    float dc_time = 0;
    int nbytes = 0;
    for (int i = 0; i < times; i++) {
        if (typeid(T) == typeid(long)) {
            c_begin = clock();
            nbytes = tsCompressINT(data.data(), data_size, compressed, TSDB_DATA_TYPE_BIGINT);
            c_end = clock();
            c_time = (i * c_time + (float)(c_end-c_begin)/CLOCKS_PER_SEC)/(i+1);
            dc_begin = clock();
            tsDecompressINT(compressed, data_size, decompressed, TSDB_DATA_TYPE_BIGINT);
            dc_end = clock();
            dc_time = (i * dc_time + (float)(dc_end-dc_begin)/CLOCKS_PER_SEC)/(i+1);
        }
        else if (typeid(T) == typeid(int)) {
            c_begin = clock();
            nbytes = tsCompressINT(data.data(), data_size, compressed, TSDB_DATA_TYPE_INT);
            c_end = clock();
            c_time = (i * c_time + (float)(c_end-c_begin)/CLOCKS_PER_SEC)/(i+1);
            dc_begin = clock();
            tsDecompressINT(compressed, data_size, decompressed, TSDB_DATA_TYPE_INT);
            dc_end = clock();
            dc_time = (i * dc_time + (float)(dc_end-dc_begin)/CLOCKS_PER_SEC)/(i+1);
        }
        else if (typeid(T) == typeid(short)) {
            c_begin = clock();
            nbytes = tsCompressINT(data.data(), data_size, compressed, TSDB_DATA_TYPE_SMALLINT);
            c_end = clock();
            c_time = (i * c_time + (float)(c_end-c_begin)/CLOCKS_PER_SEC)/(i+1);
            dc_begin = clock();
            tsDecompressINT(compressed, data_size, decompressed, TSDB_DATA_TYPE_SMALLINT);
            dc_end = clock();
            dc_time = (i * dc_time + (float)(dc_end-dc_begin)/CLOCKS_PER_SEC)/(i+1);
        }
        else if (typeid(T) == typeid(char)) {
            c_begin = clock();
            nbytes = tsCompressINT(data.data(), data_size, compressed, TSDB_DATA_TYPE_TINYINT);
            c_end = clock();
            c_time = (i * c_time + (float)(c_end-c_begin)/CLOCKS_PER_SEC)/(i+1);
            dc_begin = clock();
            tsDecompressINT(compressed, data_size, decompressed, TSDB_DATA_TYPE_TINYINT);
            dc_end = clock();
            dc_time = (i * dc_time + (float)(dc_end-dc_begin)/CLOCKS_PER_SEC)/(i+1);
        }
        else {
            std::cerr << "Wrong type!" << std::endl;
            exit(1);
        }
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
        std::cout << sizeof(T) * data_size / 1024 << "K)" << std::endl;
        std::cout << "Compression Time: " << std::setprecision(3);
        std::cout << c_time * 1000 << "ms" << std::endl;
        std::cout << "Decompression Time: " << std::setprecision(3);
        std::cout << dc_time * 1000 << "ms" << std::endl;
        std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
        std::cout << (double) nbytes / (sizeof(T) * data_size) << std::endl;
    }

    return (double) nbytes / (sizeof(T) * data_size);

}

template <typename T> double TestInt<T>::statistic(int times, bool display) {

    double result = 0;

    for (int i = 0; i < times; i++) {
        data.clear();
        std::memset(compressed, 0, data_size * sizeof(T));
        std::memset(decompressed, 0, data_size * sizeof(T));
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

template <typename T> bool TestInt<T>::isSuccessful() {
    for (int i = 0; i < data_size; i++) {
        if (data[i] != decompressed[i])
           return false; 
    }
    return true;
}

template <typename T> void TestInt<T>::showSummary(double c_rate) {
    std::cout << "=====================================================" << std::endl;
    std::cout << "Data size: " << data_size << " (" << std::fixed << std::setprecision(1);
    std::cout << (double)sizeof(T) * data_size / 1024 <<" K)" << std::endl;
    std::cout << "a: " << a <<std::endl;
    std::cout << "b: " << b <<std::endl;
    if (model == MOD2){
        std::cout << "c: " << c <<std::endl;
    }
    std::cout << "sigma: " << sigma << std::endl;
    std::cout << "Compression rate: " << std::fixed << std::setprecision(3);
    std::cout << c_rate;
    std::cout << std::endl;
    std::cout << std::endl;

}
