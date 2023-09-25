# TSZ
Error-bounded Lossy Data Compressor For Float/Double 

TSZ algorithm is come from SZ algorithm, Github url is  https://github.com/szcompressor.

Bellow is aspect of improvement :
  1) Better speed and size
  
     SZ head size about 24 bytes, we are reduced to 2 bytes.
     we delete some no use code and some unnecessary function could be droped.
  2) Support multi-threads, interface is thread-safety.
  3) Remove 2D 3D 4D 5D function, only 1D be remained.
  4) Remove int8 int16 int32 and other datatype, only float double be remained.
  5) Optimize code speed
  6) Other optimize...
  
After modify, TSZ become faster、smaller and independent.  TSZ more suitable for small block data compression.

## ADT-FSE
  ADT-FSE is an optimization algorithm of TSZ based on timeseries database scenario, which can provide higher compression rate and faster compression and decompression speed for small files. 

  Usage:
    By default, ADT-FSE algorithm is enabled. Control it by `ifAdtFse` configuration.

  Citation: 
  ```
  @inproceedings{
    adtfse_sc2023,
    author = {Tao Lu, Yu Zhong, Zibin Sun, Xiang Chen, You Zhou, Fei Wu, Ying Yang, Yunxin Huang, and Yafei Yang},
    title = {ADT-FSE: A New Encoder for SZ},
    year = {2023},
    publisher = {IEEE Press},
    booktitle = {Proceedings of the International Conference on High Performance Computing, Networking, Storage and Analysis},
    location = {Denver, Colorado},
    series = {SC'23}
  }
  ```


