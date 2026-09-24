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

## Vendored zstd

  `zstd/` is the upstream [zstd](https://github.com/facebook/zstd) `lib/` tree, currently **v1.3.8**
  (upgraded from v1.3.5, which was in the affected range of CVE-2019-11922 / GHSA-2hff-w2r4-f7g5,
  defect 7123546794). v1.3.8 still provides the deprecated `ZBUFF_*` API used by this directory.

  Local deviations from the upstream tree, keep them when re-vendoring:

  1. `common/xxhash.c` and `common/xxhash.h` are not vendored, `XXH64` comes from `ext_xxhash`
     (`DEP_ext_xxhash` in `contrib/TSZ/CMakeLists.txt`).
  2. `common/bitstream.h`: the `memset()` in `BIT_initDStream()` carries a `(void)` cast.
  3. `BUCK`, `.gitignore`, `Makefile`, `libzstd.pc.in` and `dll/` are not vendored, the sources are
     compiled by CMake.

  Upgrade procedure:

  ```
  curl -LO https://github.com/facebook/zstd/archive/refs/tags/v<X.Y.Z>.tar.gz
  tar xzf v<X.Y.Z>.tar.gz
  cp -r zstd-<X.Y.Z>/lib/{common,compress,decompress,dictBuilder,legacy,deprecated} zstd/
  cp zstd-<X.Y.Z>/lib/zstd.h zstd/zstd.h
  rm -f zstd/common/xxhash.c zstd/common/xxhash.h     # keep using ext_xxhash
  # re-apply 2. above, then rebuild: new files are picked up by AUX_SOURCE_DIRECTORY()
  ```

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


