#AM_CFLAGS = -I./include -I../zlib
#LDFLAGS=-fPIC -shared

AUTOMAKE_OPTIONS=foreign
if FORTRAN
include_HEADERS=include/MultiLevelCacheTable.h include/MultiLevelCacheTableWideInterval.h include/CacheTable.h include/defines.h\
		include/CompressElement.h include/DynamicDoubleArray.h include/rw.h include/conf.h include/dataCompression.h\
		include/dictionary.h include/DynamicFloatArray.h include/VarSet.h include/sz.h include/Huffman.h include/ByteToolkit.h include/szf.h\
		include/sz_float.h include/sz_double.h include/callZlib.h include/iniparser.h include/TypeManager.h\
		include/sz_int8.h include/sz_int16.h include/sz_int32.h include/sz_int64.h include/szd_int8.h include/szd_int16.h include/szd_int32.h include/szd_int64.h\
		include/sz_uint8.h include/sz_uint16.h include/sz_uint32.h include/sz_uint64.h include/szd_uint8.h include/szd_uint16.h include/szd_uint32.h include/szd_uint64.h\
		include/sz_float_pwr.h include/sz_double_pwr.h include/szd_float.h include/szd_double.h include/szd_float_pwr.h include/szd_double_pwr.h\
		include/sz_float_ts.h include/szd_float_ts.h include/sz_double_ts.h include/szd_double_ts.h include/utility.h include/sz_opencl.h\
		include/DynamicByteArray.h include/DynamicIntArray.h include/TightDataPointStorageI.h include/TightDataPointStorageD.h include/TightDataPointStorageF.h\
		include/pastriD.h include/pastriF.h include/pastriGeneral.h include/pastri.h include/exafelSZ.h include/ArithmeticCoding.h include/sz_omp.h include/sz_stats.h sz.mod rw.mod
lib_LTLIBRARIES=libSZ.la
libSZ_la_CFLAGS=-I./include -I../zlib/ -I../zstd/
if TIMECMPR
libSZ_la_CFLAGS+=-DHAVE_TIMECMPR
endif
if RANDOMACCESS
libSZ_la_CFLAGS+=-DHAVE_RANDOMACCESS
endif
if OPENMP
libSZ_la_CFLAGS+=-fopenmp
endif
libSZ_la_LDFLAGS = -version-info  2:1:0
libSZ_la_LIDADD=../zlib/.libs/libzlib.a ../zstd/.libs/libzstd.a
libSZ_la_SOURCES=src/MultiLevelCacheTable.c src/MultiLevelCacheTableWideInterval.c \
		src/ByteToolkit.c src/dataCompression.c src/DynamicIntArray.c src/iniparser.c src/szf.c \
		src/CompressElement.c src/DynamicByteArray.c src/rw.c src/utility.c\
		src/TightDataPointStorageI.c src/TightDataPointStorageD.c src/TightDataPointStorageF.c \
		src/conf.c src/DynamicDoubleArray.c src/rwf.c src/TypeManager.c \
		src/dictionary.c src/DynamicFloatArray.c src/VarSet.c src/callZlib.c src/Huffman.c \
		src/sz_float.c src/sz_double.c src/sz_int8.c src/sz_int16.c src/sz_int32.c src/sz_int64.c\
		src/sz_uint8.c src/sz_uint16.c src/sz_uint32.c src/sz_uint64.c src/szd_uint8.c src/szd_uint16.c src/szd_uint32.c src/szd_uint64.c\
		src/szd_float.c src/szd_double.c src/szd_int8.c src/szd_int16.c src/szd_int32.c src/szd_int64.c src/sz.c\
		src/sz_float_pwr.c src/sz_double_pwr.c src/szd_float_pwr.c src/szd_double_pwr.c src/ArithmeticCoding.c src/CacheTable.c\
		src/sz_interface.F90 src/rw_interface.F90 src/exafelSZ.c
libSZ_la_LINK=$(AM_V_CC)$(LIBTOOL) --tag=FC --mode=link $(FCLD) $(libSZ_la_CFLAGS) -O3 $(libSZ_la_LDFLAGS) -o $(lib_LTLIBRARIES)
else
include_HEADERS=include/MultiLevelCacheTable.h include/MultiLevelCacheTableWideInterval.h include/CacheTable.h include/defines.h\
		include/CompressElement.h include/DynamicDoubleArray.h include/rw.h include/conf.h include/dataCompression.h\
		include/dictionary.h include/DynamicFloatArray.h include/VarSet.h include/sz.h include/Huffman.h include/ByteToolkit.h\
		include/sz_float.h include/sz_double.h include/callZlib.h include/iniparser.h include/TypeManager.h\
		include/sz_int8.h include/sz_int16.h include/sz_int32.h include/sz_int64.h include/szd_int8.h include/szd_int16.h include/szd_int32.h include/szd_int64.h\
		include/sz_uint8.h include/sz_uint16.h include/sz_uint32.h include/sz_uint64.h include/szd_uint8.h include/szd_uint16.h include/szd_uint32.h include/szd_uint64.h\
		include/sz_float_pwr.h include/sz_double_pwr.h include/szd_float.h include/szd_double.h include/szd_float_pwr.h include/szd_double_pwr.h\
		include/sz_float_ts.h include/szd_float_ts.h include/sz_double_ts.h include/szd_double_ts.h include/utility.h include/sz_opencl.h\
		include/DynamicByteArray.h include/DynamicIntArray.h include/TightDataPointStorageI.h include/TightDataPointStorageD.h include/TightDataPointStorageF.h\
		include/pastriD.h include/pastriF.h include/pastriGeneral.h include/pastri.h include/exafelSZ.h include/ArithmeticCoding.h include/sz_omp.h include/sz_stats.h

lib_LTLIBRARIES=libSZ.la
libSZ_la_CFLAGS=-I./include -I../zlib -I../zstd/ 
if WRITESTATS
libSZ_la_CFLAGS+=-DHAVE_WRITESTATS
endif
if TIMECMPR
libSZ_la_CFLAGS+=-DHAVE_TIMECMPR
endif
if RANDOMACCESS
libSZ_la_CFLAGS+=-DHAVE_RANDOMACCESS
endif
if OPENMP
libSZ_la_CFLAGS+=-fopenmp
endif
libSZ_la_LDFLAGS = -version-info  1:4:0
libSZ_la_LIDADD=../zlib/.libs/libzlib.a ../zlib/.libs/libzstd.a
libSZ_la_SOURCES=src/MultiLevelCacheTable.c src/MultiLevelCacheTableWideInterval.c \
		src/ByteToolkit.c src/dataCompression.c src/DynamicIntArray.c src/iniparser.c\
		src/CompressElement.c src/DynamicByteArray.c src/rw.c src/utility.c\
		src/TightDataPointStorageI.c src/TightDataPointStorageD.c src/TightDataPointStorageF.c \
		src/conf.c src/DynamicDoubleArray.c src/TypeManager.c \
		src/dictionary.c src/DynamicFloatArray.c src/VarSet.c src/callZlib.c src/Huffman.c \
		src/sz_float.c src/sz_double.c src/sz_int8.c src/sz_int16.c src/sz_int32.c src/sz_int64.c\
		src/sz_uint8.c src/sz_uint16.c src/sz_uint32.c src/sz_uint64.c src/szd_uint8.c src/szd_uint16.c src/szd_uint32.c src/szd_uint64.c\
		src/szd_float.c src/szd_double.c src/szd_int8.c src/szd_int16.c src/szd_int32.c src/szd_int64.c src/sz.c\
		src/sz_float_pwr.c src/sz_double_pwr.c src/szd_float_pwr.c src/szd_double_pwr.c src/ArithmeticCoding.c src/exafelSZ.c src/CacheTable.c
if PASTRI
libSZ_la_SOURCES+=src/pastri.c
endif
if OPENMP
libSZ_la_SOURCES+=src/sz_omp.c
endif
if TIMECMPR
libSZ_la_SOURCES+=src/sz_float_ts.c src/szd_float_ts.c src/sz_double_ts.c src/szd_double_ts.c
endif
if WRITESTATS
libSZ_la_SOURCES+=src/sz_stats.c
endif

libSZ_la_LINK= $(AM_V_CC)$(LIBTOOL) --tag=CC --mode=link $(CCLD) $(libSZ_la_CFLAGS) -O3 $(libSZ_la_LDFLAGS) -o $(lib_LTLIBRARIES)
endif
