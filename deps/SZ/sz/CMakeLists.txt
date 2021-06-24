add_library (SZ
  src/ArithmeticCoding.c
  src/ByteToolkit.c
  src/CacheTable.c
  src/callZlib.c
  src/CompressElement.c
  src/conf.c
  src/dataCompression.c
  src/dictionary.c
  src/DynamicByteArray.c
  src/DynamicDoubleArray.c
  src/DynamicFloatArray.c
  src/DynamicIntArray.c
  src/Huffman.c
  src/iniparser.c
  src/MultiLevelCacheTable.c
  src/MultiLevelCacheTableWideInterval.c
  src/pastri.c
  src/exafelSZ.c
  src/rw.c
  src/rwf.c
  src/sz.c
  src/szd_double.c
  src/szd_double_pwr.c
  src/szd_double_ts.c
  src/szd_float.c
  src/szd_float_pwr.c
  src/szd_float_ts.c
  src/szd_int16.c
  src/szd_int32.c
  src/szd_int64.c
  src/szd_int8.c
  src/sz_double.c
  src/sz_double_pwr.c
  src/sz_double_ts.c
  src/szd_uint16.c
  src/szd_uint32.c
  src/szd_uint64.c
  src/szd_uint8.c
  src/szf.c
  src/sz_float.c
  src/sz_float_pwr.c
  src/sz_float_ts.c
  src/sz_int16.c
  src/sz_int32.c
  src/sz_int64.c
  src/sz_int8.c
  src/sz_omp.c
  src/sz_uint16.c
  src/sz_uint32.c
  src/sz_uint64.c
  src/sz_uint8.c
  src/TightDataPointStorageD.c
  src/TightDataPointStorageF.c
  src/TightDataPointStorageI.c
  src/TypeManager.c
  src/utility.c
  src/VarSet.c
  src/sz_stats.c
)

target_include_directories(SZ 
  PUBLIC 
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/sz>
  )


target_compile_options(SZ
	PRIVATE $<$<CONFIG:Debug>:-Wall -Wextra -Wpedantic -Wno-unused-parameter>
	)

if(BUILD_PASTRI)
  target_compile_definitions(SZ PUBLIC HAVE_PASTRI)
endif()
if(BUILD_TIMECMPR)
  target_compile_definitions(SZ PUBLIC HAVE_TIMECMPR)
endif()
if(BUILD_RANDOMACCESS)
  target_compile_definitions(SZ PUBLIC HAVE_RANDOMACCESS)
endif()
if(BUILD_FORTRAN)
  enable_language(Fortran)
  target_sources(SZ PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/rw_interface.F90
    ${CMAKE_CURRENT_SOURCE_DIR}/src/sz_interface.F90
  )
endif()
if(BUILD_STATS)
  target_compile_definitions(SZ PUBLIC HAVE_WRITESTATS)
endif()
