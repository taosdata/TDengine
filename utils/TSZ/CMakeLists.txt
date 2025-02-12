PROJECT(TDengine)

# include
INCLUDE_DIRECTORIES(sz/inc)
INCLUDE_DIRECTORIES(zstd/)
INCLUDE_DIRECTORIES(zstd/common/)

# source
AUX_SOURCE_DIRECTORY(sz/src SRC1)
AUX_SOURCE_DIRECTORY(zstd/dictBuilder SRC2)
AUX_SOURCE_DIRECTORY(zstd/common SRC3)
AUX_SOURCE_DIRECTORY(zstd/compress SRC4)
AUX_SOURCE_DIRECTORY(zstd/decompress SRC5)
AUX_SOURCE_DIRECTORY(zstd/deprecated SRC6)
AUX_SOURCE_DIRECTORY(zstd/legacy SRC7)

# archive
ADD_LIBRARY(TSZ STATIC ${SRC1} ${SRC2} ${SRC3} ${SRC4} ${SRC5} ${SRC6} ${SRC7})

if(${TD_LINUX})
  add_dependencies(TSZ ext_lzma2)
  target_include_directories(TSZ PUBLIC "${TD_CONTRIB_EXT_LZMA2_INC_DIR}")
  target_link_directories(TSZ PUBLIC "${TD_CONTRIB_EXT_LZMA2_LIB_DIR}")
  target_link_libraries(TSZ PUBLIC fast-lzma2.a)
endif()

TARGET_INCLUDE_DIRECTORIES(TSZ PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/sz/inc ${TD_SOURCE_DIR}/include)

# windows ignore warning
IF(TD_WINDOWS)
  SET_TARGET_PROPERTIES(TSZ PROPERTIES COMPILE_FLAGS -w)
ENDIF()
