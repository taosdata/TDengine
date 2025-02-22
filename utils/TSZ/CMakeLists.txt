if(NOT ${FALLBACK})           # {
    CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
    PROJECT(TDengine)
endif()                   # }

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
TARGET_INCLUDE_DIRECTORIES(TSZ PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/sz/inc ${TD_SOURCE_DIR}/include)

if(NOT ${FALLBACK})          # {
    DEP_ext_lzma2(TSZ)
endif()                      # }

# windows ignore warning
IF(TD_WINDOWS)
    if(${FALLBACK})          # {
        SET_TARGET_PROPERTIES(TSZ PROPERTIES COMPILE_FLAGS -w)
    endif()                  # }
ENDIF()
