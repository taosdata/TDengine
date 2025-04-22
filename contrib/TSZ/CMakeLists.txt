IF(NOT "${TSZ_ENABLED}" MATCHES "false")
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
  DEP_ext_xxhash(TSZ)
  TARGET_INCLUDE_DIRECTORIES(TSZ
    PUBLIC
      sz/inc
      zstd
      zstd/common
      ${TD_SOURCE_DIR}/include
  )

  # windows ignore warning
  IF(TD_WINDOWS)
      SET_TARGET_PROPERTIES(TSZ PROPERTIES COMPILE_FLAGS -w)
  ENDIF()
ENDIF()
