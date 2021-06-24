add_library(ZLIB 
  ./gzclose.c
  ./uncompr.c
  ./trees.c
  ./gzread.c
  ./adler32.c
  ./gzwrite.c
  ./compress.c
  ./inftrees.c
  ./crc32.c
  ./inffast.c
  ./zutil.c
  ./gzlib.c
  ./infback.c
  ./inflate.c
  ./deflate.c
  )

target_include_directories(ZLIB
  PUBLIC 
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  )


