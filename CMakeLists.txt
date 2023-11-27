cmake_minimum_required(VERSION 3.15)
project(ParlaySemisort)
include (FetchContent)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

FetchContent_Declare(parlaylib
  GIT_REPOSITORY  https://github.com/cmuparlay/parlaylib.git
  GIT_TAG         master
)
FetchContent_GetProperties(parlaylib)
if(NOT parlaylib_POPULATED)
  FetchContent_Populate(parlaylib)  
  add_subdirectory(${parlaylib_SOURCE_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

FetchContent_Declare(
  xxhash
  GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
  GIT_TAG        v0.8.0
)
FetchContent_MakeAvailable(xxHash)
add_subdirectory(
  "${xxhash_SOURCE_DIR}/cmake_unofficial"
  "${xxhash_BINARY_DIR}"
  EXCLUDE_FROM_ALL
)

add_executable(semisort_tests
  test/test_runner.cpp
  test/semisort_tests.cpp
  src/semisort.cpp
)
target_link_libraries(semisort_tests PRIVATE parlay GTest::gtest_main xxhash)
