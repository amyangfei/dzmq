cmake_minimum_required(VERSION 2.8)
project(dzmq)

set(PROJECT_MAJOR_VERSION 0)
set(PROJECT_MINOR_VERSION 1)
set(PROJECT_PATCH_VERSION 1)

set (PROJECT_VERSION ${PROJECT_MAJOR_VERSION}.${PROJECT_MINOR_VERSION}.${PROJECT_PATCH_VERSION})
set (CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/CMakeModules)

set (CMAKE_INCLUDE_CURRENT_DIR ON)

INCLUDE (CheckFunctionExists)
INCLUDE (CheckIncludeFiles)
INCLUDE (CheckTypeSize)

CHECK_FUNCTION_EXISTS(alloca  C_ALLOCA)
CHECK_FUNCTION_EXISTS(memcmp  HAVE_MEMCMP)
CHECK_FUNCTION_EXISTS(strndup HAVE_STRNDUP)
CHECK_FUNCTION_EXISTS(strnlen HAVE_STRNLEN)

CHECK_TYPE_SIZE("int" SIZEOF_INT)
CHECK_TYPE_SIZE("long" SIZEOF_LONG)
CHECK_TYPE_SIZE("short" SIZEOF_SHORT)

SET(CMAKE_INCLUDE_CURRENT_DIR ON)

include(BaseConfig)

find_package(libzmq REQUIRED)
find_package(libczmq REQUIRED)

set(DZMQ_EXTERNAL_LIBS
    ${LIBZMQ_LIBRARY}
    ${LIBCZMQ_LIBRARY}
)

# aux_source_directory (${PROJECT_SOURCE_DIR} DZMQ_SOURCES)

OPTION(DEFINE_DEBUG
  "Build the project using debugging code"
  OFF)
IF(DEFINE_DEBUG)
  MESSAGE("Adding Debug flag...")
  ADD_DEFINITIONS(-D_DEBUG)
  SET(CMAKE_BUILD_TYPE Debug)
  MESSAGE("Build type is " ${CMAKE_BUILD_TYPE})
ENDIF(DEFINE_DEBUG)

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DDZMQG_VERSION=${PROJECT_VERSION}")

message("Build Type: ${CMAKE_BUILD_TYPE}")
message("Std CFLAGS: ${CMAKE_C_FLAGS}")

set(CMAKE_EXE_LINKER_FLAGS "-L/usr/lib -L/usr/local/lib")

file(GLOB LIB_SOURCES *.c)
file(GLOB LIB_HEADERS *.h)
list(REMOVE_ITEM LIB_SOURCES "${PROJECT_SOURCE_DIR}/dzmq-benchmark.c")
list(REMOVE_ITEM LIB_SOURCES "${PROJECT_SOURCE_DIR}/dzmq-server.c")
add_library(dzmq ${LIB_SOURCES} ${LIB_HEADERS})

file(GLOB MAIN_SOURCES dzmq-benchmark.c dzmq-server.c)

foreach(main_source ${MAIN_SOURCES})
    string(REPLACE ".c" "" main_source_path ${main_source})
    get_filename_component(main_source_name ${main_source_path} NAME)
    add_executable(${main_source_name} ${main_source})
    target_link_libraries(${main_source_name} dzmq)
    target_link_libraries(${main_source_name} ${DZMQ_EXTERNAL_LIBS})
endforeach(main_source ${MAIN_SOURCES})
