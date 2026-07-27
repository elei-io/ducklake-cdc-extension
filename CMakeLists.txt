cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ducklake_cdc)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME} VERSION 0.6.3)

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/ducklake_cdc_extension.cpp
    src/compat_check.cpp
    src/ducklake_metadata.cpp
    src/consumer.cpp
    src/ddl.cpp
    src/dml.cpp
    src/stats.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_compile_definitions(${EXTENSION_NAME} PRIVATE DUCKLAKE_CDC_SEMVER="${PROJECT_VERSION}")
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE DUCKLAKE_CDC_SEMVER="${PROJECT_VERSION}")

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
