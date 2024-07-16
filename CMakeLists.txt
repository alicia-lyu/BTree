cmake_minimum_required(VERSION 3.16)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_SOURCE_DIR}/cmake/modules")
set(MAJOR "0")
set(MINOR "0")
set(PATCH "1")

cmake_policy(SET CMP0048 NEW)
project(BTree VERSION ${MAJOR}.${MINOR}.${PATCH})

option(FORCE_USE_SIMD "force define FC_USE_SIMD (applicable only for x86_64)" OFF)
option(FORCE_PREFER_BINARY_SEARCH "force define FC_PREFER_BINARY_SEARCH (recommended for clang only)" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include Homebrew directories for macOS
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    if (EXISTS "/opt/homebrew")
        list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew/include")
    else()
        list(APPEND CMAKE_PREFIX_PATH "/usr/local/include")
    endif()
endif()

find_package(VersionHeader)
set(PROJECT_VERSION "${MAJOR}.${MINOR}.${PATCH}.${COMMITTER_SHORTSHA}")
file(WRITE ${CMAKE_SOURCE_DIR}/VERSION.txt "${MAJOR}.${MINOR}.${PATCH}")

find_package(Catch2 3 QUIET)
set(Catch_VER 3)
if (NOT Catch2_FOUND)
    find_package(Catch2 REQUIRED)
    set(Catch_VER 2)
endif()
set(CATCH_LIBS_ALIASES Catch2::Catch2 Catch2::Catch2WithMain)

message(STATUS "Catch2 found: ${Catch2_FOUND}")
message(STATUS "Catch2 version: ${Catch_VER}")

function(get_linux_lsb_release_information)
    find_program(LSB_RELEASE_EXEC lsb_release)
    if(NOT LSB_RELEASE_EXEC)
        message(FATAL_ERROR "Could not detect lsb_release executable, can not gather required information")
    endif()
    execute_process(COMMAND "${LSB_RELEASE_EXEC}" --short --id OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND "${LSB_RELEASE_EXEC}" --short --release OUTPUT_VARIABLE LSB_RELEASE_VERSION_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND "${LSB_RELEASE_EXEC}" --short --codename OUTPUT_VARIABLE LSB_RELEASE_CODENAME_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)

    set(LSB_RELEASE_ID_SHORT "${LSB_RELEASE_ID_SHORT}" PARENT_SCOPE)
    set(LSB_RELEASE_VERSION_SHORT "${LSB_RELEASE_VERSION_SHORT}" PARENT_SCOPE)
    set(LSB_RELEASE_CODENAME_SHORT "${LSB_RELEASE_CODENAME_SHORT}" PARENT_SCOPE)
endfunction()

message(STATUS "COMMITTER_FULLSHA ${COMMITTER_FULLSHA}")
message(STATUS "COMMITTER_SHORTSHA ${COMMITTER_SHORTSHA}")
message(STATUS "COMMITTER_DATE ${COMMITTER_DATE}")
message(STATUS "PROJECT_VERSION ${PROJECT_VERSION}")

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
    get_linux_lsb_release_information()
    message(STATUS "OS Linux ${LSB_RELEASE_ID_SHORT} ${LSB_RELEASE_VERSION_SHORT} ${LSB_RELEASE_CODENAME_SHORT}")
else()
    message(STATUS "OS ${CMAKE_SYSTEM_NAME}")
    message(STATUS "OS VERSION ${CMAKE_SYSTEM_VERSION}")
endif()

file(GLOB_RECURSE FC_HEADERS include/fc/*.h)

add_library(BTree INTERFACE ${FC_HEADERS})

file(GLOB_RECURSE DB_HEADERS include/db/*.h)

add_library(DBBTree INTERFACE ${DB_HEADERS})

foreach(target IN LISTS BTree DBBTree)
    target_include_directories(${target} INTERFACE
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    )
endforeach()

add_library(BTree::BTree ALIAS BTree)
add_library(BTree::DBBTree ALIAS DBBTree)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR FORCE_PREFER_BINARY_SEARCH)
    add_definitions(-DFC_PREFER_BINARY_SEARCH=1)
endif()

if (FORCE_USE_SIMD)
    add_definitions(-DFC_USE_SIMD=1)
endif()

add_library(catch_prereqs OBJECT
    test/test_statistics.h
    test/test_statistics.cpp
    test/fc_catch2.h
)

target_link_libraries(catch_prereqs PUBLIC ${CATCH_LIBS_ALIASES})

add_executable(fc_tests
    test/unittest.cpp
    test/rwtest.cpp
)

add_executable(fc_benchmarks
    test/perftest.cpp
    test/perftest_no_simd.cpp
    test/perftest_string.cpp
)

# add_executable(db_tests
#     test/dbtest_fixed_page.cpp
# )

# add_executable(dbtests_manual_fixed_page
#     test/dbtest_fixed_page_manual.cpp
# )

add_executable(dbtests_manual_page_node
    test/dbtest_page_node_manual.cpp
)

add_executable(dbtests_manual_dbbtree
    test/dbtest_dbbtree_manual.cpp
)

set(CATCH2_EXECUTABLES fc_tests fc_benchmarks)
set(MANUAL_EXECUTABLES dbtests_manual_page_node dbtests_manual_dbbtree)
set(DB_EXECUTABLES dbtests_manual_page_node dbtests_manual_dbbtree)
set(TEST_EXECUTABLES ${CATCH2_EXECUTABLES} ${MANUAL_EXECUTABLES})
message(STATUS "TEST_EXECUTABLES: ${TEST_EXECUTABLES}")

if (${Catch_VER} EQUAL 2)
    message(STATUS "Used old target for catch2")
    if (NOT TARGET Catch2::Catch2WithMain)
        set(CATCH_LIBS_ALIASES Catch2::Catch2)
    endif()
    foreach(target ${CATCH2_EXECUTABLES})
        target_compile_definitions(${target} PRIVATE CATCH2_OLD)
    endforeach()
endif()

foreach(target ${TEST_EXECUTABLES})
    target_include_directories(${target} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/include)
    target_link_libraries(${target} PRIVATE BTree::BTree)
endforeach()

foreach(target ${CATCH2_EXECUTABLES})
    target_link_libraries(${target} PRIVATE catch_prereqs)
endforeach()

foreach(target ${DB_EXECUTABLES})
    target_link_libraries(${target} PRIVATE BTree::DBBTree)
endforeach()

foreach(target ${TEST_EXECUTABLES})
    get_target_property(INC_DIRS ${target} INCLUDE_DIRECTORIES)
    message(STATUS "Include directories for target ${target}: ${INC_DIRS}")
    get_target_property(LINK_LIBS ${target} LINK_LIBRARIES)
    message(STATUS "Link libraries for target ${target}: ${LINK_LIBS}")
endforeach()


if(MSVC)
    set(compile_options /W4 /WX /nologo /MDd /EHsc /std:c++latest /experimental:module)
else()
    set(compile_options -Wall -Wextra -Wpedantic -Werror -march=native -mtune=generic 
    )
    if (FORCE_USE_SIMD)
        list(APPEND compile_options -mavx512f)
    endif()
    if((CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS_EQUAL 14.0.3.14030022) OR
       (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS_EQUAL 14))
        set(link_options -fexperimental-library)
    endif()
endif()

foreach(target ${TEST_EXECUTABLES})
    target_compile_options(${target} PRIVATE ${compile_options})
    if (link_options)
        target_link_options(${target} PRIVATE ${link_options})
    endif()
endforeach()

if(MSVC)
    set(CMAKE_C_FLAGS_RELEASE "/Ox")
    set(CMAKE_CXX_FLAGS_RELEASE "/Ox")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "/Ox")
else()
    set(CMAKE_C_FLAGS_RELEASE "-O3")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3")
endif()

include(CTest)
include(Catch)
catch_discover_tests(fc_tests EXTRA_ARGS --benchmark-samples=1)
catch_discover_tests(fc_benchmarks EXTRA_ARGS --benchmark-samples=1)
# catch_discover_tests(db_tests EXTRA_ARGS --benchmark-samples=1)

set(CPACK_SOURCE_IGNORE_FILES
    #git files
    "\\\\.git/"
    "\\\\.github/"
    # temporary files
    "\\\\.swp$"
    # backup files
    "~$"
    # eclipse files
    "\\\\.cdtproject$"
    "\\\\.cproject$"
    "\\\\.project$"
    "\\\\.settings/"
    # others
    "\\\\.#"
    "/#"
    "/build/"
    "/_build/"
    "/\\\\.git/"
    "Makefile\\\\.in$"
)

include(GNUInstallDirs)

install(
    TARGETS BTree DBBTree
    EXPORT BTree_Targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "BTreeConfigVersion.cmake"
    VERSION ${MAJOR}.${MINOR}.${PATCH}
    COMPATIBILITY SameMajorVersion
)

configure_package_config_file(
    "${PROJECT_SOURCE_DIR}/cmake/BTreeConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/BTreeConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/BTree/cmake
)

install(
    EXPORT BTree_Targets
    FILE BTreeTargets.cmake
    NAMESPACE BTree::
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/BTree/cmake
)

install(FILES "${PROJECT_BINARY_DIR}/BTreeConfig.cmake"
    "${PROJECT_BINARY_DIR}/BTreeConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/BTree/cmake
)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/fc
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

set(CPACK_PACKAGE_NAME "BTree")
set(CPACK_PACKAGE_VERSION "${MAJOR}.${MINOR}.${PATCH}")
set(CPACK_PACKAGE_VENDOR "community")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-noarch")
set(CMAKE_PROJECT_HOMEPAGE_URL "https://github.com/bas524/BTree")
set(CPACK_PACKAGE_CONTACT "bas524")
set(CPACK_PACKAGE_MAINTAINER "bas524")
set(CPACK_PACKAGE_DESCRIPTION "A general-purpose high-performance lightweight STL-like modern C++ B-Tree")

set(CPACK_DEBIAN_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
set(CPACK_DEBIAN_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
set(CPACK_DEBIAN_PACKAGE_MAINTAINER ${CPACK_PACKAGE_MAINTAINER})
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION})
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${CMAKE_PROJECT_HOMEPAGE_URL})
set(CPACK_DEBIAN_PACKAGE_LICENSE "Apache-2.0")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "")

set(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
set(CPACK_RPM_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
set(CPACK_RPM_PACKAGE_MAINTAINER ${CPACK_PACKAGE_MAINTAINER})
set(CPACK_RPM_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION})
set(CPACK_RPM_PACKAGE_URL ${CMAKE_PROJECT_HOMEPAGE_URL})
set(CPACK_RPM_PACKAGE_LICENSE "Apache-2.0")
set(CPACK_RPM_PACKAGE_REQUIRES "")

include(CPack)
