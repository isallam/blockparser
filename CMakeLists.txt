cmake_minimum_required (VERSION 2.8)
project (blockparser)

set(CMAKE_BUILD_TYPE Release)
#set(CMAKE_CXX_FLAGS "-std=c++11") 
#set(CMAKE_CXX_FLAGS " -O6 -g0  -m64 -Wall -flto -msse3 -Wextra -Wformat -pedantic -std=c++11 -ffast-math -march=native -fno-check-new -funroll-loops -Wno-deprecated -falign-loops=16 -Wformat-security -fstrict-aliasing -Wstrict-aliasing=2 -Wno-strict-overflow -Wno-unused-variable -Wno-variadic-macros -fomit-frame-pointer -Wno-unused-parameter -finline-limit=1000000 -Wno-unused-private-field -Wno-unused-local-typedefs")

set(CMAKE_CXX_FLAGS " -O3 -fno-inline -g3 -m64 -Wall -msse3 -Wextra -Wformat -pedantic -std=c++11 -ffast-math -march=native -D__extern_always_inline=\"\" -funroll-loops -Wno-deprecated -Wformat-security -fstrict-aliasing -Wstrict-aliasing=2 -Wno-strict-overflow -Wno-unused-variable -Wno-variadic-macros -Wno-unused-parameter -fno-inline -Wno-unused-private-field -Wno-unused-local-typedefs")

ADD_DEFINITIONS(-DNDEBUG)
ADD_DEFINITIONS(-DBITCOIN)
ADD_DEFINITIONS(-DWANT_DENSE)
ADD_DEFINITIONS(-D__STDC_FORMAT_MACROS=1)

include_directories(. /opt/Objectivity/12.3/include)
 
SET(SOURCES callback.cpp opcodes.cpp option.cpp rmd160.cpp sha256.cpp timer.cpp util.cpp)

add_subdirectory (t)
add_subdirectory (h9) 
add_subdirectory (scrypt) 
add_subdirectory (crypto) 
add_subdirectory (cb) 

set(CMAKE_EXE_LINKER_FLAGS ${CMAKE_CXX_FLAGS})

#set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} /opt/Objectivity/12.3/lib)
link_directories(/opt/Objectivity/12.3/lib)

SET(SYSTEM_LIBS crypto dl)
SET(OBJYLIBS oo.12.3 ooSessionManager.12.3 ooConfiguration.12.3 ooObjectModel.12.3 ooData.12.3 ooUtil.12.3 ooError.12.3)

add_executable(blockparser ${SOURCES} parser.cpp)
add_executable(tst ${SOURCES} ${TEST_SOURCES} test.cpp tst.cpp)

target_link_libraries(blockparser LINK_PUBLIC ${SYSTEM_LIBS} ${OBJYLIBS})
target_link_libraries(tst LINK_PUBLIC ${SYSTEM_LIBS} ${OBJYLIBS})
