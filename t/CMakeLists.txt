#Bring the headers into the project if needed
include_directories(".")
 
#Can manually add the sources using the set command as follows:
set(TEST_SOURCES 
		${TEST_SOURCES} 
		${CMAKE_CURRENT_SOURCE_DIR}/base58_t.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/ripemd-160_t.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/secp256k1_t.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/sha256_t.cpp
		PARENT_SCOPE
)
 
#we could use the file(GLOB...) allows for wildcard additions:
#file(GLOB SOURCES "*.cpp")
