#Bring the headers into the project if needed
include_directories(.)
 
#Can manually add the sources using the set command as follows:
set(SOURCES 
		${SOURCES}
		${CMAKE_CURRENT_SOURCE_DIR}/dumpTX.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/help.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/transactions.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/headers.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/kafka.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/KafkaUtil.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/simpleStats.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/txoTypes.cpp
		PARENT_SCOPE
)
 
#could use  the file(GLOB...) allows for wildcard additions:
#file(GLOB SOURCES "*.cpp")
