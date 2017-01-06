#Bring the headers into the project if needed
include_directories(.)
 
#Can manually add the sources using the set command as follows:
set(SOURCES 
		${SOURCES}
		${CMAKE_CURRENT_SOURCE_DIR}/objyAccess.cpp
		PARENT_SCOPE
)
 
#could use  the file(GLOB...) allows for wildcard additions:
#file(GLOB SOURCES "*.cpp")
