#Bring the headers into the project if needed
include_directories(".")
 
#Can manually add the sources using the set command as follows:
set(SOURCES 
		${SOURCES}
		${CMAKE_CURRENT_SOURCE_DIR}/pbkdf2.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/scrypt.cpp
		PARENT_SCOPE
)
 
#the file(GLOB...) allows for wildcard additions:
#file(GLOB SOURCES "*.cpp")
