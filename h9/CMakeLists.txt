#Bring the headers into the project if needed
include_directories(".")
 
#Can manually add the sources using the set command as follows:
set(SOURCES 
		${SOURCES}
		${CMAKE_CURRENT_SOURCE_DIR}/aes_helper.c  
		${CMAKE_CURRENT_SOURCE_DIR}/blake.c       
		${CMAKE_CURRENT_SOURCE_DIR}/bmw.c       
		${CMAKE_CURRENT_SOURCE_DIR}/cubehash.c  
		${CMAKE_CURRENT_SOURCE_DIR}/echo.c   
		${CMAKE_CURRENT_SOURCE_DIR}/fugue.c  
		${CMAKE_CURRENT_SOURCE_DIR}/groestl.c  
		${CMAKE_CURRENT_SOURCE_DIR}/hamsi.c    
		${CMAKE_CURRENT_SOURCE_DIR}/jh.c      
		${CMAKE_CURRENT_SOURCE_DIR}/keccak.c  
		${CMAKE_CURRENT_SOURCE_DIR}/luffa.c    
		${CMAKE_CURRENT_SOURCE_DIR}/shavite.c  
		${CMAKE_CURRENT_SOURCE_DIR}/simd.c
		${CMAKE_CURRENT_SOURCE_DIR}/skein.c
		PARENT_SCOPE
)
 
#the file(GLOB...) allows for wildcard additions:
#file(GLOB SOURCES "*.c")
