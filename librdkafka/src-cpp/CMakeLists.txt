#Bring the headers into the project if needed
include_directories(.)
 
#Can manually add the sources using the set command as follows:
set(SOURCES 
		${SOURCES}
		${CMAKE_CURRENT_SOURCE_DIR}/HandleImpl.cpp         
		${CMAKE_CURRENT_SOURCE_DIR}/MessageImpl.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/QueueImpl.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/TopicPartitionImpl.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/ConfImpl.cpp      
		${CMAKE_CURRENT_SOURCE_DIR}/KafkaConsumerImpl.cpp  
		${CMAKE_CURRENT_SOURCE_DIR}/MetadataImpl.cpp  
		${CMAKE_CURRENT_SOURCE_DIR}/RdKafka.cpp 
		${CMAKE_CURRENT_SOURCE_DIR}/ConsumerImpl.cpp  
		${CMAKE_CURRENT_SOURCE_DIR}/ProducerImpl.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/TopicImpl.cpp
		PARENT_SCOPE
)
 
#could use  the file(GLOB...) allows for wildcard additions:
#file(GLOB SOURCES "*.cpp")
