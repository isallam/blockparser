#Bring the headers into the project if needed
include_directories(.)
 
#Can manually add the sources using the set command as follows:
set(SOURCES 
		${SOURCES}
		${CMAKE_CURRENT_SOURCE_DIR}/rdaddr.c            
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_cgrp.c            
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_op.c                   
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_subscription.c  
		${CMAKE_CURRENT_SOURCE_DIR}/rdregex.c
		${CMAKE_CURRENT_SOURCE_DIR}/rdavl.c             
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_conf.c            
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_partition.c            
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_timer.c         
		${CMAKE_CURRENT_SOURCE_DIR}/rdstring.c

		${CMAKE_CURRENT_SOURCE_DIR}/rdcrc32.c           
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_event.c           
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_pattern.c              
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_topic.c         
		${CMAKE_CURRENT_SOURCE_DIR}/regexp.c

		${CMAKE_CURRENT_SOURCE_DIR}/rdgz.c              
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_feature.c         
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_queue.c                
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_transport.c     
		${CMAKE_CURRENT_SOURCE_DIR}/snappy.c

		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_assignor.c  
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_metadata.c        
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_range_assignor.c       
		${CMAKE_CURRENT_SOURCE_DIR}/rdlist.c                
		${CMAKE_CURRENT_SOURCE_DIR}/tinycthread.c

		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_broker.c    
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_metadata_cache.c  
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_request.c              
		${CMAKE_CURRENT_SOURCE_DIR}/rdlog.c                 
		${CMAKE_CURRENT_SOURCE_DIR}/xxhash.c

		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_buf.c       
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_msg.c             
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_roundrobin_assignor.c  
		${CMAKE_CURRENT_SOURCE_DIR}/rdports.c

		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka.c           
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_offset.c          
		${CMAKE_CURRENT_SOURCE_DIR}/rdkafka_sasl.c                 
		${CMAKE_CURRENT_SOURCE_DIR}/rdrand.c
		
		PARENT_SCOPE
)
 
#could use  the file(GLOB...) allows for wildcard additions:
#file(GLOB SOURCES "*.cpp")
