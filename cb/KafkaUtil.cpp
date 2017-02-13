/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   KafkaUtil.cpp
 * Author: ibrahim
 * 
 * Created on January 24, 2017, 10:17 PM
 */

#include "KafkaUtil.h"

KafkaUtil::KafkaUtil() {
    numPartitions = 16;
    maxBatchSize = 1000;
}

KafkaUtil::KafkaUtil(const KafkaUtil& orig) {
}

KafkaUtil::~KafkaUtil() {
    RdKafka::ErrorCode resp = producer->flush(10);
    if (resp != RdKafka::ERR_NO_ERROR)
    {
      std::cerr << "% Producer flush failed: " << RdKafka::err2str(resp) << std::endl;
    }
}

void KafkaUtil::init(int numPartitions, int maxBatchSize, 
        std::string kafkaBrokers, std::string kafkaTopic) {
  this->numPartitions = numPartitions;
  this->maxBatchSize = maxBatchSize;
  
  Batch* batch;
  for (int i = 0; i < numPartitions; i++)
  {
    batch = new Batch;
    batchList.push_back(*batch);
  }
  
  // initialize the empty edge
  emptyDataElement.buffer.Clear();
  emptyEdgeElement.buffer.Clear();
  writer.Reset(emptyDataElement.buffer); 
  writer.StartObject();  writer.EndObject();
  writer.Reset(emptyEdgeElement.buffer);
  writer.StartObject();  writer.EndObject();
  
  // kafka stuff
    std::string brokers = kafkaBrokers;
    std::string errstr;
    std::string topic_str = kafkaTopic;
    std::string debug;

    /*
     * Create configuration objects
     */
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    // -------------------
    // compression option.
    // -------------------
//  -z <codec>      Enable compression:
//                  none|gzip|snappy
//    case 'z':
//      if (conf->set("compression.codec", optarg, errstr) !=
//          RdKafka::Conf::CONF_OK) {
    // -------------------
    // stats option
    // -------------------
//  -M <intervalms> Enable statistics
//      if (conf->set("statistics.interval.ms", optarg, errstr) !=
//          RdKafka::Conf::CONF_OK) {
    /*
     * Set configuration properties
     */
    conf->set("metadata.broker.list", brokers, errstr);

    // -----------------------
    // Debug options if needed
    // -----------------------
//-d [facs..]     Enable debugging contexts:
//            all,generic,broker,topic,metadata,queue,msg,protocol,cgrp,security,fetch,feature

    if (!debug.empty()) {
      if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
      }
    }
    
    
    /*
     * Create producer using accumulated global configuration.
     */
    producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
      std::cerr << "Failed to create producer: " << errstr << std::endl;
      exit(1);
    }
    std::cout << "% Created producer " << producer->name() << std::endl;

    /*
     * Create topic handle.
     */
    topic = RdKafka::Topic::create(producer, topic_str,
               tconf, errstr);
    if (!topic) {
      std::cerr << "Failed to create topic: " << errstr << std::endl;
      exit(1);
    }

//    producer->poll(0);
//    RdKafka::ErrorCode resp = producer->flush(0);
//    if (resp != RdKafka::ERR_NO_ERROR)
//    {
//      std::cerr << "% Producer flush failed: " << RdKafka::err2str(resp) << std::endl;
//      exit(-1);
//    }
}


void KafkaUtil::blockToJson(int id, int version, uint8_t* prevBlockHash, 
        uint8_t* merkleRootHash, long blkTime, uint8_t* hash)
{
  blockElement.buffer.Clear();
  blockWriter.Reset(blockElement.buffer);
  blockWriter.StartObject();
  blockWriter.Key("className"); blockWriter.String("Block");
  blockWriter.Key("oidAsLong"); blockWriter.Uint(0);
  blockWriter.Key("primaryFieldValue"); blockWriter.String(reinterpret_cast<char*>(hash), SizeOfHash);
  blockWriter.Key("values"); blockWriter.StartObject();
    blockWriter.Key("m_Id"); blockWriter.Uint(id);
    blockWriter.Key("m_Version"); blockWriter.Uint(version);
    blockWriter.Key("m_Time"); blockWriter.Uint64(blkTime);
    blockWriter.Key("m_Hash"); blockWriter.String(reinterpret_cast<char*>(hash), SizeOfHash);
    blockWriter.Key("m_PrevBlockHash"); blockWriter.String(reinterpret_cast<char*>(prevBlockHash), SizeOfHash);
    blockWriter.Key("m_MerkleRootHash"); blockWriter.String(reinterpret_cast<char*>(merkleRootHash), SizeOfHash);
    blockWriter.EndObject();
  blockWriter.EndObject();
  blockElement.key = reinterpret_cast<char*>(hash);
}

void KafkaUtil::prevBlockToJson(uint8_t* hash)
{
  prevBlockElement.buffer.Clear();
  blockWriter.Reset(prevBlockElement.buffer);
  blockWriter.StartObject();
  blockWriter.Key("className"); blockWriter.String("Block");
  blockWriter.Key("oidAsLong"); blockWriter.Uint(0);
  blockWriter.Key("primaryFieldValue"); blockWriter.String(reinterpret_cast<char*>(hash), SizeOfHash);
  blockWriter.Key("values"); blockWriter.StartObject(); blockWriter.EndObject();
  blockWriter.EndObject();
  prevBlockElement.key = reinterpret_cast<char*>(hash);
}

/**
 * 
 * @param id
 * @param hash
 * @return 
 */
void KafkaUtil::transactionToJson(int id, uint8_t* hash)
{
  transactionElement.buffer.Clear();
  transactionWriter.Reset(transactionElement.buffer);
  transactionWriter.StartObject();
  transactionWriter.Key("className"); transactionWriter.String("Transaction");
  transactionWriter.Key("oidAsLong"); transactionWriter.Uint(0);
  transactionWriter.Key("primaryFieldValue"); transactionWriter.String(reinterpret_cast<char*>(hash), SizeOfHash);
  transactionWriter.Key("values"); transactionWriter.StartObject();
    transactionWriter.Key("m_Id"); transactionWriter.Uint(id);
    transactionWriter.Key("m_Hash"); transactionWriter.String(reinterpret_cast<char*>(hash), SizeOfHash);
    transactionWriter.EndObject();
  transactionWriter.EndObject();
  transactionElement.key = reinterpret_cast<char*>(hash);
}

void KafkaUtil::upTxToJson(int id, uint8_t* hash)
{
  upTxElement.buffer.Clear();
  writer.Reset(upTxElement.buffer);
  
  writer.StartObject();
  writer.Key("className"); writer.String("Transaction");
  writer.Key("oidAsLong"); writer.Uint(0);
  writer.Key("primaryFieldValue"); writer.String(reinterpret_cast<char*>(hash));
  writer.Key("values"); writer.StartObject();
    writer.Key("m_Id"); writer.Uint(id);
    writer.Key("m_Hash"); writer.String(reinterpret_cast<char*>(hash));
    writer.EndObject();
  writer.EndObject();
  
  upTxElement.key = reinterpret_cast<char*>(hash);
}

/**
 * 
 * @param id
 * @param upTxHash
 * @param upTrxRef
 * @param isCoinBase
 * @return 
 */
void KafkaUtil::inputToJson(int id, uint8_t* upTrxHash, bool isCoinBase)
{
  inputElement.buffer.Clear();
  writer.Reset(inputElement.buffer);
  
  writer.StartObject();
  writer.Key("className"); writer.String("Input");
  writer.Key("oidAsLong"); writer.Uint(0);
  writer.Key("sourceAttributeName"); writer.String("m_Transaction");
  writer.Key("targetAttributeName"); writer.String("m_UpTx");
  writer.Key("values"); writer.StartObject();
    writer.Key("m_Id"); writer.Uint(id);
    writer.Key("m_IsCoinBase"); writer.Bool(isCoinBase);
    writer.Key("m_UpTrxHash"); writer.String(reinterpret_cast<char*>(upTrxHash), SizeOfHash);
    writer.EndObject();
  writer.EndObject();
}

/**
 * 
 * @param id
 * @param address
 * @param addressRef
 * @param trxValue
 * @return 
 */
void KafkaUtil::outputToJson(int id, uint8_t* address, uint64_t trxValue)
{
  outputElement.buffer.Clear();
  writer.Reset(outputElement.buffer);
  
  writer.StartObject();
  writer.Key("className"); writer.String("Output");
  writer.Key("oidAsLong"); writer.Uint(0);
  writer.Key("sourceAttributeName"); writer.String("m_Transaction");
  writer.Key("targetAttributeName"); writer.String("m_Address");
  writer.Key("values"); writer.StartObject();
    writer.Key("m_Id"); writer.Uint(id);
    writer.Key("m_Value"); writer.Uint64(trxValue);
    writer.Key("m_AddressHash"); writer.String(reinterpret_cast<char*>(address), SizeOfAddress);
    writer.EndObject();
  writer.EndObject();  
}

/**
 * 
 * @param hash
 * @return 
 */
void KafkaUtil::addressToJson(uint8_t* hash)
{
  addressElement.buffer.Clear();
  writer.Reset(addressElement.buffer);
  
  writer.StartObject();
  writer.Key("className"); writer.String("Address");
  writer.Key("oidAsLong"); writer.Uint(0);
  writer.Key("primaryFieldValue"); writer.String(reinterpret_cast<char*>(hash), SizeOfAddress);
  writer.Key("values"); writer.StartObject();
    writer.Key("m_Hash"); writer.String(reinterpret_cast<char*>(hash), SizeOfAddress);
    writer.EndObject();
  writer.EndObject();
  
  addressElement.key = reinterpret_cast<char*>(hash);
}


void KafkaUtil::submitTriple(const DataElement& from, const EdgeElement& edge, 
        const char* attribute)
{
  
  return submitTriple(from, edge, emptyDataElement, attribute, "");
}  

void KafkaUtil::submitTriple(const DataElement& from, const DataElement& to, 
        const char* attribute, const char* inverseAttribute)
{
  
  return submitTriple(from, emptyEdgeElement, to, attribute, inverseAttribute);
  
//    // Attempt to filter/process the triple message if enabled
//    const DataElement& triple = constructTriple(from, to, attribute, inverseAttribute);
//     
//    int partition = std::hash<std::string>()(triple.key) % numPartitions;
//    addTripleToBatch(partition, triple);
}

void KafkaUtil::submitTriple(const DataElement& from, const EdgeElement& edge, 
        const DataElement& to, 
        const char* attribute, const char* inverseAttribute)
{
    // Attempt to filter/process the triple message if enabled
    const DataElement& triple = constructTriple(from, edge, to, attribute, inverseAttribute);
     
//    int partition = std::hash<std::string>()(triple.key) % numPartitions;
    int partition = abs(hashCode(triple.key)) % numPartitions;
    addTripleToBatch(partition, triple);
}

const DataElement& KafkaUtil::constructTriple(const DataElement& from, 
        const EdgeElement& edge, const DataElement& to, 
        const char* attribute, const char* inverseAttribute)
{
    tripleElement.buffer.Clear();
    tripleWriter.Reset(tripleElement.buffer);
    tripleWriter.StartObject();
    tripleWriter.Key("from"); 
    tripleWriter.RawValue(from.buffer.GetString(), from.buffer.GetSize(), kObjectType);
    tripleWriter.Key("to"); 
    tripleWriter.RawValue(to.buffer.GetString(), to.buffer.GetSize(), kObjectType);
    tripleWriter.Key("connectionMessage"); 
    tripleWriter.RawValue(edge.buffer.GetString(), edge.buffer.GetSize(), kObjectType);
    tripleWriter.Key("attribute"); tripleWriter.String(attribute);
    tripleWriter.Key("inverseAttribute"); tripleWriter.String(inverseAttribute);
    tripleWriter.EndObject();
    
    if (from.key.empty())
      printf("Empty Key for: %s\n", from.buffer.GetString());
    tripleElement.key = from.key;
    return tripleElement;
}


void KafkaUtil::addTripleToBatch(int partition, const DataElement& tripleElement)
{
  Batch& batch = batchList[partition];
  if (batch.size() >= maxBatchSize)
  {
    submitBatch(partition, batch);
    batch.clear();
  }
  batch.add(tripleElement);
}

void KafkaUtil::submitBatch(int partition, const Batch& batch)
{

  StringBuffer batchStrBuf;
  batchWriter.Reset(batchStrBuf);
  batchWriter.StartObject();
  batchWriter.Key("tripleMessages");
  batchWriter.StartArray();
    std::vector<std::string> triples = batch.tripleList;
    std::vector<std::string>::iterator tripleItr = triples.begin();
    while (tripleItr != triples.end())
    {
      batchWriter.RawValue(tripleItr->c_str(), tripleItr->size(), kObjectType);
      tripleItr++;
    }
  batchWriter.EndArray();
  
  batchWriter.Key("keys");
  batchWriter.StartArray();
    std::vector<std::string> keys = batch.keyList;
    std::vector<std::string>::iterator keyItr = keys.begin();
    while (keyItr != keys.end())
    {
//      batchWriter.RawValue(keyItr->c_str(), keyItr->size(), kObjectType);
      batchWriter.String(keyItr->c_str(), keyItr->size(), true);
      keyItr++;
    }
  batchWriter.EndArray();
  
  batchWriter.EndObject();
  
  // send to Kafka.
  //printf(">>> (%d): size(%ld) <<<\n", partition, batchStrBuf.GetSize());
  // ------------------------
  // produce into kafka topic
  // ------------------------
      /*
       * Produce message
       */
      RdKafka::ErrorCode resp =
      producer->produce(topic, partition,
          RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
          const_cast<char *>(batchStrBuf.GetString()), batchStrBuf.GetSize(),
          NULL, NULL);
      if (resp != RdKafka::ERR_NO_ERROR)
      {
        std::cerr << "% Produce failed: " << RdKafka::err2str(resp) << std::endl;
        std::cerr << "% we used partition: " << partition << std::endl;
        exit(-1);
      }
    //  else
    //    std::cerr << "% Produced message (" << batchStrBuf.GetSize() << " bytes)" << std::endl;

      producer->poll(0);
  
}

int KafkaUtil::hashCode(const std::string stringValue) {
  int hash = 0;
  for (int i = 0; i < stringValue.size(); i++) {
    hash = (hash << 5) - hash + stringValue[i];
  }
  return hash;
}
