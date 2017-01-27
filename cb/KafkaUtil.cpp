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
}

void KafkaUtil::init(int numPartitions, int maxBatchSize) {
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
     
    int partition = std::hash<std::string>()(triple.key) % numPartitions;
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
    tripleWriter.Key("To"); 
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
      batchWriter.RawValue(keyItr->c_str(), keyItr->size(), kObjectType);
      keyItr++;
    }
  batchWriter.EndArray();
  
  batchWriter.EndObject();
  
  // send to Kafka.
  printf(">>> (%d): size(%ld) <<<\n", partition, batchStrBuf.GetSize());
}