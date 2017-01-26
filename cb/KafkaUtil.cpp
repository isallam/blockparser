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
}

KafkaUtil::KafkaUtil(const KafkaUtil& orig) {
}

KafkaUtil::~KafkaUtil() {
}

void KafkaUtil::init() {
  batchStrBuf.Clear();
  //
  const char* trxStr = "{\"className\":\"Transaction\","
                       "\"oidAsLong\": 0, "
                       "\"primaryFieldValue\":\"\", "
                       "\"values\": { "
                       "\"m_Id\": 0, \"m_Hash\":\"\" }"
                       "}";
  
  const char* inputStr = "{\"className\":\"Input\","
                       "\"oidAsLong\": 0, "
                       "\"primaryFieldValue\":\"\", "
                       "\"values\": { "
                       "\"m_Id\": 0, \"m_IsCoinBase\": true, \"m_UpTrxHash\":\"\" }"
                       "}";
//  const char* jsonStr = "{"
//    "\"hello\": \"world\","
//    "\"t\": true ,"
//    "\"i\": 123,"
//    "\"a\": [1, 2, 3, 4]"
//    "}";
  
  transactionDoc.Parse(trxStr);
  inputDoc.Parse(inputStr);

//  StringBuffer buffer;
//  Writer<StringBuffer> writer(buffer);
//  inputDoc.Accept(writer);
//  printf(">>>%s<<<\n\n", buffer.GetString());
//  exit(0);
}

//void KafkaUtil::saveBlockAsPrevious() {
//  strcpy(prevBlockStrBuf, blockStrBuf.GetString());
//}

void KafkaUtil::blockToJson(int id, int version, uint8_t* prevBlockHash, 
        uint8_t* merkleRootHash, long blkTime, uint8_t* hash)
{
  blockStrBuf.Clear();
  blockWriter.Reset(blockStrBuf);
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
}

void KafkaUtil::prevBlockToJson(uint8_t* hash)
{
  prevBlockStrBuf.Clear();
  blockWriter.Reset(blockStrBuf);
  blockWriter.StartObject();
  blockWriter.Key("className"); blockWriter.String("Block");
  blockWriter.Key("oidAsLong"); blockWriter.Uint(0);
  blockWriter.Key("primaryFieldValue"); blockWriter.String(reinterpret_cast<char*>(hash), SizeOfHash);
  blockWriter.Key("values"); blockWriter.StartObject(); blockWriter.EndObject();
  blockWriter.EndObject();
}

/**
 * 
 * @param id
 * @param hash
 * @return 
 */
void KafkaUtil::transactionToJson(int id, uint8_t* hash)
{
  transactionStrBuf.Clear();
  transactionWriter.Reset(transactionStrBuf);
  transactionWriter.StartObject();
  transactionWriter.Key("className"); transactionWriter.String("Transaction");
  transactionWriter.Key("oidAsLong"); transactionWriter.Uint(0);
  transactionWriter.Key("primaryFieldValue"); transactionWriter.String(reinterpret_cast<char*>(hash), SizeOfHash);
  transactionWriter.Key("values"); transactionWriter.StartObject();
    transactionWriter.Key("m_Id"); transactionWriter.Uint(id);
    transactionWriter.Key("m_Hash"); transactionWriter.String(reinterpret_cast<char*>(hash), SizeOfHash);
    transactionWriter.EndObject();
  transactionWriter.EndObject();

//  Value& primeValue = transactionDoc["primaryFieldValue"];
//  primeValue.SetString(StringRef(reinterpret_cast<char*>(hash), SizeOfHash));
//  transactionDoc["values"]["m_Id"] = id;
//  Value& hashValue = transactionDoc["values"]["m_Hash"];
//  hashValue.SetString(StringRef(reinterpret_cast<char*>(hash), SizeOfHash/*, 
//          transactionDoc.GetAllocator()*/));
//
//  transactionStrBuf.Clear();
//  transactionWriter.Reset(transactionStrBuf);
//  transactionDoc.Accept(transactionWriter);
}

void KafkaUtil::upTxToJson(int id, uint8_t* hash)
{
  upTxStrBuf.Clear();
  writer.Reset(upTxStrBuf);
  writer.StartObject();
  writer.Key("className"); writer.String("Transaction");
  writer.Key("oidAsLong"); writer.Uint(0);
  writer.Key("primaryFieldValue"); writer.String(reinterpret_cast<char*>(hash));
  writer.Key("values"); writer.StartObject();
    writer.Key("m_Id"); writer.Uint(id);
    writer.Key("m_Hash"); writer.String(reinterpret_cast<char*>(hash));
    writer.EndObject();
  writer.EndObject();
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
  inputStrBuf.Clear();
  writer.Reset(inputStrBuf);
  writer.StartObject();
  writer.Key("className"); writer.String("Input");
  writer.Key("oidAsLong"); writer.Uint(0);
  writer.Key("primaryFieldValue"); writer.String("");
  writer.Key("values"); writer.StartObject();
    writer.Key("m_Id"); writer.Uint(id);
    writer.Key("m_IsCoinBase"); writer.Bool(isCoinBase);
    writer.Key("m_UpTrxHash"); writer.String(reinterpret_cast<char*>(upTrxHash), SizeOfHash);
    writer.EndObject();
  writer.EndObject();
//  inputDoc["values"]["m_Id"] = id;
//  inputDoc["values"]["m_IsCoinBase"] = isCoinBase;
//  Value& hashValue = inputDoc["values"]["m_UpTrxHash"];
//  hashValue.SetString(StringRef(reinterpret_cast<char*>(upTrxHash), SizeOfHash/*, 
//          transactionDoc.GetAllocator()*/));
//
//  inputStrBuf.Clear();
//  writer.Reset(inputStrBuf);
//  inputDoc.Accept(writer);
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
  outputStrBuf.Clear();
  writer.Reset(outputStrBuf);
  writer.StartObject();
  writer.Key("className"); writer.String("Output");
  writer.Key("oidAsLong"); writer.Uint(0);
  writer.Key("primaryFieldValue"); writer.String("");
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
  addressStrBuf.Clear();
  writer.Reset(addressStrBuf);
  writer.StartObject();
  writer.Key("className"); writer.String("Address");
  writer.Key("oidAsLong"); writer.Uint(0);
  writer.Key("primaryFieldValue"); writer.String(reinterpret_cast<char*>(hash), SizeOfAddress);
  writer.Key("values"); writer.StartObject();
    writer.Key("m_Hash"); writer.String(reinterpret_cast<char*>(hash), SizeOfAddress);
    writer.EndObject();
  writer.EndObject();
}


void KafkaUtil::submitTriple(const char* from , const char* to, 
        const char* attribute, const char* inverseAttribute)
{
  // process message to kafka topic if size reaches max
//     if(m_Triples.size() > m_MaxBatchCount)
//     {
//         sendMessages();
//     }
//     // Attempt to filter/process the triple message if enabled
//     TripleMessage triple = new TripleMessage((InstanceMessage) from, attributeName, inverseAttributeName, (InstanceMessage) to);
//     
//     if(enableFilter && m_TripleFilter.contains(triple))
//     {
//         m_FilterCount++;
//     }
//     else
//     {
//         String key = from.getPrimaryFieldValue();
//         int partition = Math.abs(key.hashCode()) % m_PartitionCount;
//         m_Triples.add(partition, key, triple);
//     }
  createTriple(from, to, attribute, inverseAttribute);
}

void KafkaUtil::createTriple(const char* from, const char* to, 
        const char* attribute, const char* inverseAttribute)
{
    tripleStrBuf.Clear();
    tripleWriter.Reset(tripleStrBuf);
    tripleWriter.StartObject();
    tripleWriter.Key("from"); tripleWriter.RawValue(from, strlen(from), kObjectType);
    tripleWriter.Key("To"); tripleWriter.RawValue(to, strlen(to), kObjectType);
    tripleWriter.Key("connectionMessage"); tripleWriter.StartObject(); tripleWriter.EndObject();
    tripleWriter.Key("attribute"); tripleWriter.String(attribute);
    tripleWriter.Key("inverseAttribute"); tripleWriter.String(inverseAttribute);
    tripleWriter.EndObject();
    addTripleToBatch();
}

void KafkaUtil::startBatch()
{
  init();
  batchWriter.Reset(batchStrBuf);
  batchWriter.StartObject();
  batchWriter.Key("tripleMessages");
  batchWriter.StartArray();
}

void KafkaUtil::endBatch()
{
  //batchWriter.Reset(batchStrBuf);
  batchWriter.EndArray();
  batchWriter.EndObject();
}

void KafkaUtil::addTripleToBatch()
{
  //writer.Reset(batchStrBuf);
  batchWriter.RawValue(tripleStrBuf.GetString(), tripleStrBuf.GetSize(), kObjectType);
}