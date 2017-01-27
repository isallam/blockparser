/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   KafkaUtil.h
 * Author: ibrahim
 *
 * Created on January 24, 2017, 10:17 PM
 */

#ifndef KAFKAUTIL_H
#define KAFKAUTIL_H

#include <iostream>
#include <string>
#include <functional>
#include <vector>

#include <util.h>
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/document.h"

using namespace rapidjson;

const int SizeOfAddress = 40;
const int SizeOfHash = 2 * kSHA256ByteSize; 

struct DataElement {
  StringBuffer buffer;
  std::string key;  
};

struct EdgeElement {
  StringBuffer buffer;
  // there is no "key" for edgeElement.
};

struct Batch {
  std::vector<std::string> tripleList;
  std::vector<std::string> keyList;
 
  void clear() {
    tripleList.clear();
    keyList.clear();
  };

  int size() {
    return tripleList.size();
  };
  
  void add(const DataElement& tripleElement)
  {
    tripleList.push_back(tripleElement.buffer.GetString());
    keyList.push_back(tripleElement.key);
  };
};

typedef std::vector<Batch> BatchList;
typedef std::vector<Batch>::iterator BatchListItr;


class KafkaUtil {

public:
  KafkaUtil();
  
  KafkaUtil(const KafkaUtil& orig);
  virtual ~KafkaUtil();
  
  void init(int numPipelines = 16, int maxBatchSize = 10);

  void blockToJson(int id, int version, uint8_t* prevBlockHash, 
          uint8_t* blockMerkleRoot, long blkTime, uint8_t* hash);
  void prevBlockToJson(uint8_t* hash);
  void transactionToJson(int id, uint8_t* hash);
  void inputToJson(int id, uint8_t* upTxHash, bool isCoinBase);
  void outputToJson(int id, uint8_t* address, uint64_t trxValue);
  void addressToJson(uint8_t* hash);
  void upTxToJson(int id,  uint8_t* hash);

  //const char* getBatchAsJson() { return batchStrBuf.GetString(); }

 
  DataElement& getBlockElement() { return blockElement; }
  DataElement& getTransactionElement() { return transactionElement; }
  EdgeElement& getInputElement() { return inputElement; }
  EdgeElement& getOutputElement() { return outputElement; }
  DataElement& getAddressElement() { return addressElement; }
  DataElement& getPrevBlockElement() { return prevBlockElement; }
  DataElement& getUpTxElement() { return upTxElement; }
 
  void submitTriple(
          const DataElement& from , const DataElement& to, 
          const char* attribute, const char* inverseAttribute);

  void submitTriple(
          const DataElement& from , const EdgeElement& edge, const DataElement& to, 
          const char* attribute, const char* inverseAttribute);

  void submitTriple(
          const DataElement& from , const EdgeElement& edge, 
          const char* attribute);
  //void saveBlockAsPrevious();

private:
  void addTripleToBatch(int partition, const DataElement& tripleElement);
  const DataElement& constructTriple(
                        const DataElement& from , const EdgeElement& edge, 
                        const DataElement& to, 
                        const char* attribute, const char* inverseAttribute);

  void submitBatch(int partition, const Batch& batch);

  
private:
    DataElement blockElement;
    DataElement transactionElement;
    EdgeElement inputElement;
    EdgeElement outputElement;
    DataElement addressElement;
    
    
    DataElement prevBlockElement;
    DataElement upTxElement;  // used to represnet the UpTx for the input.
    
    DataElement tripleElement;

    DataElement emptyDataElement;
    EdgeElement emptyEdgeElement;
    
    //StringBuffer batchStrBuf;
 
    Writer<StringBuffer> writer;
    Writer<StringBuffer> blockWriter;
    Writer<StringBuffer> transactionWriter;
    Writer<StringBuffer> tripleWriter;
    Writer<StringBuffer> batchWriter;
    
    int numPartitions;
    int maxBatchSize;

    BatchList batchList;
};

#endif /* KAFKAUTIL_H */

