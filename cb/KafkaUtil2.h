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

#include <util.h>
#include <iostream>
#include "json.hpp"

using json = nlohmann::json;

const int SizeOfAddress = 40;
const int SizeOfHash = 2 * kSHA256ByteSize; 

class KafkaUtil2 {

  // just for information until we write the API to generate the 
  // correct batch
 
    const char* tripleBatchJson = "{ \
                    \"tripleMessages\": [], \
                    \"keys\":[] \
                  }";


public:
  KafkaUtil2();
  KafkaUtil2(const KafkaUtil2& orig);
  virtual ~KafkaUtil2();
  
  void init();

//  std::string getBlockAsJson() { return blockJson.dump(); }
//  std::string getTransactionAsJson() { return transactionJson.dump(); }
//  std::string getInputAsJson() { return inputJson.dump(); }
//  std::string getOutputAsJson() { return outputJson.dump(); }
//  std::string getAddressAsJson() { return addressJson.dump(); }
//  std::string getPrevBlockAsJson() { return prevBlockJson.dump(); }
//  std::string getUpTxAsJson() { return upTxJson.dump(); }

  json& getBlockAsJson() { return blockJson; }
  json& getTransactionAsJson() { return transactionJson; }
  json& getInputAsJson() { return inputJson; }
  json& getOutputAsJson() { return outputJson; }
  json& getAddressAsJson() { return addressJson; }
  json& getPrevBlockAsJson() { return prevBlockJson; }
  json& getUpTxAsJson() { return upTxJson; }

  void blockToJson(int id, int version, uint8_t* prevBlockHash, 
          uint8_t* blockMerkleRoot, long blkTime, uint8_t* hash);
  void transactionToJson(int id, uint8_t* hash);
  void inputToJson(int id, uint8_t* upTxHash, bool isCoinBase);
  void outputToJson(int id, uint8_t* address, uint64_t trxValue);
  void addressToJson(uint8_t* hash);
  void upTxToJson(int id,  uint8_t* hash);

  void startBatch();
  void endBatch();
  std::string getBatchAsJson() { return batchJson.dump(); }

  void writeTriple(json& from , json& to, 
          const char* attribute, const char* inverseAttribute);

  void saveBlockAsPrevious();

private:

  
private:
    char prevBlockStrBuf[1024];
    json blockJson;
    json prevBlockJson;
    json transactionJson;
    json inputJson;
    json outputJson;
    json addressJson;
    
    json upTxJson;  // used to represnet the UpTx for the input.
    
    json tripleJson;
    json batchJson;
    json keysJson;

};

#endif /* KAFKAUTIL_H */

