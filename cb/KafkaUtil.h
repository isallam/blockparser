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
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/document.h"
#include <iostream>

using namespace rapidjson;

const int SizeOfAddress = 40;
const int SizeOfHash = 2 * kSHA256ByteSize; 

class KafkaUtil {

  // just for information until we write the API to generate the 
  // correct batch
     const char* tripleJson = "{ \
                        \"from\": {}, \
                        \"to\":{}, \
                        \"connectionMessage\":{}, \
                        \"attribute\":\"\", \
                        \"inverseAttribute\":\"\" \
                        }";

    const char* tripleBatchJson = "{ \
                    \"tripleMessages\": [], \
                    \"keys\":[] \
                  }";


public:
  KafkaUtil();
  KafkaUtil(const KafkaUtil& orig);
  virtual ~KafkaUtil();
  
  void init();

  const char* getBlockAsJson() { return blockStrBuf.GetString(); }
  const char* getTransactionAsJson() { return transactionStrBuf.GetString(); }
  const char* getInputAsJson() { return inputStrBuf.GetString(); }
  const char* getOutputAsJson() { return outputStrBuf.GetString(); }
  const char* getAddressAsJson() { return addressStrBuf.GetString(); }
  const char* getPrevBlockAsJson() { return prevBlockStrBuf.GetString(); }
  const char* getUpTxAsJson() { return upTxStrBuf.GetString(); }

  void blockToJson(int id, int version, uint8_t* prevBlockHash, 
          uint8_t* blockMerkleRoot, long blkTime, uint8_t* hash);
  void prevBlockToJson(uint8_t* hash);
  void transactionToJson(int id, uint8_t* hash);
  void inputToJson(int id, uint8_t* upTxHash, bool isCoinBase);
  void outputToJson(int id, uint8_t* address, uint64_t trxValue);
  void addressToJson(uint8_t* hash);
  void upTxToJson(int id,  uint8_t* hash);

  void startBatch();
  void endBatch();
  const char* getBatchAsJson() { return batchStrBuf.GetString(); }

  void submitTriple(const char* from , 
          const char* to, const char* attribute, const char* inverseAttribute);

  //void saveBlockAsPrevious();

private:
  void addTripleToBatch();
  void createTriple(const char* from , 
          const char* to, const char* attribute, const char* inverseAttribute);


  
private:
    //char prevBlockStrBuf[1024];
    StringBuffer blockStrBuf;
    StringBuffer transactionStrBuf;
    Document transactionDoc;
    StringBuffer inputStrBuf;
    Document inputDoc;
    StringBuffer outputStrBuf;
    StringBuffer addressStrBuf;
    
    StringBuffer prevBlockStrBuf;
    StringBuffer upTxStrBuf;  // used to represnet the UpTx for the input.
    
    StringBuffer tripleStrBuf;
    StringBuffer batchStrBuf;
    StringBuffer keysStrBuf;

    Writer<StringBuffer> writer;
    Writer<StringBuffer> blockWriter;
    Writer<StringBuffer> transactionWriter;
    Writer<StringBuffer> tripleWriter;
    Writer<StringBuffer> batchWriter;

};

#endif /* KAFKAUTIL_H */

