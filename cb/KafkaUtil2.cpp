/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   KafkaUtil2.cpp
 * Author: ibrahim
 * 
 * Created on January 24, 2017, 10:17 PM
 */

#include "KafkaUtil2.h"

KafkaUtil2::KafkaUtil2() {
}

KafkaUtil2::KafkaUtil2(const KafkaUtil2& orig) {
}

KafkaUtil2::~KafkaUtil2() {
}

void KafkaUtil2::init() {
  //
  blockJson["className"] = "Block";
  blockJson["oidAsLong"] = 0;
  //
  transactionJson["className"] = "Transaction";
  transactionJson["oidAsLong"] = 0;
  transactionJson["primaryFieldValue"] = "";
  transactionJson["values"]["m_Id"] = 0;
  transactionJson["values"]["m_Hash"] = "";
  //
  upTxJson["className"] = "Transaction";
  upTxJson["oidAsLong"] = 0;
  upTxJson["values"] = {};
  //
  inputJson["className"] = "Input";
  inputJson["oidAsLong"] = 0;
  inputJson["primaryFieldValue"] = "";
  //
  outputJson["className"] = "Output";
  outputJson["oidAsLong"] = 0;
  outputJson["primaryFieldValue"] = "";
  //
  addressJson["className"] = "Address";
  addressJson["oidAsLong"] = 0;
  //
  batchJson["tripleMessages"] = {};
}

void KafkaUtil2::saveBlockAsPrevious() {
  //strcpy(prevBlockStrBuf, blockStrBuf.GetString());
  prevBlockJson = blockJson;
}

void KafkaUtil2::blockToJson(int id, int version, uint8_t* prevBlockHash, 
        uint8_t* merkleRootHash, long blkTime, uint8_t* hash)
{
  //blockJson.clear();
//  blockJson["className"] = "Block";
//  blockJson["oidAsLong"] = 0;
  blockJson["primaryFieldValue"] = reinterpret_cast<char*>(hash);
  blockJson["values"]["m_Id"] = id;
  blockJson["values"]["m_Version"] = version;
  blockJson["values"]["m_Time"] = blkTime;
  blockJson["values"]["m_Hash"] = reinterpret_cast<char*>(hash);
  blockJson["values"]["m_PrevBlockHash"] = reinterpret_cast<char*>(prevBlockHash);
  blockJson["values"]["m_MerkleRootHash"] = reinterpret_cast<char*>(merkleRootHash);
}

/**
 * 
 * @param id
 * @param hash
 * @return 
 */
void KafkaUtil2::transactionToJson(int id, uint8_t* hash)
{
  transactionJson["primaryFieldValue"] = reinterpret_cast<char*>(hash);
  transactionJson["values"]["m_Id"] = id;
  transactionJson["values"]["m_Hash"] = reinterpret_cast<char*>(hash);
//  transactionJson.at("primaryFieldValue") = reinterpret_cast<char*>(hash);
//  transactionJson.at("values").at("m_Id") = id;
//  transactionJson.at("values").at("m_Hash") = reinterpret_cast<char*>(hash);
}

void KafkaUtil2::upTxToJson(int id, uint8_t* hash)
{
//  upTxToJson["className"] = "Transaction";
//  upTxToJson["oidAsLong"] = 0;
  upTxJson["primaryFieldValue"] = reinterpret_cast<char*>(hash);
//  upTxToJson["values"] = {};
}

/**
 * 
 * @param id
 * @param upTxHash
 * @param upTrxRef
 * @param isCoinBase
 * @return 
 */
void KafkaUtil2::inputToJson(int id, uint8_t* upTrxHash, bool isCoinBase)
{
//  inputJson["className"] = "Input";
//  inputJson["oidAsLong"] = 0;
//  inputJson["primaryFieldValue"] = "";
  inputJson["values"]["m_Id"] = id;
  inputJson["values"]["m_IsCoinBase"] = isCoinBase;
  inputJson["values"]["m_UpTrxHash"] = reinterpret_cast<char*>(upTrxHash);
}

/**
 * 
 * @param id
 * @param address
 * @param addressRef
 * @param trxValue
 * @return 
 */
void KafkaUtil2::outputToJson(int id, uint8_t* address, uint64_t trxValue)
{
//  outputJson["className"] = "Output";
//  outputJson["oidAsLong")] = 0;
//  outputJson["primaryFieldValue"] = "";
  outputJson["values"]["m_Id"] = id;
  outputJson["values"]["m_Value"] = trxValue;
  outputJson["values"]["m_AddressHash"] = reinterpret_cast<char*>(address);
}

/**
 * 
 * @param hash
 * @return 
 */
void KafkaUtil2::addressToJson(uint8_t* hash)
{
//  addressJson["className"] = "Address";
//  addressJson["oidAsLong"] = 0;
  addressJson["primaryFieldValue"] = reinterpret_cast<char*>(hash);
  addressJson["values"]["m_Hash"] = reinterpret_cast<char*>(hash);
}


void KafkaUtil2::writeTriple(json& from , json& to, 
        const char* attribute, const char* inverseAttribute)
{
  //tripleJson.clear();
  
  tripleJson["from"] = from;
  tripleJson["To"] = to;
  tripleJson["connectionMessage"] = {};
  tripleJson["attribute"] = attribute;
  tripleJson["inverseAttribute"] = inverseAttribute;
  
  batchJson["tripleMessages"].push_back(tripleJson);
  //batchJson["tripleMessages"] += tripleJson;
  
}

void KafkaUtil2::startBatch()
{
  batchJson["tripleMessages"].clear();
  //batchJson["tripleMessages"] = [];
}

void KafkaUtil2::endBatch()
{
//  batchWriter.EndArray();
//  batchWriter.EndObject();
}