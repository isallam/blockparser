/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   objySchema.h
 * Author: ibrahim
 *
 * Created on January 5, 2017, 4:06 PM
 */

#ifndef OBJYSCHEMA_H
#define OBJYSCHEMA_H

#include <ooObjy.h>
#include <objy/db/Connection.h>
#include <objy/data/Data.h>
#include <objy/data/List.h>
#include <objy/data/DataSpecificationBuilder.h>

namespace objydb = objy::db;
namespace objydata = objy::data;
namespace objyschema = objy::schema_provider;

struct TheClass {
  char* name;
  objydata::Class classRef;
};

struct BlockClass : TheClass {
  objydata::Attribute idAttr;
  objydata::Attribute versionAttr;
  objydata::Attribute timeAttr; 
  objydata::Attribute hashAttr;
  objydata::Attribute prevBlockHashAttr;
  objydata::Attribute merkleRootHashAttr;
  objydata::Attribute prevBlockAttr;
  objydata::Variable value;
  objydata::Utf8String stringValue = objydata::createUtf8String();
};

struct TransactionClass : TheClass {
  objydata::Attribute idAttr;
  objydata::Attribute hashAttr;
  objydata::Variable value;
  objydata::Utf8String stringValue = objydata::createUtf8String();
};

struct InputClass : TheClass {
  objydata::Attribute idAttr;
  objydata::Attribute isCoinBaseAttr;
  objydata::Attribute upTxHashAttr; 
  objydata::Attribute upTxAttr;
  objydata::Variable value;
  objydata::Utf8String stringValue = objydata::createUtf8String();
};

struct OutputClass : TheClass {
  objydata::Attribute idAttr;
  objydata::Attribute valueAttr;
  objydata::Attribute addressHashAttr; 
  objydata::Attribute addressAttr;
  objydata::Variable value;
  objydata::Utf8String stringValue = objydata::createUtf8String();
};

struct AddressClass : TheClass {
  objydata::Attribute hashAttr;
  objydata::Variable value;
  objydata::Utf8String stringValue = objydata::createUtf8String();
};

class ObjyAccess {
public:
  ObjyAccess();
  virtual ~ObjyAccess();
  bool createSchema();
  bool setupCache();
  
  objydata::Reference createBlock(
        int id, int version, uint8_t* prevBlockHash, uint8_t* blockMerkleRoot, 
        long blkTime, uint8_t* hash, objydata::Reference& prevBlock);
  objydata::Reference createTransaction(int id, uint8_t* hash);
  objydata::Reference createInput(
          int id, uint8_t* upTxHash, objydata::Reference& upTrxRef, bool isCoinBase);
  objydata::Reference createOutput(int id, 
          uint8_t* address, objydata::Reference& addressRef, uint64_t trxValue);
  objydata::Reference createAddress(uint8_t* hash);

  bool addTransactionToBlock(objydata::Reference& transaction, objydata::Reference& block);
  bool addInputToTransaction(objydata::Reference& input, objydata::Reference& transaction);
  bool addOutputToTransaction(objydata::Reference& output, objydata::Reference& transaction);

private:
  
  // class names
  const char* BlockClassName        = "Block";
  const char* TransactionClassName  = "Transaction";
  const char* InputClassName        = "Input";
  const char* OutputClassName       = "Output";
  const char* AddressClassName      = "Address";

// cached classes
//  objydata::Class blockClass;
//  objydata::Class transactionClass;
//  objydata::Class inputClass;
//  objydata::Class outputClass;
//  objydata::Class addressClass;

// more caching 
  BlockClass blockClass;
  TransactionClass transactionClass;
  InputClass inputClass;
  OutputClass outputClass;
  AddressClass addressClass;
};

#endif /* OBJYSCHEMA_H */

