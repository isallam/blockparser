/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   ObjyAccess.cpp
 * Author: ibrahim
 * 
 * Created on January 5, 2017, 4:06 PM
 */

#include "objyAccess.h"

ObjyAccess::ObjyAccess() {
}

ObjyAccess::~ObjyAccess() {
}

void addStringAttribute(objydata::ClassBuilder& builder,
                        const char* name,
                        objy::uint_16 length)
{
  objydata::DataSpecificationHandle spec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::String>()
    .setEncoding(objydata::StringEncoding::Utf8)
    .setStorage(objydata::StringStorage::Fixed)
    .setFixedLength(length)
    .build();

  builder.addAttribute(name, spec);
}

/**
 * createSchema 
 * 
 * @return 
 */
bool ObjyAccess::createSchema()
{
  // Block Class
   objydata::DataSpecificationHandle blockRefSpec = 
           objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()    
          .setReferencedClass(BlockClassName)
          .build();

 // transaction arrays 
  objydata::DataSpecificationHandle transactionsElemSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          //.setIdentifierSpecification(objydata::SpecificationFor<objy::uint_64>::get())
          .setReferencedClass(TransactionClassName)
          .build();
                  
  objydata::DataSpecificationHandle transactionsSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(transactionsElemSpec)
          //.setCollectionName("SegmentedArray")
          .build();

  objydata::DataSpecificationHandle stringSpec = 
        objydata::DataSpecificationBuilder<objydata::LogicalType::String>()
    .setEncoding(objydata::StringEncoding::Utf8)
    .setStorage(objydata::StringStorage::Fixed)
    .setFixedLength(66)
    .build();

  objydata::Class blockClass = 
                objydata::ClassBuilder(BlockClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>("id")
                 .addAttribute<objy::int_32>("version")
                 .addAttribute<objydata::DateTime>("time")
                 .addAttribute<objydata::Utf8String>("hash")
                 .addAttribute<objydata::Utf8String>("prevBlockHash")
                 .addAttribute<objydata::Utf8String>("merkleRootHash")
                 //.addAttribute("hash", stringSpec)
                 //.addAttribute("prevBlockHash", stringSpec)
                 //.addAttribute("merkleRootHash", stringSpec)
                 .addAttribute("prevBlock", blockRefSpec)
                 .addAttribute("transactions", transactionsSpec)
                 .build();

  // Transaction Class
  
  objydata::DataSpecificationHandle inputsElemSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(InputClassName)
          .build();
                  
  objydata::DataSpecificationHandle inputsSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(inputsElemSpec)
          //.setCollectionName("SegmentedArray")
          .build();
  
  objydata::DataSpecificationHandle outputsElemSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(OutputClassName)
          .build();
                  
  objydata::DataSpecificationHandle outputsSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(outputsElemSpec)
          //.setCollectionName("SegmentedArray")
          .build();
  
  objydata::Class transactionClass = 
                 objydata::ClassBuilder(TransactionClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>("id")
                 //.addAttribute(objydata::LogicalType.DateTime, "time")
                 .addAttribute<objydata::Utf8String>("hash")
                 //.addAttribute("hash", stringSpec)
                 .addAttribute("inputs", inputsSpec)
                 .addAttribute("outputs", outputsSpec)
                 .build();
  
  // Input Class
  objydata::DataSpecificationHandle transactionRefSpec = 
           objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(TransactionClassName)
          .build();
  
  objydata::Class inputClass = 
                 objydata::ClassBuilder(InputClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>("id")
                 .addAttribute<objydata::Utf8String>("upTxHash")
                 .addAttribute("upTx", transactionRefSpec)
                 .addAttribute<bool>("isCoinBase")
                 .build();

  // Address Class
  objydata::Class addressClass = 
                 objydata::ClassBuilder(AddressClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objydata::Utf8String>("hash")
                 .build();
  
  // output Class
  objydata::DataSpecificationHandle addressRefSpec = 
           objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(AddressClassName)
          .build();

  objydata::Class outputClass = 
                 objydata::ClassBuilder(OutputClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>("id")
                 .addAttribute<objydata::Utf8String>("addressHash")
                 .addAttribute("address", addressRefSpec)
                 .addAttribute<objy::uint_64>("value")
                 .build();
  
  
  objyschema::SchemaProvider* provider = objyschema::SchemaProvider::defaultPersistentProvider();
  provider->represent(blockClass);
  provider->represent(transactionClass);
  provider->represent(inputClass);
  provider->represent(outputClass);
  provider->represent(addressClass);
  
  // cache classess.
  
  return true;
  
}

/**
 * 
 * @return 
 */
bool ObjyAccess::setupCache()
{
  // cache classes.
  blockClass.classRef = objydata::lookupClass(BlockClassName);
  blockClass.idAttr = blockClass.classRef.lookupAttribute("id");
  blockClass.versionAttr = blockClass.classRef.lookupAttribute("version");
  blockClass.timeAttr = blockClass.classRef.lookupAttribute("time"); 
  blockClass.hashAttr = blockClass.classRef.lookupAttribute("hash");
  blockClass.prevBlockHashAttr = blockClass.classRef.lookupAttribute("prevBlockHash");
  blockClass.merkleRootHashAttr = blockClass.classRef.lookupAttribute("merkleRootHash");
  blockClass.prevBlockAttr = blockClass.classRef.lookupAttribute("prevBlock");

  transactionClass.classRef = objydata::lookupClass(TransactionClassName);
  transactionClass.idAttr = transactionClass.classRef.lookupAttribute("id");
  transactionClass.hashAttr = transactionClass.classRef.lookupAttribute("hash");
    
  inputClass.classRef = objydata::lookupClass(InputClassName);
  inputClass.idAttr = inputClass.classRef.lookupAttribute("id");
  inputClass.isCoinBaseAttr = inputClass.classRef.lookupAttribute("isCoinBase");
  inputClass.upTxHashAttr = inputClass.classRef.lookupAttribute("upTxHash"); 
  inputClass.upTxAttr = inputClass.classRef.lookupAttribute("upTx");
    
  outputClass.classRef = objydata::lookupClass(OutputClassName);
  outputClass.idAttr = outputClass.classRef.lookupAttribute("id");
  outputClass.valueAttr = outputClass.classRef.lookupAttribute("value");
  outputClass.addressHashAttr = outputClass.classRef.lookupAttribute("addressHash"); 
  outputClass.addressAttr = outputClass.classRef.lookupAttribute("address");
    
  addressClass.classRef = objydata::lookupClass(AddressClassName);
  addressClass.hashAttr = addressClass.classRef.lookupAttribute("hash");
  
    return true;
}

/**
 * 
 * @param id
 * @param version
 * @param prevBlockHash
 * @param blockMerkleRoot
 * @param blkTime
 * @param hash
 * @param prevBlock
 * @return 
 */
objydata::Reference ObjyAccess::createBlock( 
        int id, int version, uint8_t* prevBlockHash, 
        uint8_t* blockMerkleRoot, long blkTime, uint8_t* hash, 
        objydata::Reference& prevBlock)
{
  objydata::Object object = objydata::createPersistentObject(blockClass.classRef);
  
  //object.attributeValue("id").set<objy::uint_64>(id);
  object.attributeValue(blockClass.idAttr, blockClass.value);
  blockClass.value.set(id);
  
  //object.attributeValue("version").set<objy::int_32>(version);
  object.attributeValue(blockClass.versionAttr, blockClass.value);
  blockClass.value.set(version);
  
  //objydata::DateTime dateTime;
  
  //object.attributeValue("time").set<objydata::DateTime>(objydata::DateTime(blkTime, 0));
  object.attributeValue(blockClass.timeAttr, blockClass.value);
  blockClass.value.set(objydata::DateTime(blkTime, 0));
  
  //objydata::Utf8String value = objydata::createUtf8String();
  blockClass.stringValue.set(reinterpret_cast<char*>(hash));
//  object.attributeValue("hash").set<objydata::Utf8String>(value);
  object.attributeValue(blockClass.hashAttr, blockClass.value);
  //blockClass.value.set(reinterpret_cast<char*>(hash));
  blockClass.value.set(blockClass.stringValue);
  
  blockClass.stringValue.set(reinterpret_cast<char*>(prevBlockHash));
//  object.attributeValue("prevBlockHash").set<objydata::Utf8String>(value);
  object.attributeValue(blockClass.prevBlockHashAttr, blockClass.value);
  blockClass.value.set(blockClass.stringValue);

  blockClass.stringValue.set(reinterpret_cast<char*>(blockMerkleRoot));
//  object.attributeValue("merkleRootHash").set<objydata::Utf8String>(value);
  object.attributeValue(blockClass.merkleRootHashAttr, blockClass.value);
  blockClass.value.set(blockClass.stringValue);

  if (prevBlock)
  {
//    object.attributeValue("prevBlock").set<objydata::Reference>(prevBlock);
    object.attributeValue(blockClass.prevBlockAttr, blockClass.value);
    blockClass.value.set(prevBlock);
  }
  /**
                 .addAttribute("prevBlock", refSpec)
  **/
  return objydata::createReference(object);
}

/**
 * 
 * @param id
 * @param hash
 * @return 
 */
objydata::Reference ObjyAccess::createTransaction(
        int id, uint8_t* hash)
{
  objydata::Object object = objydata::createPersistentObject(transactionClass.classRef);
  
  //object.attributeValue("id").set<objy::uint_64>(id);
  object.attributeValue(transactionClass.idAttr, transactionClass.value);
  transactionClass.value.set(id);
  
  //objydata::Utf8String value = objydata::createUtf8String();
  transactionClass.stringValue.set(reinterpret_cast<char*>(hash));
  //object.attributeValue("hash").set<objydata::Utf8String>(value);
  object.attributeValue(transactionClass.hashAttr, transactionClass.value);
  transactionClass.value.set(transactionClass.stringValue);
  
  /**
  //.addAttribute(objydata::LogicalType.DateTime, "time")
   **/
  
  return objydata::createReference(object);
}

/**
 * 
 * @param id
 * @param upTxHash
 * @param upTrxRef
 * @param isCoinBase
 * @return 
 */
objydata::Reference ObjyAccess::createInput(
        int id, uint8_t* upTxHash, 
        objydata::Reference& upTrxRef, bool isCoinBase)
{
  objydata::Object object = objydata::createPersistentObject(inputClass.classRef);

  //object.attributeValue("id").set<objy::uint_64>(id);
  object.attributeValue(inputClass.idAttr, inputClass.value);
  inputClass.value.set(id);
  
  //object.attributeValue("isCoinBase").set<bool>(isCoinBase);
  object.attributeValue(inputClass.isCoinBaseAttr, inputClass.value);
  inputClass.value.set(isCoinBase);
  
  //objydata::Utf8String value = objydata::createUtf8String();
  inputClass.stringValue.set(reinterpret_cast<char*>(upTxHash));
  //object.attributeValue("upTxHash").set<objydata::Utf8String>(value);
  
  object.attributeValue(inputClass.upTxHashAttr, inputClass.value);
  inputClass.value.set(inputClass.stringValue);
  
  if (!isCoinBase && upTrxRef)
  {
    //object.attributeValue("upTx").set<objydata::Reference>(upTrxRef);
    object.attributeValue(inputClass.upTxAttr, inputClass.value);
    inputClass.value.set(upTrxRef);
  }

  return objydata::createReference(object);
}

/**
 * 
 * @param id
 * @param address
 * @param addressRef
 * @param trxValue
 * @return 
 */
objydata::Reference ObjyAccess::createOutput(
        int id, uint8_t* address, 
        objydata::Reference& addressRef, uint64_t trxValue)
{
  objydata::Object object = objydata::createPersistentObject(outputClass.classRef);
  
  //object.attributeValue("id").set<objy::uint_64>(id);
  object.attributeValue(outputClass.idAttr, outputClass.value);
  outputClass.value.set(id);
  
  //object.attributeValue("value").set<objy::uint_64>(trxValue);
  object.attributeValue(outputClass.valueAttr, outputClass.value);
  outputClass.value.set(trxValue);
  
  //objydata::Utf8String value = objydata::createUtf8String();
  outputClass.stringValue.set(reinterpret_cast<char*>(address));
  //object.attributeValue("addressHash").set<objydata::Utf8String>(value);
  object.attributeValue(outputClass.addressHashAttr, outputClass.value);
  outputClass.value.set(outputClass.stringValue);
  
  if (addressRef)
  {
    //object.attributeValue("address").set<objydata::Reference>(addressRef);
    object.attributeValue(outputClass.addressAttr, outputClass.value);
    outputClass.value.set(addressRef);
  }
  return objydata::createReference(object);
}

/**
 * 
 * @param hash
 * @return 
 */
objydata::Reference ObjyAccess::createAddress(uint8_t* hash)
{
  objydata::Object object = objydata::createPersistentObject(addressClass.classRef);
  
  //objydata::Utf8String value = objydata::createUtf8String();
  addressClass.stringValue.set(reinterpret_cast<char*>(hash));
  //object.attributeValue("hash").set<objydata::Utf8String>(value);
  
  object.attributeValue(addressClass.hashAttr, addressClass.value);
  addressClass.value.set(addressClass.stringValue);
  return objydata::createReference(object);
}

bool ObjyAccess::addTransactionToBlock(objydata::Reference& transaction, objydata::Reference& block)
{
//  objydata::List list = 
//          block.referencedObject().attributeValue("transactions").get<objydata::List>();
//  list.append(transaction);
  
  block.referencedObject().attributeValue("transactions").get<objydata::List>().append(transaction);
  return true;
}

bool ObjyAccess::addInputToTransaction(objydata::Reference& input, objydata::Reference& transaction)
{
  
//  objydata::List list = 
//          transaction.referencedObject().attributeValue("inputs").get<objydata::List>();
//  list.append(input);

  transaction.referencedObject().attributeValue("inputs").get<objydata::List>().append(input);
  return true;
}

bool ObjyAccess::addOutputToTransaction(objydata::Reference& output, objydata::Reference& transaction)
{
//  objydata::List list = 
//          transaction.referencedObject().attributeValue("outputs").get<objydata::List>();
//  list.append(output);
  
  transaction.referencedObject().attributeValue("outputs").get<objydata::List>().append(output);
  return true;
}
