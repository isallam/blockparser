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

#include "objyMeta.h"
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
  // -----------------------------------------------------------
  // Some needed specs for various references and collections...
  // -----------------------------------------------------------
  
  // Block Reference
   objydata::DataSpecificationHandle blockRefSpec = 
           objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()    
          .setReferencedClass(BlockClassName)
          .build();

  // transaction Reference 
  objydata::DataSpecificationHandle transactionRefSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(TransactionClassName)
          .build();
                  
   // transaction List                 
  objydata::DataSpecificationHandle transactionsSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(transactionRefSpec)
          //.setCollectionName("SegmentedArray")
          .build();

  // input reference
  objydata::DataSpecificationHandle inputsRefSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(InputClassName)
          .build();

    // Input list
  objydata::DataSpecificationHandle inputsSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(inputsRefSpec)
          //.setCollectionName("SegmentedArray")
          .build();
  
  // Output reference
  objydata::DataSpecificationHandle outputRefSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(OutputClassName)
          .build();
  
  // Output list
  objydata::DataSpecificationHandle outputsSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(outputRefSpec)
          //.setCollectionName("SegmentedArray")
          .build();

  
  // Output list for address
  objydata::DataSpecificationHandle outputsForAddressSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(outputRefSpec)
          //.setCollectionName("SegmentedArray")
          .build();
  
  // Address reference
  objydata::DataSpecificationHandle addressRefSpec = 
           objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(AddressClassName)
          .build();
  
  // Embedded string spec (currently not used)
  objydata::DataSpecificationHandle stringSpec = 
        objydata::DataSpecificationBuilder<objydata::LogicalType::String>()
    .setEncoding(objydata::StringEncoding::Utf8)
    .setStorage(objydata::StringStorage::Fixed)
    .setFixedLength(66)
    .build();

  
  // -------------------
  // Block Class
  // -------------------
  objydata::Class blockClass = 
                objydata::ClassBuilder(BlockClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>(BlockIdAttr)
                 .addAttribute<objy::int_32>(BlockVersionAttr)
                 .addAttribute<objydata::DateTime>(BlockTimeAttr)
                 .addAttribute<objydata::ByteString /*Utf8String*/>(BlockHashAttr)
                 .addAttribute<objydata::ByteString /*Utf8String*/>(BlockPrevBlockHashAttr)
                 .addAttribute<objydata::ByteString /*Utf8String*/>(BlockMerkleRootHashAttr)
                 //.addAttribute("hash", stringSpec)
                 //.addAttribute("prevBlockHash", stringSpec)
                 //.addAttribute("merkleRootHash", stringSpec)
                 .addAttribute(BlockPrevBlockAttr, blockRefSpec)
                 .addAttribute(BlockNextBlockAttr, blockRefSpec)
                 .addAttribute(BlockTransactionsAttr, transactionsSpec)
                 .build();

  
  // -------------------
  // Transaction Class
  // -------------------
  objydata::Class transactionClass = 
                 objydata::ClassBuilder(TransactionClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>(TransactionIdAttr)
                 //.addAttribute(objydata::LogicalType.DateTime, "time")
                 .addAttribute<objydata::ByteString /*Utf8String*/>(TransactionHashAttr)
                 .addAttribute(TransactionBlockAttr, blockRefSpec)
                 .addAttribute(TransactionInputsAttr, inputsSpec)
                 .addAttribute(TransactionOutputsAttr, outputsSpec)
                 .build();
  
  // -------------------
  // Input Class
  // -------------------
  objydata::Class inputClass = 
                 objydata::ClassBuilder(InputClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>(InputIdAttr)
                 .addAttribute(InputTransactionAttr, transactionRefSpec)
                 .addAttribute<objydata::ByteString /*Utf8String*/>(InputUpTxHashAttr)
                 .addAttribute(InputUpTxAttr, transactionRefSpec)
                 .addAttribute<bool>(InputIsCoinBaseAttr)
                 .build();

  // -------------------
  // Address Class
  // -------------------
  objydata::Class addressClass = 
                 objydata::ClassBuilder(AddressClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objydata::ByteString/*Utf8String*/>(AddressHashAttr)
                 .addAttribute(AddressOutputsAttr, outputsForAddressSpec)
                 .build();
  
  // -------------------
  // output Class
  // -------------------
  objydata::Class outputClass = 
                 objydata::ClassBuilder(OutputClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>(OutputIdAttr)
                 .addAttribute(OutputTransactionAttr, transactionRefSpec)
                 .addAttribute<objydata::ByteString/*Utf8String*/>(OutputAddressHashAttr)
                 .addAttribute(OutputAddressAttr, addressRefSpec)
                 .addAttribute<objy::uint_64>(OutputValueAttr)
                 .build();
  
  
  objyschema::SchemaProvider* provider = objyschema::SchemaProvider::defaultPersistentProvider();
  provider->represent(blockClass);
  provider->represent(transactionClass);
  provider->represent(inputClass);
  provider->represent(outputClass);
  provider->represent(addressClass);
  
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
  blockClass.idAttr = blockClass.classRef.lookupAttribute(BlockIdAttr);
  blockClass.versionAttr = blockClass.classRef.lookupAttribute(BlockVersionAttr);
  blockClass.timeAttr = blockClass.classRef.lookupAttribute(BlockTimeAttr); 
  blockClass.hashAttr = blockClass.classRef.lookupAttribute(BlockHashAttr);
  blockClass.prevBlockHashAttr = blockClass.classRef.lookupAttribute(BlockPrevBlockHashAttr);
  blockClass.merkleRootHashAttr = blockClass.classRef.lookupAttribute(BlockMerkleRootHashAttr);
  blockClass.prevBlockAttr = blockClass.classRef.lookupAttribute(BlockPrevBlockAttr);
  blockClass.nextBlockAttr = blockClass.classRef.lookupAttribute(BlockNextBlockAttr);
  blockClass.transactionsAttr = blockClass.classRef.lookupAttribute(BlockTransactionsAttr);
  

  transactionClass.classRef = objydata::lookupClass(TransactionClassName);
  transactionClass.idAttr = transactionClass.classRef.lookupAttribute(TransactionIdAttr);
  transactionClass.hashAttr = transactionClass.classRef.lookupAttribute(TransactionHashAttr);
  transactionClass.blockAttr = transactionClass.classRef.lookupAttribute(TransactionBlockAttr);
  transactionClass.inputsAttr = transactionClass.classRef.lookupAttribute(TransactionInputsAttr);
  transactionClass.outputsAttr = transactionClass.classRef.lookupAttribute(TransactionOutputsAttr);
    
  inputClass.classRef = objydata::lookupClass(InputClassName);
  inputClass.idAttr = inputClass.classRef.lookupAttribute(InputIdAttr);
  inputClass.isCoinBaseAttr = inputClass.classRef.lookupAttribute(InputIsCoinBaseAttr);
  inputClass.upTxHashAttr = inputClass.classRef.lookupAttribute(InputUpTxHashAttr); 
  inputClass.upTxAttr = inputClass.classRef.lookupAttribute(InputUpTxAttr);
  inputClass.transactionAttr = inputClass.classRef.lookupAttribute(InputTransactionAttr);
  
    
  outputClass.classRef = objydata::lookupClass(OutputClassName);
  outputClass.idAttr = outputClass.classRef.lookupAttribute(OutputIdAttr);
  outputClass.valueAttr = outputClass.classRef.lookupAttribute(OutputValueAttr);
  outputClass.addressHashAttr = outputClass.classRef.lookupAttribute(OutputAddressHashAttr); 
  outputClass.addressAttr = outputClass.classRef.lookupAttribute(OutputAddressAttr);
  outputClass.transactionAttr = outputClass.classRef.lookupAttribute(OutputTransactionAttr);
    
  addressClass.classRef = objydata::lookupClass(AddressClassName);
  addressClass.hashAttr = addressClass.classRef.lookupAttribute(AddressHashAttr);
  addressClass.outputsAttr = addressClass.classRef.lookupAttribute(AddressOutputsAttr);
  
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
  
  object.attributeValue(blockClass.idAttr, blockClass.value);
  blockClass.value.set(id);
  
  object.attributeValue(blockClass.versionAttr, blockClass.value);
  blockClass.value.set(version);
  
  object.attributeValue(blockClass.timeAttr, blockClass.value);
  blockClass.value.set(objydata::DateTime(blkTime, 0));
  
  blockClass.stringValue.set(reinterpret_cast<char*>(hash));
  object.attributeValue(blockClass.hashAttr, blockClass.value);
  blockClass.value.set(blockClass.stringValue);
  
  blockClass.stringValue.set(reinterpret_cast<char*>(prevBlockHash));
  object.attributeValue(blockClass.prevBlockHashAttr, blockClass.value);
  blockClass.value.set(blockClass.stringValue);

  blockClass.stringValue.set(reinterpret_cast<char*>(blockMerkleRoot));
  object.attributeValue(blockClass.merkleRootHashAttr, blockClass.value);
  blockClass.value.set(blockClass.stringValue);

  objydata::Reference objectRef = objydata::createReference(object);
  
  if (prevBlock)
  {
    object.attributeValue(blockClass.prevBlockAttr, blockClass.value);
    blockClass.value.set(prevBlock);
    // set next block on prevBlock
    prevBlock.referencedObject().attributeValue(blockClass.nextBlockAttr, blockClass.value);
    blockClass.value.set<objydata::Reference>(objectRef);
  }
  /**
                 .addAttribute("prevBlock", refSpec)
  **/
  return objectRef;
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

  object.attributeValue(inputClass.idAttr, inputClass.value);
  inputClass.value.set(id);
  
  object.attributeValue(inputClass.isCoinBaseAttr, inputClass.value);
  inputClass.value.set(isCoinBase);
  
  inputClass.stringValue.set(reinterpret_cast<char*>(upTxHash));
  
  object.attributeValue(inputClass.upTxHashAttr, inputClass.value);
  inputClass.value.set(inputClass.stringValue);
  
  if (!isCoinBase && upTrxRef)
  {
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
  
  object.attributeValue(outputClass.idAttr, outputClass.value);
  outputClass.value.set(id);
  
  object.attributeValue(outputClass.valueAttr, outputClass.value);
  outputClass.value.set(trxValue);
  
  outputClass.stringValue.set(reinterpret_cast<char*>(address));
  object.attributeValue(outputClass.addressHashAttr, outputClass.value);
  outputClass.value.set(outputClass.stringValue);
  
  objydata::Reference objectRef = objydata::createReference(object);
  
  if (addressRef)
  {
    object.attributeValue(outputClass.addressAttr, outputClass.value);
    outputClass.value.set(addressRef);
    
    // add output to address
    addressRef.referencedObject().attributeValue(addressClass.outputsAttr, addressClass.value);
    addressClass.value.get<objydata::List>().append(objectRef);

    
  }
  return objectRef;
}

/**
 * 
 * @param hash
 * @return 
 */
objydata::Reference ObjyAccess::createAddress(uint8_t* hash)
{
  objydata::Object object = objydata::createPersistentObject(addressClass.classRef);
  
  addressClass.stringValue.set(reinterpret_cast<char*>(hash));
  object.attributeValue(addressClass.hashAttr, addressClass.value);
  addressClass.value.set(addressClass.stringValue);
  
  return objydata::createReference(object);
}

bool ObjyAccess::addTransactionToBlock(objydata::Reference& transaction, objydata::Reference& block)
{

  //block.referencedObject().attributeValue("transactions").get<objydata::List>().append(transaction);
  block.referencedObject().attributeValue(blockClass.transactionsAttr, blockClass.value);
  blockClass.value.get<objydata::List>().append(transaction);
  
  // add block to transaction.
  transaction.referencedObject().attributeValue(transactionClass.blockAttr, transactionClass.value);
  transactionClass.value.set<objydata::Reference>(block);
  
  return true;
}

bool ObjyAccess::addInputToTransaction(objydata::Reference& input, objydata::Reference& transaction)
{
  //transaction.referencedObject().attributeValue("inputs").get<objydata::List>().append(input);
  transaction.referencedObject().attributeValue(transactionClass.inputsAttr, transactionClass.value);
  transactionClass.value.get<objydata::List>().append(input);
  
  // add transaction to input
  input.referencedObject().attributeValue(inputClass.transactionAttr, inputClass.value);
  inputClass.value.set<objydata::Reference>(transaction);
  
  return true;
}

bool ObjyAccess::addOutputToTransaction(objydata::Reference& output, objydata::Reference& transaction)
{  
  //transaction.referencedObject().attributeValue("outputs").get<objydata::List>().append(output);
  transaction.referencedObject().attributeValue(transactionClass.outputsAttr, transactionClass.value);
  transactionClass.value.get<objydata::List>().append(output);
  
  // add transaction to input
  output.referencedObject().attributeValue(inputClass.transactionAttr, outputClass.value);
  outputClass.value.set<objydata::Reference>(transaction);

  return true;
}
