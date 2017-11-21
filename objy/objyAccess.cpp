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

#include <iostream>

#include "objyMeta.h"
#include "objyAccess.h"


ObjyAccess::ObjyAccess() {
  _stringVariable = objydata::createUtf8String();
}

ObjyAccess::~ObjyAccess() {
}

/**
 * 
 * @return 
 */
bool ObjyAccess::setupCache()
{
  // cache classes. although we don't need to do this ahead of using class proxies
  this->getClassProxy(BlockClassName);
  this->getClassProxy(TransactionClassName);
  this->getClassProxy(InputClassName);
  this->getClassProxy(OutputClassName);
  this->getClassProxy(AddressClassName);
  
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
        uint64_t id, int version, uint8_t* prevBlockHash, 
        uint8_t* blockMerkleRoot, long blkTime, uint8_t* hash, 
        objydata::Reference& prevBlock)
{
  //cout << "... creating block" << endl;

  ClassAccessor* classAccessor = this->getClassProxy(BlockClassName);
  
  objydata::Object object = classAccessor->createInstance();
  objy::data::Variable value;
  
  value.set(id);
  classAccessor->setAttributeValue(object, BlockIdAttr, value);
  
  value.set(version);
  classAccessor->setAttributeValue(object, BlockVersionAttr, value);

  value.set(objydata::DateTime(blkTime, 0));
  classAccessor->setAttributeValue(object, BlockTimeAttr, value);
  
  
  _stringVariable.set(reinterpret_cast<char*>(hash));
//  value.set(_stringVariable);
  classAccessor->setAttributeValue(object, BlockHashAttr, _stringVariable);
  
//  _stringVariable.set(reinterpret_cast<char*>(prevBlockHash));
////  value.set(_stringVariable);
//  classAccessor->setAttributeValue(object, BlockPrevBlockHashAttr, _stringVariable);

  _stringVariable.set(reinterpret_cast<char*>(blockMerkleRoot));
//  value.set(_stringVariable);
  classAccessor->setAttributeValue(object, BlockMerkleRootHashAttr, _stringVariable);

  objydata::Reference objectRef = objydata::createReference(object);
  
  if (prevBlock)
  {
    classAccessor->setReference(object, BlockPrevBlockAttr, prevBlock);
    // set next block on prevBlock
    const objydata::Object& prevBlockRef = prevBlock.referencedObject();
    classAccessor->setReference(prevBlockRef, BlockNextBlockAttr,
            objectRef);
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
        uint64_t id, uint8_t* hash, time_t blkTime,
        uint64_t blockId)
{
  //cout << "... creating transaction" << endl;

  ClassAccessor* classAccessor = this->getClassProxy(TransactionClassName);
  objydata::Object object = classAccessor->createInstance();

  objydata::Variable value;
  
  value.set(id);
  classAccessor->setAttributeValue(object, TransactionIdAttr, value);
  
  _stringVariable.set(reinterpret_cast<char*>(hash));
  //value.set(_stringVariable);
  classAccessor->setAttributeValue(object, TransactionHashAttr, _stringVariable);

  value.set(objydata::DateTime(blkTime, 0));
  classAccessor->setAttributeValue(object, TransactionTimeAttr, value);

  // add blockId to transaction.
  value.set(blockId);
  classAccessor->setAttributeValue(object, TransactionBlockIdAttr, value);
  
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
        uint64_t index, uint64_t inValue, ooId& upTrxRef, 
        uint64_t upTrxIndex, objydata::Reference& transaction)
{
  ClassAccessor* classAccessor = this->getClassProxy(InputClassName);
  objydata::Object object = classAccessor->createInstance();

  objydata::Variable value;
  
  value.set(index);
  classAccessor->setAttributeValue(object, InputIndexAttr, value);
 
  value.set(inValue);
  classAccessor->setAttributeValue(object, InputValueAttr, value);
 
//  _stringVariable.set(reinterpret_cast<char*>(upTxHash));
////  value.set(_stringVariable);
//  classAccessor->setAttributeValue(object, InputUpTxHashAttr, _stringVariable);
  
//  if (!isCoinBase/* && !upTrxRef*/)
  {
    classAccessor->setReference(object, InputUpTxAttr, 
                      objydata::referenceFor(upTrxRef));

    value.set(upTrxIndex);
    classAccessor->setAttributeValue(object, InputUpTxIndexAttr, value);
 
  }

  //printf("adding input to transaction: %s\n", objy::data::oidFor(transaction).sprint());
  classAccessor->setReference(object, InputTransactionAttr, transaction);
  
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
        uint64_t index, uint64_t outValue, objydata::Reference& addressRef, 
        objydata::Reference& transaction)
{
  ClassAccessor* classAccessor = this->getClassProxy(OutputClassName);
  objydata::Object object = classAccessor->createInstance();
  
  objydata::Variable value;
  
  value.set(index);
  classAccessor->setAttributeValue(object, OutputIndexAttr, value);

  value.set(outValue);
  classAccessor->setAttributeValue(object, OutputValueAttr, value);
  
//  _stringVariable.set(reinterpret_cast<char*>(address));
//  //value.set(_stringVariable);
//  classAccessor->setAttributeValue(object, OutputAddressHashAttr, _stringVariable);
  
  objydata::Reference objectRef = objydata::createReference(object);
  
  if (addressRef)
  {
    //std::cout << "addressRef: " << objydata::oidFor(addressRef).sprint() << std::endl;
    classAccessor->setReference(object, OutputAddressAttr, addressRef);

    // add output to address
    ClassAccessor* addressClassAccessor = this->getClassProxy(AddressClassName);
    addressClassAccessor->setReference(addressRef.referencedObject(), 
                            AddressOutputsAttr, objectRef);
    
    addressClassAccessor->incUInt64AttributeValue(addressRef.referencedObject(),
                            AddressNumOutputsAttr);
    
    addressClassAccessor->addToUInt64AttributeValue(addressRef.referencedObject(),
                            AddressOutValueAttr, outValue);
  }
  
  classAccessor->setReference(object, OutputTransactionAttr, transaction);

  return objectRef;
}

/**
 * 
 * @param hash
 * @return 
 */
objydata::Reference ObjyAccess::createAddress(uint8_t* hash)
{
  ClassAccessor* classAccessor = this->getClassProxy(AddressClassName);
  objydata::Object object = classAccessor->createInstance();
  
  objydata::Variable value;
  
  _stringVariable.set(reinterpret_cast<char*>(hash));
  //value.set(_stringVariable);
  classAccessor->setAttributeValue(object, AddressHashAttr, _stringVariable);

  return objydata::createReference(object);
}

bool ObjyAccess::addTransactionToBlock(objydata::Reference& transaction, objydata::Reference& block)
{

  ClassAccessor* classAccessor = this->getClassProxy(BlockClassName);
  
  //block.referencedObject().attributeValue("transactions").get<objydata::List>().append(transaction);
  classAccessor->setReference(block.referencedObject(), BlockTransactionsAttr, transaction);
  
  return true;
}

bool ObjyAccess::addInputToTransaction(objydata::Reference& input, objydata::Reference& transaction)
{
  ClassAccessor* classAccessor = this->getClassProxy(TransactionClassName);
  classAccessor->setReference(transaction.referencedObject(), TransactionInputsAttr, input);
  
  return true;
}

bool ObjyAccess::addOutputToTransaction(objydata::Reference& output, objydata::Reference& transaction)
{  
  ClassAccessor* classAccessor = this->getClassProxy(TransactionClassName);
  classAccessor->setReference(transaction.referencedObject(), TransactionOutputsAttr, output);

  return true;
}

bool ObjyAccess::updateTransactionValues(objydata::Reference& transaction, 
        bool isCoinBase, uint64_t trxInValue, uint64_t trxOutValue, 
        uint64_t numInputs, uint64_t numOutputs)
{
  ClassAccessor* classAccessor = this->getClassProxy(TransactionClassName);
  objydata::Variable value;
  
  value.set(isCoinBase);
  classAccessor->setAttributeValue(transaction.referencedObject(), 
          TransactionIsCoinBaseAttr, value);
  
  value.set(trxInValue);
  classAccessor->setAttributeValue(transaction.referencedObject(), 
          TransactionInValueAttr, value);
  value.set(trxOutValue);
  classAccessor->setAttributeValue(transaction.referencedObject(), 
          TransactionOutValueAttr, value);
  value.set(numInputs);
  classAccessor->setAttributeValue(transaction.referencedObject(), 
          TransactionNumInputsAttr, value); 
  value.set(numOutputs);
  classAccessor->setAttributeValue(transaction.referencedObject(), 
          TransactionNumOutputsAttr, value);
  }
