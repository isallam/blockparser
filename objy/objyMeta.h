/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   objyMeta.h
 * Author: ibrahim
 *
 * Created on January 9, 2017, 10:46 AM
 */

#ifndef OBJYMETA_H
#define OBJYMETA_H

#include <objy/data/Data.h>

namespace objydata = objy::data;


// class names
const char* BlockClassName        = "Block";
const char* TransactionClassName  = "Transaction";
const char* InputClassName        = "Input";
const char* OutputClassName       = "Output";
const char* AddressClassName      = "Address";



const char* BlockIdAttr             = "m_Id";
const char* BlockVersionAttr        = "m_Version";
const char* BlockTimeAttr           = "m_Time"; 
const char* BlockHashAttr           = "m_Hash";
const char* BlockPrevBlockHashAttr  = "m_PrevBlockHash";
const char* BlockMerkleRootHashAttr = "m_MerkleRootHash";
const char* BlockPrevBlockAttr      = "m_PrevBlock";
const char* BlockNextBlockAttr      = "m_NextBlock";
const char* BlockTransactionsAttr   = "m_Transactions";



const char* TransactionIdAttr      = "m_Id";
const char* TransactionHashAttr    = "m_Hash";
const char* TransactionBlockAttr   = "m_Block";
const char* TransactionInputsAttr  = "m_Inputs";
const char* TransactionOutputsAttr = "m_Outputs";



const char* InputIdAttr          = "m_Id";
const char* InputIsCoinBaseAttr  = "m_IsCoinBase";
const char* InputUpTxHashAttr    = "m_UpTxHash"; 
const char* InputUpTxAttr        = "m_UpTx";
const char* InputTransactionAttr = "m_Transaction";



const char* OutputIdAttr          = "m_Id";
const char* OutputValueAttr       = "m_Value";
const char* OutputAddressHashAttr = "m_AddressHash"; 
const char* OutputAddressAttr     = "m_Address";
const char* OutputTransactionAttr = "m_Transaction";



const char* AddressHashAttr    = "m_Hash";
const char* AddressOutputsAttr = "m_Outputs";



#endif /* OBJYMETA_H */

