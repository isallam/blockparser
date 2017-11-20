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
const char* TagClassName          = "Tag";


const char* TagRefAttr            = "_tag";

const char* BlockIdAttr             = "_id";
const char* BlockVersionAttr        = "_version";
const char* BlockTimeAttr           = "_time"; 
const char* BlockHashAttr           = "_hash";
const char* BlockPrevBlockHashAttr  = "_prevBlockHash";
const char* BlockMerkleRootHashAttr = "_merkleRootHash";
const char* BlockPrevBlockAttr      = "_prevBlock";
const char* BlockNextBlockAttr      = "_nextBlock";
const char* BlockTransactionsAttr   = "_transactions";


const char* TransactionIdAttr         = "_id";
const char* TransactionHashAttr       = "_hash";
const char* TransactionTimeAttr       = "_mintTime";
const char* TransactionInValueAttr    = "_inValue";
const char* TransactionOutValueAttr   = "_outValue";
const char* TransactionBlockIdAttr    = "_blockId";
const char* TransactionNumInputsAttr  = "_numInputs";
const char* TransactionInputsAttr     = "_inputs";
const char* TransactionNumOutputsAttr = "_numOutputs";
const char* TransactionOutputsAttr    = "_outputs";


const char* InputIdAttr          = "_id";
const char* InputIsCoinBaseAttr  = "_isCoinBase";
const char* InputUpTxHashAttr    = "_upTxHash"; 
const char* InputUpTxAttr        = "_parent";
const char* InputTransactionAttr = "_transaction";


const char* OutputIdAttr          = "_id";
const char* OutputValueAttr       = "_value";
const char* OutputAddressHashAttr = "_addressHash"; 
const char* OutputAddressAttr     = "_address";
const char* OutputTransactionAttr = "_transaction";


const char* AddressHashAttr    = "_hash";
const char* AddressOutputsAttr = "_outputs";

const char* TagLabelAttr        = "_label";
const char* TagObjectAttr       = "_ref";


#endif /* OBJYMETA_H */

