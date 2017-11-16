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
const char* BlockClassName           = "Block";
const char* TransactionClassName     = "Transaction";
const char* GenTransactionClassName  = "GenTransaction";
//const char* InputClassName           = "Input";
const char* OutputClassName          = "Output";
const char* AddressClassName         = "Address";
const char* TagClassName             = "Tag";


const char* TagRefAttr              = "_tag";

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
const char* TransactionChildAttr      = "_child";
const char* TransactionNumOutputsAttr = "_numOutputs";
const char* TransactionOutputsAttr    = "_outputs";

const char* TransactionNumParentsAttr = "_numParents";
const char* TransactionParentsAttr    = "_parents";

const char* TransactionIsCoinBaseAttr = "_isCoinBase";


//const char* InputIdAttr          = "m_Id";
//const char* InputIsCoinBaseAttr  = "m_IsCoinBase";
//const char* InputUpTxHashAttr    = "m_UpTxHash"; 
//const char* InputUpTxAttr        = "m_UpTx";
//const char* InputTransactionAttr = "m_Transaction";
//

const char* OutputIdAttr          = "_id";
const char* OutputValueAttr       = "_value";
const char* OutputAddressHashAttr = "_addressHash"; 
const char* OutputNumAddressesAttr= "_numAddresses";
const char* OutputAddressAttr     = "_address";
const char* OutputTransactionAttr = "_transaction";


const char* AddressHashAttr       = "_hash";
const char* AddressNumOutputsAttr = "_numOutputs";
const char* AddressOutputsAttr    = "_outputs";

const char* TagLabelAttr        = "_label";
const char* TagObjectAttr       = "_ref";


#endif /* OBJYMETA_H */

