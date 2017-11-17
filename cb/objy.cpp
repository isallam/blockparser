
// Full Objy / Thingspan dump of the blockchain

#include <util.h>
#include <stdio.h>
#include <string>
#include <list>
#include <timer.h>
#include <common.h>
#include <errlog.h>
#include <option.h>
#include <callback.h>

#include <ooObjy.h>
#include <objy/Configuration.h>
#include <objy/Tools.h>
#include <objy/db/Connection.h>
#include <objy/db/Transaction.h>
#include <objy/db/TransactionScope.h>
#include <objy/data/Data.h>
#include <objy/data/List.h>

#include "../objy/objyAccess.h"

namespace objydb = objy::db;
namespace objydata = objy::data;
namespace objyconfig = objy::configuration;


static uint8_t empty[kSHA256ByteSize] = {0x42};
static uint8_t emptyAddressKey[kRIPEMD160ByteSize] = {0x52};


typedef GoogMap<Hash256, uint64_t, Hash256Hasher, Hash256Equal>::Map OutputMap;

typedef GoogMap<Hash256, ooId, Hash256Hasher, Hash256Equal>::Map TrxMap;

typedef GoogMap<Hash160, ooId, Hash160Hasher, Hash160Equal>::Map AddrMap;

struct ObjyDump : public Callback {

  // objy stuff
  std::string fdname;
  objydb::Connection* connection;
  objydb::Transaction* trx;

  uint64_t blkID;
  uint64_t trxID;
  uint8_t trxHash[2 * kSHA256ByteSize + 1];
  uint64_t inputID;
  uint64_t outputID;
  uint64_t numCommits;
      

  bool isCoinBase;
  int64_t cutoffBlock;

  TrxMap trxMap;
  AddrMap addrMap;
  //OutputMap outputMap;
  std::list<ooId> inputList;

  optparse::OptionParser parser;
  ObjyAccess objyAccess;

  bool enoughProcessing;

  // cached objects
  objydata::Reference previousBlock;
  objydata::Reference currentBlock;
  objydata::Reference currentTransaction;
  // cached data
  uint64_t currentBlockTime;
  uint64_t currentTrxInValue;
  uint64_t currentTrxOutValue;

  ObjyDump() {
    parser
            .usage("[options] [list of addresses to restrict output to]")
            .version("")
            .description("create an Objy dump of the blockchain")
            .epilog("")
            ;
    parser
            .add_option("-a", "--atBlock")
            .action("store")
            .type("int")
            .set_default(-1)
            .help("stop dump at block <block> (default: all)")
            ;
  }

  virtual const char *name() const {
    return "objydump";
  }

  virtual const optparse::OptionParser *optionParser() const {
    return &parser;
  }

  virtual bool needUpstream() const {
    return true;
  }

  virtual void aliases(
          std::vector<const char*> &v
          ) const {
    v.push_back("objydump");
  }

  virtual int init(
          int argc,
          const char *argv[]
          ) {
    blkID = 0;
    trxID = 0;
    inputID = 0;
    outputID = 0;
    numCommits = 0;  

    // TBD... boot file is hard coded for now, will be params later
    fdname = "../data/bitcoin.boot";
    ooObjy::setLoggingOptions(oocLogAll, true, false, "../logs");
    ooObjy::startup(24);

    objyconfig::ConfigurationManager* cfgMgr = objyconfig::ConfigurationManager::getInstance();
    cfgMgr->enableConfiguration(true, 0, 0, 0, 0);

    //connection = ooObjy::getConnection(bootfile);
    connection = objydb::Connection::connect(fdname.c_str());

    try {
      trx = new objydb::Transaction(objydb::OpenMode::Update, "write");
      trx->commit();
    } catch (ooKernelException& e) {
      errFatal(e.what());
      return 1;
    } catch (ooBaseException& e) {
      errFatal(e.what());
      return 1;
    }

    enoughProcessing = false;

    static uint64_t sz = 32 * 1000 * 1000;
    //outputMap.setEmptyKey(empty);
    //outputMap.resize(sz);

    trxMap.setEmptyKey(empty);
    trxMap.resize(sz);

    addrMap.setEmptyKey(emptyAddressKey);
    addrMap.resize(sz);

    optparse::Values &values = parser.parse_args(argc, argv);
    cutoffBlock = values.get("atBlock").asInt64();

    info("dumping the blockchain ...");

    trx->start(objydb::OpenMode::Update);

    objyAccess.setupCache(); // cache schema and attributes for later.

    return 0;
  }

  virtual void startBlock(
          const Block *b,
          uint64_t
          ) {
    if (0 <= cutoffBlock && cutoffBlock < b->height) {
      wrapup();
    }

    static auto last = Timer::usecs();

    const uint8_t * p = b->chunk->getData();
    uint8_t blockHash[kSHA256ByteSize];
    sha256Twice(blockHash, p, 80);

    LOAD(uint32_t, version, p);
    LOAD(uint256_t, prevBlkHash, p);
    LOAD(uint256_t, blkMerkleRoot, p);
    LOAD(uint32_t, blkTime, p);

    size_t size = kSHA256ByteSize;
    uint8_t* bufBlockHash = (uint8_t*) alloca(2 * size + 1);
    uint8_t* bufPrevBlockHash = (uint8_t*) alloca(2 * size + 1);
    uint8_t* bufBlockMerkleRoot = (uint8_t*) alloca(2 * size + 1);
    toHex(bufBlockHash, blockHash);
    toHex(bufPrevBlockHash, prevBlkHash.v);
    toHex(bufBlockMerkleRoot, blkMerkleRoot.v);

    blkID = b->height - 1;
    // id BIGINT PRIMARY KEY
    // hash BINARY(32)
    // time BIGINT

    if (currentBlock)
      previousBlock = currentBlock;

    currentBlock = objyAccess.createBlock(blkID, version, bufPrevBlockHash,
            bufBlockMerkleRoot, blkTime, bufBlockHash, previousBlock);

    currentBlockTime = blkTime;

    int commitEvery = 50000; // commit every 100K transactions once we reach
                              // a block boundary.

    if ((trxID / commitEvery) > numCommits) {
      trx->commit();
      numCommits++;
      auto now = Timer::usecs();
      auto elapsedSinceLastTime = now - last;
      auto elapsedInMSec = (elapsedSinceLastTime*1e-3);
      info(" # block:%8" PRIu64 " "
              "- # transactions:%12" PRIu64 " - time: %10.2f msec.    \r",
              b->height,
              trxID, 
              elapsedInMSec);
      last = now;
      trx->start(objydb::OpenMode::Update);

      //enoughProcessing = true;
    }
  }

  virtual bool done() {
    return enoughProcessing;
  }

  virtual void startTX(
          const uint8_t *p,
          const uint8_t *hash
          ) {
    // id BIGINT PRIMARY KEY
    // hash BINARY(32)
    // blockID BIGINT
    
    //size_t size = kSHA256ByteSize;
    //uint8_t* buf = (uint8_t*) alloca(2 * size + 1);
    //toHex(buf, hash);
    toHex(trxHash, hash);
    
    currentTrxInValue = 0;
    currentTrxOutValue = 0;
    
    //memcpy(trxHash, buf, kSHA256ByteSize);
//   uint8_t *key = allocHash256();
//   memcpy(key, hash, kSHA256ByteSize);
//
//    currentTransaction = objyAccess.createTransaction(trxID, trxHash, currentBlockTime,
//            blkID);
//    trxMap[key] = objydata::oidFor(currentTransaction);

    //info("... START Trx...\r");
    currentTransaction = 0;
  }

  virtual void endTX(
          const uint8_t *p
          ) {
    //info("... END Trx...\r");
    //LOAD(uint32_t, lockTime, p);
    // update TrxInValue and TrxOutValue
//    if (!currentTransaction)
//    {
//      info("CurrentTransaction is NULL.... %d\n", trxID);
//    }
//    else {
//      info("CurrentTransaction %d, in:%lf, out:%lf\n", trxID, 
//              satoshisToNormaForm(currentTrxInValue), 
//              satoshisToNormaForm(currentTrxOutValue));
//    }
    objyAccess.updateTransactionValues(currentTransaction, 
                    satoshisToNormaForm(currentTrxInValue), 
                    satoshisToNormaForm(currentTrxOutValue));
    if (inputList.size() > 0)
      objyAccess.addInputList(currentTransaction, inputList);
    objyAccess.addTransactionToBlock(currentTransaction, currentBlock);
    trxID++;
  }

  virtual void startInputs(
          const uint8_t *p
          ) {
    //info("... START Inputs...\r");

    inputID = 0;
    isCoinBase = false;
    inputList.clear();
  }

  // Called when a TX input is encountered

  virtual void startInput(
          const uint8_t *p
          ) {
    static uint256_t gNullHash;
    LOAD(uint256_t, upTXHash, p);
    LOAD(uint32_t, upOutputIndex, p);
    LOAD_VARINT(inputScriptSize, p);

    //info("... START Input...\r");
    
    // later...
    //        printf("%sscript = '\n", spaces);
    //            pop();
    //                showScript(p, inputScriptSize, 0, (const char *)spaces);
    //            push();
    //        printf("%s'\n", spaces);

    isCoinBase = (0 == memcmp(gNullHash.v, upTXHash.v, sizeof (gNullHash)));
    // later.
    if (isCoinBase) {
      uint64_t reward = getBaseReward(blkID);
      //canonicalHexDump(p, inputScriptSize, "        ");
      currentTrxInValue += reward;
    }
    
    if (!currentTransaction)
    {
      uint8_t *key = allocHash256();
      memcpy(key, trxHash, kSHA256ByteSize);
      //printf("trxHash as    :%s\n", trxHash);
      
      if (isCoinBase) {
        currentTransaction = objyAccess.createGenTransaction(trxID, trxHash, 
                currentBlockTime, blkID);
      } else {
        currentTransaction = objyAccess.createTransaction(trxID, trxHash, 
                currentBlockTime, blkID);
      }
      //printf("trxHash as key:%s\n", key);
      trxMap[key] = objydata::oidFor(currentTransaction);
    }
//      objydata::Reference input = objyAccess.createInput(inputID, NULL, upTrxRef,
//              isCoinBase, currentTransaction);
//      objyAccess.addInputToTransaction(input, currentTransaction);

  }
  
    virtual void edge(
        uint64_t      value,
        const uint8_t *upTXHash,
        uint64_t      outputIndex,
        const uint8_t *outputScript,
        uint64_t      outputScriptSize,
        const uint8_t *downTXHash,
        uint64_t      inputIndex,
        const uint8_t *inputScript,
        uint64_t      inputScriptSize
    ) {
        // this is called when there is input (not generated) that connect to output
        uint8_t buf[1 + 2*kSHA256ByteSize];
        toHex(buf, upTXHash);
//        printf("upTXHash toHex(): %s\n", buf);
//        printf("upTXHash mapKey : %s\n", lookupKey);
        currentTrxInValue += value;

        ooId upTrxRef;

        TrxMap::iterator val = trxMap.find(buf);
        if (trxMap.end() == val) {
           printf("trxMap size:%ld\n", trxMap.size());
           printf("unconnected input, upTXHash:%s\n", buf);
         } else {
           //printf(" >>> FOUND upTrxRef\n");
           upTrxRef = val->second;
         }

        inputList.push_back(upTrxRef);
//        objydata::Reference input = objyAccess.createInput(inputIndex, buf, upTrxRef,
//                false, currentTransaction);
//
//        objyAccess.addInputToTransaction(input, currentTransaction);
   }
  
  // Called when at the end of a TX input

  virtual void endInput(
          const uint8_t *p
          ) {
    ++inputID;
  }

  virtual void endInputs(
          const uint8_t *p
          ) {
    //info("... END Inputs...\r");
  }

  virtual void endOutput(
          const uint8_t *p,
          uint64_t value,
          const uint8_t *txHash,
          uint64_t outputIndex,
          const uint8_t *outputScript,
          uint64_t outputScriptSize
          ) {
    uint8_t address[40];
    address[0] = 'X';
    address[1] = 0;

    uint8_t addrType[3];
    uint160_t pubKeyHash;
    int type = solveOutputScript(
            pubKeyHash.v,
            outputScript,
            outputScriptSize,
            addrType
            );

    objydata::Reference addressRef;

    if (likely(0 <= type)) {
      hash160ToAddr(
              address,
              pubKeyHash.v,
              false,
              addrType[0]
              );
      // find out if we already seen such address.
      AddrMap::iterator val = addrMap.find(pubKeyHash.v);
      if (addrMap.end() == val) {
        // create an address object
        addressRef = objyAccess.createAddress(address);
        // add it to the map
        uint8_t *key = allocHash160();
        memcpy(key, pubKeyHash.v, kRIPEMD160ByteSize);
        addrMap[key] = objydata::oidFor(addressRef);
      } else {
        addressRef = objydata::referenceFor(val->second);
      }
    }

    // id BIGINT PRIMARY KEY
    // dstAddress CHAR(36)
    // value BIGINT
    // txID BIGINT
    // offset INT

    currentTrxOutValue += value;
    objydata::Reference output = objyAccess.createOutput(outputIndex, address,
            addressRef, satoshisToNormaForm(value), currentTransaction);
    objyAccess.addOutputToTransaction(output, currentTransaction);

    //uint32_t oi = outputIndex;
    //uint8_t *h = allocHash256();
    //memcpy(h, txHash, kSHA256ByteSize);

    //uintptr_t ih = reinterpret_cast<uintptr_t> (h);
    //uint32_t *h32 = reinterpret_cast<uint32_t*> (ih);
    //h32[0] ^= oi;

    //outputMap[h] = outputID++;
    outputID++;
  }

  virtual void wrapup() {

    if (trx->getOpenMode() != objydb::OpenMode::NotOpened)
      trx->commit();

    info("done\n");
  }
};

static ObjyDump objyDump;


