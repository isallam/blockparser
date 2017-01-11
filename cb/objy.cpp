
// Full Objy / Thingspan dump of the blockchain

#include <util.h>
#include <stdio.h>
#include <string>
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


static uint8_t empty[kSHA256ByteSize] = { 0x42 };
static uint8_t emptyAddressKey[kRIPEMD160ByteSize] = { 0x52 };


typedef GoogMap<Hash256, uint64_t, Hash256Hasher, Hash256Equal>::Map OutputMap;

typedef GoogMap<Hash256, ooId, Hash256Hasher, Hash256Equal>::Map TrxMap;

typedef GoogMap<Hash160, ooId, Hash160Hasher, Hash160Equal>::Map AddrMap;






struct ObjyDump : public Callback {

    FILE *objySimFile;
    FILE *objyEdgeSimFile;
    
    // objy stuff
    std::string fdname;
    objydb::Connection* connection;
	objydb::Transaction* trx;
    
    uint64_t txID;
    uint64_t blkID;
    uint64_t inputID;
    uint64_t outputID;
    
    bool isCoinBase;
    int64_t cutoffBlock;
    
    TrxMap    trxMap;
    AddrMap   addrMap;
    //OutputMap outputMap;

    optparse::OptionParser parser;
    ObjyAccess objyAccess;
    
    bool dumpToFiles;
    bool enoughProcessing;
    
    // cached objects
    objydata::Reference previousBlock;
    objydata::Reference currentBlock;
    objydata::Reference currentTransaction;
    
    

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

    virtual const char                   *name() const         { return "objydump"; }
    virtual const optparse::OptionParser *optionParser() const { return &parser;   }
    virtual bool                       needUpstream() const    { return true;      }

    virtual void aliases(
        std::vector<const char*> &v
    ) const {
        v.push_back("objydump");
    }

    virtual int init(
        int argc,
        const char *argv[]
    ) {
        txID = -1;
        blkID = 0;
        inputID = 0;
        outputID = 0;
        
        // TBD... boot file is hard coded for now, will be params later
        fdname = "../data/bitcoin.boot";
        ooObjy::setLoggingOptions(oocLogAll, true, false, "../logs");
        ooObjy::startup(24);
        
        objyconfig::ConfigurationManager* cfgMgr = objyconfig::ConfigurationManager::getInstance();
        cfgMgr->enableConfiguration(true, 0, 0, 0, 0);
        
        //connection = ooObjy::getConnection(bootfile);
		connection = objydb::Connection::connect(fdname.c_str());
   
        try {
          trx = new objydb::Transaction(objydb::OpenMode::Update, "write_session");
          // create schema 
          bool bRet = objyAccess.createSchema();
          
          trx->commit();
	  // importPlacement
          objy::tool::ToolParameters params;
          params.add("infile", "../data/bitcoin.pmd");
          params.add("bootfile", fdname.c_str());

          objy::tool::StringToolOutputSink sink;
          objy::tool::Tool::execute("importPlacement", params, &sink);
        } 
        catch (ooKernelException& e)
        {
          errFatal(e.what());
          return 1;
        }
        catch (ooBaseException& e)
        {
          errFatal(e.what());
          return 1;
        }

        dumpToFiles = false;
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

        objySimFile = fopen("objySimFile.txt", "w");
        if(!objySimFile) sysErrFatal("couldn't open file objySimFile.txt for writing\n");
        objyEdgeSimFile = fopen("objyEdgeSimFile.txt", "w");
        if(!objyEdgeSimFile) sysErrFatal("couldn't open file objyEdgeSimFile.txt for writing\n");

        trx->start(objydb::OpenMode::Update);
        
        objyAccess.setupCache(); // cache schema and attributes for later.
        
        return 0;
    }

    virtual void startBlock(
        const Block *b,
        uint64_t
    ) {
        if(0<=cutoffBlock && cutoffBlock<b->height) {
            wrapup();
        }

        const uint8_t * p = b->chunk->getData();
        uint8_t blockHash[kSHA256ByteSize];
        sha256Twice(blockHash, p, 80);

        LOAD(uint32_t, version, p);
        LOAD(uint256_t, prevBlkHash, p);
        LOAD(uint256_t, blkMerkleRoot, p);
        LOAD(uint32_t, blkTime, p);

        size_t size = kSHA256ByteSize;
        uint8_t* bufBlockHash = (uint8_t*)alloca(2*size + 1);
        uint8_t* bufPrevBlockHash = (uint8_t*)alloca(2*size + 1);
        uint8_t* bufBlockMerkleRoot = (uint8_t*)alloca(2*size + 1);
        toHex(bufBlockHash, blockHash);
        toHex(bufPrevBlockHash, prevBlkHash.v);
        toHex(bufBlockMerkleRoot, blkMerkleRoot.v);

        blkID = b->height-1;
        // id BIGINT PRIMARY KEY
        // hash BINARY(32)
        // time BIGINT
        if (dumpToFiles)
          fprintf(objySimFile, 
                "Block(ID:%" PRIu64 ", "
                "VERSION:%" PRIu64 ","
                "PrevBlockHash:%s,"
                "BlockMerkleRoot:%s,"
                "TIME:%" PRIu64 ","
                "BlockHash:%s)\n", 
                blkID,
                (uint64_t)version,
                bufPrevBlockHash, 
                bufBlockMerkleRoot,
                (uint64_t)blkTime,
                bufBlockHash);

        if (currentBlock)
          previousBlock = currentBlock;
        
        currentBlock = objyAccess.createBlock(blkID, version, bufPrevBlockHash, 
                bufBlockMerkleRoot, blkTime, bufBlockHash, previousBlock);
      
        int commitEvery = 1000; 
	if (b->height > 100000)
            commitEvery = 500; 
	if (b->height > 200000)
            commitEvery = 100; 
	
        if(0==(b->height)%commitEvery) {
            fprintf(
                stderr,
                "block=%8" PRIu64 " "
                "nbOutputs=%8" PRIu64 "\r",
                b->height,
                outputID //outputMap.size()
            );
            trx->commit();
            trx->start(objydb::OpenMode::Update);
//            if (0==(b->height)%2000) {
//              trx->abort();
////              trx->start(objydb::OpenMode::Update);
////            }
            
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
        size_t size = kSHA256ByteSize;
        uint8_t* buf = (uint8_t*)alloca(2*size + 1);
        toHex(buf, hash);

        if (dumpToFiles)
          fprintf(objySimFile, 
              "  Trx(TXID:%" PRIu64 ", "
                "BLKID:%" PRIu64 ", "
                "HASH:%s)\n",
                ++txID,
                blkID,
                buf);
        
        uint8_t *key = allocHash256();
        memcpy(key, hash, kSHA256ByteSize);
        
        currentTransaction = objyAccess.createTransaction(txID, buf);
        trxMap[key] = objydata::oidFor(currentTransaction);
        
        objyAccess.addTransactionToBlock(currentTransaction, currentBlock);

    }

    virtual void endTX(
        const uint8_t *p
    ) {
        LOAD(uint32_t, lockTime, p);
        if (dumpToFiles)
          fprintf(objySimFile, 
                "  EndTX(    lockTime = %" PRIu32 "\n", 
                (uint32_t)lockTime);
    }
        
    virtual void startInputs(
        const uint8_t *p
    ) {
        inputID = 0;
        isCoinBase = false;
    }
    
        // Called when a TX input is encountered
    virtual void startInput(
        const uint8_t *p
    ) {
        static uint256_t gNullHash;
        LOAD(uint256_t, upTXHash, p);
        LOAD(uint32_t, upOutputIndex, p);
        LOAD_VARINT(inputScriptSize, p);

        // later...
//        printf("%sscript = '\n", spaces);
//            pop();
//                showScript(p, inputScriptSize, 0, (const char *)spaces);
//            push();
//        printf("%s'\n", spaces);

        isCoinBase = (0==memcmp(gNullHash.v, upTXHash.v, sizeof(gNullHash)));
        // later.
        if(isCoinBase) {
//            uint64_t value = getBaseReward(currBlock);
//            printf("%sisCoinBase = true\n", spaces);
//            printf(
//                "%svalue = %" PRIu64 " # %.08f\n",
//                spaces,
//                value,
//                satoshisToNormaForm(value)
//            );
//            printf("%scoinBase = '\n", spaces);
//            push();
//                canonicalHexDump(
//                    p,
//                    inputScriptSize,
//                    (const char *)spaces
//                );
//            pop();
//            printf("%s'\n", spaces);
        }
       
        size_t size = kSHA256ByteSize;
        uint8_t* bufUpTxHash = (uint8_t*)alloca(2*size + 1);
        toHex(bufUpTxHash, upTXHash.v);
        
        if (dumpToFiles)
          fprintf(objySimFile, 
              "    Input(InputID:%" PRIu64 ", "
                "UpTxHash:%s, "
                "UpOutputIndex:%" PRIu64 ", "
                "IsCoinBase:%d)\n",
                inputID,
                bufUpTxHash,
                (uint64_t)upOutputIndex,
                isCoinBase);
        ooId upTrxRef;
        
        if (!isCoinBase)
        {
          TrxMap::iterator val = trxMap.find(upTXHash.v);
          if(trxMap.end() == val) {
              printf("trxMap size:%ld\n", trxMap.size());
//              TrxMap::iterator itr = trxMap.begin();
//              while (itr != trxMap.end())
//              {
//                printf("... key:'%s' >> val:'%ld'\n", itr->first, 
//                        itr->second.identifier().get<objy::uint_64>());
//                itr++;
//              }
              printf("unconnected input, upTXHash:%s\n", bufUpTxHash);
          } else {
            //printf(" >>> FOUND upTrxRef\n");
            upTrxRef = val->second;
          }
        }
        
        objydata::Reference input = objyAccess.createInput(inputID, bufUpTxHash, upTrxRef,
                isCoinBase);
        
        objyAccess.addInputToTransaction(input, currentTransaction);
    }
    // Called when at the end of a TX input
    virtual void endInput(
        const uint8_t *p
    ) {
        ++inputID;
    }

    virtual void endOutput(
        const uint8_t *p,
        uint64_t      value,
        const uint8_t *txHash,
        uint64_t      outputIndex,
        const uint8_t *outputScript,
        uint64_t      outputScriptSize
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
        
        if(likely(0<=type)) {
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
           }
           else 
           {
             addressRef = objydata::referenceFor(val->second);
           }
        }

        // id BIGINT PRIMARY KEY
        // dstAddress CHAR(36)
        // value BIGINT
        // txID BIGINT
        // offset INT
        if (dumpToFiles)
          fprintf(
            objySimFile,
            "    Output(ID:%" PRIu64 ", "
            "Address:%s, "
            "Value:%" PRIu64 ", "
            "TxID:%" PRIu64 ", "
            "Index:%" PRIu32 ")\n"
            ,
            outputID,
            address,
            value,
            txID,
            (uint32_t)outputIndex
        );
        
        objydata::Reference output = objyAccess.createOutput(outputID, address, 
                addressRef, value);
        objyAccess.addOutputToTransaction(output, currentTransaction);
        
        uint32_t oi = outputIndex;
        uint8_t *h = allocHash256();
        memcpy(h, txHash, kSHA256ByteSize);

        uintptr_t ih = reinterpret_cast<uintptr_t>(h);
        uint32_t *h32 = reinterpret_cast<uint32_t*>(ih);
        h32[0] ^= oi;

        //outputMap[h] = outputID++;
        outputID++;
    }

//    virtual void edge(
//        uint64_t      value,
//        const uint8_t *upTXHash,
//        uint64_t      outputIndex,
//        const uint8_t *outputScript,
//        uint64_t      outputScriptSize,
//        const uint8_t *downTXHash,
//        uint64_t      inputIndex,
//        const uint8_t *inputScript,
//        uint64_t      inputScriptSize
//    ) {
//        uint256_t h;
//        uint32_t oi = outputIndex;
//        memcpy(h.v, upTXHash, kSHA256ByteSize);
//
//        uintptr_t ih = reinterpret_cast<uintptr_t>(h.v);
//        uint32_t *h32 = reinterpret_cast<uint32_t*>(ih);
//        h32[0] ^= oi;
//
//        OutputMap::iterator src = outputMap.find(h.v);
//        if(outputMap.end()==src) {
//            errFatal("unconnected input");
//        }
//
//        size_t size = kSHA256ByteSize;
//        uint8_t* bufUpTxHash = (uint8_t*)alloca(2*size + 1);
//        if (upTXHash != NULL)
//          toHex(bufUpTxHash, upTXHash);
//        else
//          *bufUpTxHash = '\n';
//        uint8_t* bufDownTxHash = (uint8_t*)alloca(2*size + 1);
//        toHex(bufDownTxHash, downTXHash);
//
//        // id BIGINT PRIMARY KEY
//        // outputID BIGINT
//        // txID BIGINT
//        // offset INT
//        if (dumpToFiles)
//          fprintf(
//            objyEdgeSimFile,
//            "Edge(InputId:%" PRIu64 ", "
//            "OutputId:%" PRIu64 ", "
//            "TxId:%" PRIu64 ", "
//            "InputIndex:%" PRIu32 ", "
//            //"Value:%" PRIu64 " # %.08f, "
//            "Value: %" PRIu64 ", "
//            "sourceTXOutputIndex:%d, "
//            "sourceTXHash:%s, "
//            "downTxHash:%s"
//            ")\n"
//            ,
//            inputID,
//            src->second,
//            txID,
//            (uint32_t)inputIndex,
//            //satoshisToNormaForm(value),
//            value,
//            (int)outputIndex,
//            bufUpTxHash,
//            bufDownTxHash
//        );
//    }

    virtual void wrapup() {
        fclose(objySimFile);
        
        if (trx->getOpenMode() != objydb::OpenMode::NotOpened)
          trx->commit();
        
        info("done\n");
    }
};

static ObjyDump objyDump;


