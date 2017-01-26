
// Full dump to Kafka triples of the blockchain

#include <util.h>
#include <stdio.h>
#include <string>
#include <common.h>
#include <errlog.h>
#include <option.h>
#include <callback.h>

#include "KafkaUtil2.h"

struct KafkaDump2 : public Callback {

    FILE *objySimFile;
    FILE *objyEdgeSimFile;
    
    // objy stuff
    uint64_t txID;
    uint64_t blkID;
    uint64_t inputID;
    uint64_t outputID;
    
    //bool isCoinBase;
    int64_t cutoffBlock;
    
    optparse::OptionParser parser;
    
    
    bool dumpToFiles;
    bool enoughProcessing;
    
    KafkaUtil2 kafkaUtil;
     
    KafkaDump2() {
        parser
            .usage("[options] [list of addresses to restrict output to]")
            .version("")
            .description("produce triples in Kafka for the blockchain")
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

    virtual const char                   *name() const         { return "kafkjsondump"; }
    virtual const optparse::OptionParser *optionParser() const { return &parser;   }
    virtual bool                       needUpstream() const    { return true;      }

    virtual void aliases(
        std::vector<const char*> &v
    ) const {
        v.push_back("kafkjsondump");
    }

    virtual int init(
        int argc,
        const char *argv[]
    ) {
        txID = -1;
        blkID = 0;
        inputID = 0;
        outputID = 0;
        
        dumpToFiles = false;
        enoughProcessing = false;

        static uint64_t sz = 32 * 1000 * 1000;
        
        //trxMap.setEmptyKey(empty);
        //trxMap.resize(sz);
        
        //addrMap.setEmptyKey(emptyAddressKey);
        //addrMap.resize(sz);

        optparse::Values &values = parser.parse_args(argc, argv);
        cutoffBlock = values.get("atBlock").asInt64();

        info("dumping the blockchain ...");

        objySimFile = fopen("objySimFile.txt", "w");
        if(!objySimFile) sysErrFatal("couldn't open file objySimFile.txt for writing\n");
        objyEdgeSimFile = fopen("objyEdgeSimFile.txt", "w");
        if(!objyEdgeSimFile) sysErrFatal("couldn't open file objyEdgeSimFile.txt for writing\n");

        kafkaUtil.init();
        
        return 0;
    }

    virtual void startBlock(
        const Block *b,
        uint64_t
    ) {
      
        kafkaUtil.startBatch();
      
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

//        currentBlock = objyAccess.createBlock(blkID, version, bufPrevBlockHash, 
//                bufBlockMerkleRoot, blkTime, bufBlockHash, previousBlock);
    
        if (blkID > 0) {
          // store previous block
          kafkaUtil.saveBlockAsPrevious();
        }
        kafkaUtil.blockToJson(blkID, version, bufPrevBlockHash, 
                bufBlockMerkleRoot, blkTime, bufBlockHash);
        
        if (blkID > 0)
        {
          //associate block with previous
          kafkaUtil.writeTriple(kafkaUtil.getBlockAsJson(), 
                  kafkaUtil.getPrevBlockAsJson(), "m_prevBlock", "m_NextBlock");
        }
        
//        int commitEvery = 1000; 
//        if(0==(b->height)%commitEvery) {
//            fprintf(
//                stderr,
//                "block=%8" PRIu64 " "
//                "nbOutputs=%8" PRIu64 "\r",
//                b->height,
//                outputID //outputMap.size()
//            );
//            //enoughProcessing = true;
//        }
    }

    virtual void endBlock(
        const Block *b
    ) {
      kafkaUtil.endBatch();
      fprintf(
        objySimFile,
        " BLOCK(%s)\n\n",
        kafkaUtil.getBatchAsJson().c_str());
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
        
        kafkaUtil.transactionToJson(txID, buf);
        //associate transaction to block
        kafkaUtil.writeTriple(kafkaUtil.getTransactionAsJson(), 
                kafkaUtil.getBlockAsJson(), "m_block", "m_Transactions");
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
        //isCoinBase = false;
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

        bool isCoinBase = (0==memcmp(gNullHash.v, upTXHash.v, sizeof(gNullHash)));
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
        
//        ooId upTrxRef;
//        
//        if (!isCoinBase)
//        {
//          TrxMap::iterator val = trxMap.find(upTXHash.v);
//          if(trxMap.end() == val) {
//              printf("trxMap size:%ld\n", trxMap.size());
//              printf("unconnected input, upTXHash:%s\n", bufUpTxHash);
//          } else {
//            //printf(" >>> FOUND upTrxRef\n");
//            upTrxRef = val->second;
//          }
//        }
        
//        objydata::Reference input = objyAccess.createInput(inputID, bufUpTxHash, upTrxRef,
//                isCoinBase);        
//        objyAccess.addInputToTransaction(input, currentTransaction);

        kafkaUtil.inputToJson(inputID, bufUpTxHash, isCoinBase);
        //associate transaction to block
        kafkaUtil.writeTriple(kafkaUtil.getTransactionAsJson(), 
                kafkaUtil.getInputAsJson(), "m_Inputs", "m_Transaction");
        if (!isCoinBase) 
        {
          kafkaUtil.upTxToJson(0, bufUpTxHash);
          kafkaUtil.writeTriple(kafkaUtil.getInputAsJson(),
                  kafkaUtil.getUpTxAsJson(), "m_UpTx", "");
        }
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
        
        //objydata::Reference addressRef;
        
        if(likely(0<=type)) {
            hash160ToAddr(
                address,
                pubKeyHash.v,
                false,
                addrType[0]
            );
            // create an address object
            kafkaUtil.addressToJson(address);

    //        objydata::Reference output = objyAccess.createOutput(outputID, address, 
    //                addressRef, value);
    //        objyAccess.addOutputToTransaction(output, currentTransaction);
            kafkaUtil.outputToJson(outputID, address, value);
            //associate transaction to output
            kafkaUtil.writeTriple(kafkaUtil.getTransactionAsJson(), 
                    kafkaUtil.getOutputAsJson(), "m_Outputs", "m_Transaction");
            // associate output to address.
            kafkaUtil.writeTriple(kafkaUtil.getOutputAsJson(),
                    kafkaUtil.getAddressAsJson(), "m_Address", "m_Outputs");
        } else {
          // TBD.
          // report warning that such output doesn't have a valid address.
        }
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
        
        info("done\n");
    }
};

static KafkaDump2 kafkaDump2;


