
// Full dump to Kafka triples of the blockchain

#include <util.h>
#include <stdio.h>
#include <string>
#include <common.h>
#include <errlog.h>
#include <option.h>
#include <callback.h>

#include "KafkaUtil.h"

struct KafkaDump : public Callback {

    FILE *objySimFile;
    FILE *objyEdgeSimFile;
    
    uint64_t txID;
    uint64_t blkID;
    uint64_t inputID;
    uint64_t outputID;
    
    uint64_t beginBlockId;
    uint64_t endBlockId;
    bool doProcessing;
    
    optparse::OptionParser parser;
    
    uint8_t prevBlockHash[2*kSHA256ByteSize + 1];
    
    bool dumpToFiles;
    bool enoughProcessing;
    
    KafkaUtil kafkaUtil;
     
    KafkaDump() {
        parser
            .usage("[options] [list of addresses to restrict output to]")
            .version("")
            .description("produce triples in Kafka for the blockchain")
            .epilog("")
        ;
        parser
            .add_option("-b", "--beginBlock")
            .action("store")
            .type("int")
            .set_default(0)
            .help("begin processing at block <block> (default: 0)")
        ;
        parser
            .add_option("-e", "--endBlock")
            .action("store")
            .type("int")
            .set_default(-1)
            .help("end processing at block <block> (default: 0)")
        ;
        parser
            .add_option("-p", "--numPartitions")
            .action("store")
            .type("int")
            .set_default(16)
            .help("number of Kafka partitions (default: 16)")
        ;
        parser
            .add_option("-s", "--batchSize")
            .action("store")
            .type("int")
            .set_default(10)
            .help("number of triple messages in a JSON batch (default: 10)")
        ;
        parser
            .add_option("-h", "--brokers")
            .action("store")
            .type("string")
            .set_default("localhost")
            .help("kafka brokers list (default: localhost)")
        ;
        parser
            .add_option("-t", "--topic")
            .action("store")
            .type("string")
            .set_default("bitcoin")
            .help("kafka topic (default: bitcoin)")
        ;
    }

    virtual const char                   *name() const         { return "kafkadump"; }
    virtual const optparse::OptionParser *optionParser() const { return &parser;   }
    virtual bool                       needUpstream() const    { return true;      }

    virtual void aliases(
        std::vector<const char*> &v
    ) const {
        v.push_back("kafkadump");
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
        beginBlockId = values.get("beginBlock").asInt64();
        endBlockId = values.get("endBlock").asInt64();
        
        
        doProcessing = false;
        
        info("dumping the blockchain ...");

        objySimFile = fopen("objySimFile.txt", "w");
        if(!objySimFile) sysErrFatal("couldn't open file objySimFile.txt for writing\n");
        objyEdgeSimFile = fopen("objyEdgeSimFile.txt", "w");
        if(!objyEdgeSimFile) sysErrFatal("couldn't open file objyEdgeSimFile.txt for writing\n");

        int numPartitions = values.get("numPartitions").asInt32();
        int batchSize = values.get("batchSize").asInt32();
        std::string kafkaBrokers = static_cast<const char*>(values.get("brokers"));
        std::string kafkaTopic = static_cast<const char*>(values.get("topic"));
        
        //printf("p: %d, b: %d, h: %s, t: %s\n", numPartitions, batchSize, kafkaBrokers.c_str(), kafkaTopic.c_str());
        kafkaUtil.init(numPartitions, batchSize, kafkaBrokers, kafkaTopic);
        
        return 0;
    }
    
    virtual void startBlock(
        const Block *b,
        uint64_t
    ) {
      
        blkID = b->height-1;

        if (!doProcessing && blkID >= beginBlockId)
          doProcessing = true;
      
        if (!doProcessing)
          return;
      
        if(1 <= endBlockId && endBlockId < blkID) {
            enoughProcessing = true;
            return;
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


//        currentBlock = objyAccess.createBlock(blkID, version, bufPrevBlockHash, 
//                bufBlockMerkleRoot, blkTime, bufBlockHash, previousBlock);
    
        if (blkID > 0) {
          // generate prevBlock
          kafkaUtil.prevBlockToJson(prevBlockHash);
        }
        kafkaUtil.blockToJson(blkID, version, bufPrevBlockHash, 
                bufBlockMerkleRoot, blkTime, bufBlockHash);
        
        if (blkID > 0)
        {
          //associate block with previous
          kafkaUtil.submitTriple(kafkaUtil.getBlockElement(), 
                  kafkaUtil.getPrevBlockElement(), "m_prevBlock", "m_NextBlock");
        }
        
        memcpy(prevBlockHash, bufBlockHash, 2 * kSHA256ByteSize + 1);
    }

    virtual void endBlock(
        const Block *b
    ) {
       
        if (!doProcessing)
          return;

//      fprintf(objySimFile,
//        " BLOCK(%s)\n\n",
//        kafkaUtil.getBatchAsJson());
    }
    
    virtual bool done() {
      return enoughProcessing;
    }
    
    virtual void startTX(
        const uint8_t *p,
        const uint8_t *hash
    ) {
      
        if (!doProcessing)
          return;
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
        kafkaUtil.submitTriple(kafkaUtil.getTransactionElement(), 
                kafkaUtil.getBlockElement(), "m_block", "m_Transactions");
     }

    virtual void endTX(
        const uint8_t *p
    ) {
      
        if (!doProcessing)
          return;

        LOAD(uint32_t, lockTime, p);
        if (dumpToFiles)
          fprintf(objySimFile, 
                "  EndTX(    lockTime = %" PRIu32 "\n", 
                (uint32_t)lockTime);
    }
        
    virtual void startInputs(
        const uint8_t *p
    ) {
      
        if (!doProcessing)
          return;

        inputID = 0;
        //isCoinBase = false;
    }
    
        // Called when a TX input is encountered
    virtual void startInput(
        const uint8_t *p
    ) {
      
        if (!doProcessing)
          return;

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
                
//        objydata::Reference input = objyAccess.createInput(inputID, bufUpTxHash, upTrxRef,
//                isCoinBase);        
//        objyAccess.addInputToTransaction(input, currentTransaction);

        kafkaUtil.inputToJson(inputID, bufUpTxHash, isCoinBase);
        //associate transaction to block

        if (!isCoinBase) {
          kafkaUtil.upTxToJson(0, bufUpTxHash);
          // input doesn't have a key so we'll route it to the transaction
          kafkaUtil.submitTriple(kafkaUtil.getTransactionElement(), 
                  kafkaUtil.getInputElement(),
                  kafkaUtil.getUpTxElement(), "m_Inputs", "");
        } else {
          kafkaUtil.submitTriple(kafkaUtil.getTransactionElement(), 
                   kafkaUtil.getInputElement(), "m_Inputs");
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
      
        if (!doProcessing)
          return;
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
            kafkaUtil.submitTriple(kafkaUtil.getTransactionElement(), 
                    kafkaUtil.getOutputElement(), 
                    kafkaUtil.getAddressElement(),
                    "m_Outputs", "m_Outputs");
        } else {
          // TBD.
          // report warning that such output doesn't have a valid address.
        }
        outputID++;
    }

    virtual void wrapup() {
        fclose(objySimFile);
        
        info("done\n");
    }
};

static KafkaDump kafkaDump;


