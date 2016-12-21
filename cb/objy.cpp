
// Full Objy / Thingspan dump of the blockchain

#include <util.h>
#include <stdio.h>
#include <string>
#include <common.h>
#include <errlog.h>
#include <option.h>
#include <callback.h>
#include <ooObjy.h>
#include <objy/db/Connection.h>
#include <objy/db/Transaction.h>
#include <objy/db/TransactionScope.h>

using namespace objy::db;

static uint8_t empty[kSHA256ByteSize] = { 0x42 };

typedef GoogMap<
    Hash256,
    uint64_t,
    Hash256Hasher,
    Hash256Equal
>::Map OutputMap;

struct ObjyDump : public Callback {

    FILE *objySimFile;
    FILE *objyEdgeSimFile;

    // objy stuff
    std::string fdname;
    objy::db::Connection* connection;
	objy::db::Transaction* trx;
    
    uint64_t txID;
    uint64_t blkID;
    bool isCoinBase;
    uint64_t inputID;
    uint64_t outputID;
    int64_t cutoffBlock;
    OutputMap outputMap;
    optparse::OptionParser parser;

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
        ooObjy::startup();
        
        //connection = ooObjy::getConnection(bootfile);
		connection = Connection::connect(fdname.c_str());
   
		trx = new Transaction(OpenMode::Update);
		trx->commit();
		trx->release();

        static uint64_t sz = 32 * 1000 * 1000;
        outputMap.setEmptyKey(empty);
        outputMap.resize(sz);

        optparse::Values &values = parser.parse_args(argc, argv);
        cutoffBlock = values.get("atBlock").asInt64();

        info("dumping the blockchain ...");

        objySimFile = fopen("objySimFile.txt", "w");
        if(!objySimFile) sysErrFatal("couldn't open file objySimFile.txt for writing\n");
        objyEdgeSimFile = fopen("objyEdgeSimFile.txt", "w");
        if(!objyEdgeSimFile) sysErrFatal("couldn't open file objyEdgeSimFile.txt for writing\n");

        return 0;
    }

    virtual void startBlock(
        const Block *b,
        uint64_t
    ) {
        if(0<=cutoffBlock && cutoffBlock<b->height) {
            wrapup();
        }

        auto p = b->chunk->getData();
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

        // id BIGINT PRIMARY KEY
        // hash BINARY(32)
        // time BIGINT
        fprintf(objySimFile, 
                "Block(ID:%" PRIu64 ", "
                "VERSION:%" PRIu64 ","
                "PrevBlockHash:%s,"
                "BlockMerkleRoot:%s,"
                "TIME:%" PRIu64 ","
                "BlockHash:%s)", 
                (blkID = b->height-1),
                (uint64_t)version,
                bufPrevBlockHash, 
                bufBlockMerkleRoot,
                (uint64_t)blkTime,
                bufBlockHash);

        fputc('\n', objySimFile);

        if(0==(b->height)%500) {
            fprintf(
                stderr,
                "block=%8" PRIu64 " "
                "nbOutputs=%8" PRIu64 "\r",
                b->height,
                outputMap.size()
            );
        }
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

      fprintf(objySimFile, 
              "  Trx(TXID:%" PRIu64 ", "
                "BLKID:%" PRIu64 ", "
                "HASH:%s)\n",
                ++txID,
                blkID,
                buf);

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
//        if(isCoinBase) {
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
//        }
       
        size_t size = kSHA256ByteSize;
        uint8_t* bufUpTxHash = (uint8_t*)alloca(2*size + 1);
        toHex(bufUpTxHash, upTXHash.v);
        
        fprintf(objySimFile, 
              "    Input(InputID:%" PRIu64 ", "
                "UpTxHash:%s, "
                "UpOutputIndex:%" PRIu64 ", "
                "IsCoinBase:%d)\n",
                inputID,
                bufUpTxHash,
                (uint64_t)upOutputIndex,
                isCoinBase);

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
        if(likely(0<=type)) {
            hash160ToAddr(
                address,
                pubKeyHash.v,
                false,
                addrType[0]
            );
        }

        // id BIGINT PRIMARY KEY
        // dstAddress CHAR(36)
        // value BIGINT
        // txID BIGINT
        // offset INT
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

        uint32_t oi = outputIndex;
        uint8_t *h = allocHash256();
        memcpy(h, txHash, kSHA256ByteSize);

        uintptr_t ih = reinterpret_cast<uintptr_t>(h);
        uint32_t *h32 = reinterpret_cast<uint32_t*>(ih);
        h32[0] ^= oi;

        outputMap[h] = outputID++;
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
        uint256_t h;
        uint32_t oi = outputIndex;
        memcpy(h.v, upTXHash, kSHA256ByteSize);

        uintptr_t ih = reinterpret_cast<uintptr_t>(h.v);
        uint32_t *h32 = reinterpret_cast<uint32_t*>(ih);
        h32[0] ^= oi;

        auto src = outputMap.find(h.v);
        if(outputMap.end()==src) {
            errFatal("unconnected input");
        }

        size_t size = kSHA256ByteSize;
        uint8_t* bufUpTxHash = (uint8_t*)alloca(2*size + 1);
        if (upTXHash != NULL)
          toHex(bufUpTxHash, upTXHash);
        else
          *bufUpTxHash = '\n';
        uint8_t* bufDownTxHash = (uint8_t*)alloca(2*size + 1);
        toHex(bufDownTxHash, downTXHash);

        // id BIGINT PRIMARY KEY
        // outputID BIGINT
        // txID BIGINT
        // offset INT
        fprintf(
            objyEdgeSimFile,
            "Edge(InputId:%" PRIu64 ", "
            "OutputId:%" PRIu64 ", "
            "TxId:%" PRIu64 ", "
            "InputIndex:%" PRIu32 ", "
            //"Value:%" PRIu64 " # %.08f, "
            "Value: %" PRIu64 ", "
            "sourceTXOutputIndex:%d, "
            "sourceTXHash:%s, "
            "downTxHash:%s"
            ")\n"
            ,
            inputID,
            src->second,
            txID,
            (uint32_t)inputIndex,
            //satoshisToNormaForm(value),
            value,
            (int)outputIndex,
            bufUpTxHash,
            bufDownTxHash
        );
    }

    virtual void wrapup() {
        fclose(objySimFile);
        info("done\n");
    }
};

static ObjyDump objyDump;

