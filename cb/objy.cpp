
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
#include <objy/data/Data.h>
#include <objy/data/List.h>
#include <objy/data/DataSpecificationBuilder.h>

namespace objydb = objy::db;
namespace objydata = objy::data;
namespace objyschema = objy::schema_provider;


static uint8_t empty[kSHA256ByteSize] = { 0x42 };
static uint8_t emptyAddressKey[kRIPEMD160ByteSize] = { 0x52 };


typedef GoogMap<Hash256, uint64_t, Hash256Hasher, Hash256Equal>::Map OutputMap;

typedef GoogMap<Hash256, objydata::Reference, Hash256Hasher, Hash256Equal>::Map TrxMap;

typedef GoogMap<Hash160, objydata::Reference, Hash160Hasher, Hash160Equal>::Map AddrMap;


bool createSchema();
objydata::Reference createBlock(int id, int version, uint8_t* prevBlockHash, 
        uint8_t* blockMerkleRoot, long blkTime, uint8_t* hash, objydata::Reference& prevBlock);
objydata::Reference createTransaction(int id, uint8_t* hash);
objydata::Reference createInput(int id, uint8_t* upTxHash, objydata::Reference& upTrxRef,
        bool isCoinBase);
objydata::Reference createOutput(int id, uint8_t* address, 
        objydata::Reference& addressRef, uint64_t trxValue);
objydata::Reference createAddress(uint8_t* hash);

bool addTransactionToBlock(objydata::Reference& transaction, objydata::Reference& block);
bool addInputToTransaction(objydata::Reference& input, objydata::Reference& transaction);
bool addOutputToTransaction(objydata::Reference& output, objydata::Reference& transaction);



// class names
const char* BlockClassName        = "Block";
const char* TransactionClassName  = "Transaction";
const char* InputClassName        = "Input";
const char* OutputClassName       = "Output";
const char* AddressClassName      = "Address";


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
    OutputMap outputMap;

    optparse::OptionParser parser;
    
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
        ooObjy::startup();
        
        //connection = ooObjy::getConnection(bootfile);
		connection = objydb::Connection::connect(fdname.c_str());
   
        try {
          trx = new objydb::Transaction(objydb::OpenMode::Update);
          // create schema 
          bool bRet = createSchema();
          
          trx->commit();
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
        outputMap.setEmptyKey(empty);
        outputMap.resize(sz);
        
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
                (blkID = b->height-1),
                (uint64_t)version,
                bufPrevBlockHash, 
                bufBlockMerkleRoot,
                (uint64_t)blkTime,
                bufBlockHash);

        if (currentBlock)
          previousBlock = currentBlock;
        
        currentBlock = createBlock(blkID, version, bufPrevBlockHash, 
                bufBlockMerkleRoot, blkTime, bufBlockHash, previousBlock);
        
        if(0==(b->height)%500) {
            fprintf(
                stderr,
                "block=%8" PRIu64 " "
                "nbOutputs=%8" PRIu64 "\r",
                b->height,
                outputMap.size()
            );
            trx->commit();
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
        
        currentTransaction = createTransaction(txID, buf);
        trxMap[key] = currentTransaction;
        
        addTransactionToBlock(currentTransaction, currentBlock);

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
        objydata::Reference upTrxRef;
        
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
        
        objydata::Reference input = createInput(inputID, bufUpTxHash, upTrxRef,
                isCoinBase);
        
        addInputToTransaction(input, currentTransaction);
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
             addressRef = createAddress(address);
             // add it to the map
              uint8_t *key = allocHash160();
              memcpy(key, pubKeyHash.v, kRIPEMD160ByteSize);
              addrMap[key] = addressRef;
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
        
        objydata::Reference output = createOutput(outputID, address, 
                addressRef, value);
        addOutputToTransaction(output, currentTransaction);
        
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

        OutputMap::iterator src = outputMap.find(h.v);
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
        if (dumpToFiles)
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
        
        if (trx->getOpenMode() != objydb::OpenMode::NotOpened)
          trx->commit();
        
        info("done\n");
    }
};

static ObjyDump objyDump;


// createSchema 
// TBD: It's here for now but perhaps we need to move it to another location,
//      once we refactor the concept of ingesting such data into Objy/Thingspan
bool createSchema()
{
  // Block Class
   objydata::DataSpecificationHandle refSpec = 
           objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()    
          .setReferencedClass(BlockClassName)
          .build();

 // transaction arrays 
  objydata::DataSpecificationHandle transactionsElemSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          //.setIdentifierSpecification(objydata::SpecificationFor<objy::uint_64>::get())
          .setReferencedClass(TransactionClassName)
          .build();
                  
  objydata::DataSpecificationHandle transactionsSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(transactionsElemSpec)
          //.setCollectionName("SegmentedArray")
          .build();

  objydata::Class blockClass = objydata::ClassBuilder(BlockClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>("id")
                 .addAttribute<objy::int_32>("version")
                 .addAttribute<objydata::DateTime>("time")
                 .addAttribute<objydata::Utf8String>("hash")
                 .addAttribute<objydata::Utf8String>("prevBlockHash")
                 .addAttribute<objydata::Utf8String>("merkleRootHash")
                 .addAttribute("prevBlock", refSpec)
                 .addAttribute("transactions", transactionsSpec)
                 .build();

  // Transaction Class
  
  objydata::DataSpecificationHandle inputsElemSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(InputClassName)
          .build();
                  
  objydata::DataSpecificationHandle inputsSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(inputsElemSpec)
          //.setCollectionName("SegmentedArray")
          .build();
  
  objydata::DataSpecificationHandle outputsElemSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(OutputClassName)
          .build();
                  
  objydata::DataSpecificationHandle outputsSpec = 
          objydata::DataSpecificationBuilder<objydata::LogicalType::List>()
          .setElementSpecification(outputsElemSpec)
          //.setCollectionName("SegmentedArray")
          .build();
  
  objydata::Class transactionClass = 
                 objydata::ClassBuilder(TransactionClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>("id")
                 //.addAttribute(objydata::LogicalType.DateTime, "time")
                 .addAttribute<objydata::Utf8String>("hash")
                 .addAttribute("inputs", inputsSpec)
                 .addAttribute("outputs", outputsSpec)
                 .build();

  // Input Class
  objydata::DataSpecificationHandle transactionRefSpec = 
           objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(TransactionClassName)
          .build();
  
  objydata::Class inputClass = 
                 objydata::ClassBuilder(InputClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>("id")
                 .addAttribute<objydata::Utf8String>("upTxHash")
                 .addAttribute("upTx", transactionRefSpec)
                 .addAttribute<bool>("isCoinBase")
                 .build();

  // Address Class
  objydata::Class addressClass = 
                 objydata::ClassBuilder(AddressClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objydata::Utf8String>("hash")
                 .build();
  
  // output Class
  objydata::DataSpecificationHandle addressRefSpec = 
           objydata::DataSpecificationBuilder<objydata::LogicalType::Reference>()
          .setReferencedClass(AddressClassName)
          .build();

  objydata::Class outputClass = 
                 objydata::ClassBuilder(OutputClassName)
                 .setSuperclass("ooObj")
                 .addAttribute<objy::uint_64>("id")
                 .addAttribute<objydata::Utf8String>("addressHash")
                 .addAttribute("address", addressRefSpec)
                 .addAttribute<objy::uint_64>("value")
                 .build();
  
  
  objyschema::SchemaProvider* provider = objyschema::SchemaProvider::defaultPersistentProvider();
  provider->represent(blockClass);
  provider->represent(transactionClass);
  provider->represent(inputClass);
  provider->represent(outputClass);
  provider->represent(addressClass);
  
  return true;
  
}

objydata::Reference createBlock(int id, int version, uint8_t* prevBlockHash, 
        uint8_t* blockMerkleRoot, long blkTime, uint8_t* hash, 
        objydata::Reference& prevBlock)
{
  objydata::Class objectClass = objydata::lookupClass(BlockClassName);
  objydata::Object object = objydata::createPersistentObject(objectClass);
  
  object.attributeValue("id").set<objy::uint_64>(id);
  object.attributeValue("version").set<objy::int_32>(version);
  objydata::DateTime dateTime;
  
  object.attributeValue("time").set<objydata::DateTime>(objydata::DateTime(blkTime, 0));
  
  objydata::Utf8String value = objydata::createUtf8String();
  value.set(reinterpret_cast<char*>(hash));
  object.attributeValue("hash").set<objydata::Utf8String>(value);
  value.set(reinterpret_cast<char*>(prevBlockHash));
  object.attributeValue("prevBlockHash").set<objydata::Utf8String>(value);
  value.set(reinterpret_cast<char*>(blockMerkleRoot));
  object.attributeValue("merkleRootHash").set<objydata::Utf8String>(value);
  
  if (prevBlock)
  {
    object.attributeValue("prevBlock").set<objydata::Reference>(prevBlock);
  }
  /**
                 .addAttribute("prevBlock", refSpec)
  **/
  return objydata::createReference(object);
}

objydata::Reference createTransaction(int id, uint8_t* hash)
{
  objydata::Class objectClass = objydata::lookupClass(TransactionClassName);
  objydata::Object object = objydata::createPersistentObject(objectClass);
  
  object.attributeValue("id").set<objy::uint_64>(id);
  objydata::Utf8String value = objydata::createUtf8String();
  value.set(reinterpret_cast<char*>(hash));
  object.attributeValue("hash").set<objydata::Utf8String>(value);
  /**
  //.addAttribute(objydata::LogicalType.DateTime, "time")
   **/
  
  return objydata::createReference(object);
}

objydata::Reference createInput(int id, uint8_t* upTxHash, 
        objydata::Reference& upTrxRef, bool isCoinBase)
{
  objydata::Class objectClass = objydata::lookupClass(InputClassName);
  objydata::Object object = objydata::createPersistentObject(objectClass);

  object.attributeValue("id").set<objy::uint_64>(id);
  object.attributeValue("isCoinBase").set<bool>(isCoinBase);
  
  objydata::Utf8String value = objydata::createUtf8String();
  
  value.set(reinterpret_cast<char*>(upTxHash));
  object.attributeValue("upTxHash").set<objydata::Utf8String>(value);
  
  if (!isCoinBase && upTrxRef)
  {
    object.attributeValue("upTx").set<objydata::Reference>(upTrxRef);
  }

  return objydata::createReference(object);
}


objydata::Reference createOutput(int id, uint8_t* address, 
        objydata::Reference& addressRef, uint64_t trxValue)
{
  objydata::Class objectClass = objydata::lookupClass(OutputClassName);
  objydata::Object object = objydata::createPersistentObject(objectClass);
  
  object.attributeValue("id").set<objy::uint_64>(id);
  object.attributeValue("value").set<objy::uint_64>(trxValue);
  
  objydata::Utf8String value = objydata::createUtf8String();
  
  value.set(reinterpret_cast<char*>(address));
  object.attributeValue("addressHash").set<objydata::Utf8String>(value);
  
  if (addressRef)
  {
    object.attributeValue("address").set<objydata::Reference>(addressRef);
  }
  return objydata::createReference(object);
}

objydata::Reference createAddress(uint8_t* hash)
{
  objydata::Class objectClass = objydata::lookupClass(AddressClassName);
  objydata::Object object = objydata::createPersistentObject(objectClass);
  
  objydata::Utf8String value = objydata::createUtf8String();
  value.set(reinterpret_cast<char*>(hash));
  object.attributeValue("hash").set<objydata::Utf8String>(value);
  
  return objydata::createReference(object);
}

bool addTransactionToBlock(objydata::Reference& transaction, objydata::Reference& block)
{
  objydata::List list = 
          block.referencedObject().attributeValue("transactions").get<objydata::List>();
  
  list.append(transaction);
  
  return true;
}

bool addInputToTransaction(objydata::Reference& input, objydata::Reference& transaction)
{
  
  objydata::List list = 
          transaction.referencedObject().attributeValue("inputs").get<objydata::List>();
  
  list.append(input);

  return true;
}

bool addOutputToTransaction(objydata::Reference& output, objydata::Reference& transaction)
{
  objydata::List list = 
          transaction.referencedObject().attributeValue("outputs").get<objydata::List>();
  
  list.append(output);
  
  return true;
}