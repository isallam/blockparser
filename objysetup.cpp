/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   objysetup.cpp
 * Author: ibrahim
 *
 * The main reason of this file is to be able to create the schema and setup
 * the Placement before the ingest process runs. If you don't call this, you'll
 * have the correct schema but no placement
 * 
 * Created on January 11, 2017, 4:41 PM
 */

#include <cstdlib>
#include <ooObjy.h>
#include <objy/Configuration.h>
#include <objy/Tools.h>
#include <objy/db/Connection.h>
#include <objy/db/Transaction.h>
#include <objy/db/TransactionScope.h>
#include <objy/data/Data.h>
#include <objy/data/List.h>

#include "objy/objyAccess.h"

namespace objydb = objy::db;
namespace objydata = objy::data;
namespace objyconfig = objy::configuration;

using namespace std;

/*
 * 
 */
int main(int argc, char** argv) {
  
    std::string fdname;
    objydb::Connection* connection;
    objydb::Transaction* trx;
    ObjyAccess objyAccess;

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
      
      std::cout << sink.getOut() << std::endl;
      std::cerr << sink.getErr() << std::endl;
    } 
    catch (ooKernelException& e)
    {
      cout << e.what() << endl;
      return 1;
    }
    catch (ooBaseException& e)
    {
      cout << e.what() << endl;
      return 1;
    }

  return 0;
}

