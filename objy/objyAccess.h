/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   objySchema.h
 * Author: ibrahim
 *
 * Created on January 5, 2017, 4:06 PM
 */

#ifndef OBJYSCHEMA_H
#define OBJYSCHEMA_H

#include <string>
#include <map>

#include <ooObjy.h>
#include <objy/data/Data.h>
#include <objy/data/List.h>
#include <objy/data/DataSpecificationBuilder.h>

namespace objydata = objy::data;
namespace objyschema = objy::schema_provider;

using namespace std;

class ClassAccessor;

typedef std::map<string, ClassAccessor*> ClassAccessorMap;
typedef map<string, objydata::Attribute> AttributeMap;


class ClassAccessor {
  public:
    ClassAccessor(const ClassAccessor& orig) = delete;
 
    virtual ~ClassAccessor() {
    }

    ClassAccessor(const std::string& name) :
    _className(name) { }

    ClassAccessor(ClassAccessor&& other) :
      _className(std::move(other._className)), 
      _classRef(std::move(other._classRef)),
      _attributeMap(std::move(other._attributeMap))
    { }

    void init() {
      //cout << "locating class: " << _className.c_str() << endl;
      objy::data::Class clazz = objy::data::lookupClass(_className.c_str());
      _classRef = clazz;
      for (unsigned i = 0; i < _classRef.numberOfAttributes(); i++) {
        objy::data::Attribute attr = _classRef.attribute(i);
        _attributeMap[attr.name()] = attr;
      }
    }
    
    const objy::data::Attribute& getAttribute(const string& attrName) const
    {
      AttributeMap::const_iterator itr = _attributeMap.find(attrName);
      if (itr == _attributeMap.end()) {
        cerr << "Failed to get attribute: '" << attrName
                << "' for class: '" << _className << "'" << endl;
        throw std::invalid_argument("Failed to get attribute: " + attrName);
      }

      return itr->second;
    }

    string getClassName() const {
      return _className;
    }

    objy::data::Class getObjyClass() const {
      return _classRef;
    }

    objy::data::Object createInstance() const {
      //objectCreatedCounter++;
      return objy::data::createPersistentObject(_classRef);
    }

    void setAttributeValue(objy::data::Object instance,
            const string& attributeName, const objy::data::Variable& value) const
    {
      const objy::data::Attribute& attribute = this->getAttribute(attributeName);
      if (!attribute.isNull())
        setAttributeValue(instance, attribute, value);
    }

    void setReference(objy::data::Object instance,
            const string& attributeName, const objy::data::Reference value) const
    {
      const objy::data::Attribute& attribute = this->getAttribute(attributeName);
      if (instance.isNull() || value.isNull() || attribute.isNull()) {
        std::cerr << "For attr: " << attributeName
                << " - instance/attribute/value: " << objy::data::oidFor(instance).sprint()
                << " / " << attribute.name() << " / " << objy::data::oidFor(value).sprint()
                << std::endl;
      }
      setReference(instance, attribute, value);
    }

    void addReference(objy::data::Object instance,
            const string& attributeName, const objy::data::Reference value) const
    {
      const objy::data::Attribute& attribute = this->getAttribute(attributeName);
      setReference(instance, attribute, value);
    }

    void incUInt64AttributeValue(objy::data::Object instance, const string& attributeName) const
    {
      const objy::data::Attribute& attribute = this->getAttribute(attributeName);
      if (!attribute.isNull())
        incUInt64AttributeValue(instance, attribute);
    }
 
    void addToUInt64AttributeValue(objy::data::Object instance, 
            const string& attributeName, uint64_t value) const
    {
      const objy::data::Attribute& attribute = this->getAttribute(attributeName);
      if (!attribute.isNull())
        addToUInt64AttributeValue(instance, attribute, value);
    }

    
    
  private:
    void setAttributeValue(objy::data::Object instance,
            const objy::data::Attribute& attribute, const objy::data::Variable& value) const
    {
      objy::data::Variable varValue;
      instance.attributeValue(attribute, varValue);
      //varValue.set(value);
      varValue = value;
    }

    // NOTE: this version doesn't check if a reference exist in a list before
    //       adding it, but does that for the map since it's cheaper operation
    void setReference(objy::data::Object instance,
            const objy::data::Attribute& attribute, const objy::data::Reference value) const
    {
      objy::data::Variable varValue;
      instance.attributeValue(attribute, varValue);

      objy::data::LogicalType::type attrLogicalType =
              attribute.attributeValueSpecification()->logicalType();

      if (attrLogicalType == objy::data::LogicalType::List) {
        objy::data::List list = varValue.get<objy::data::List>();
        list.append(value);
      } else if (attrLogicalType == objy::data::LogicalType::Map) {
        objy::data::Map map = varValue.get<objy::data::Map>();
        addReferenceIfDoesnotExist(map, value);
      } else if (attrLogicalType == objy::data::LogicalType::Reference) {
        varValue.set(value);
      } else {
        std::cerr << "Illegal attribute type " << objy::data::LogicalType::toString(attrLogicalType)
                << " for Instance value." << std::endl;
      }      
    }

    void addReferenceIfDoesnotExist(objy::data::Map& map,
            const objy::data::Reference objRef) const {
      // set the key and value in the map from the call object.... 
      ooId oid = objy::data::oidFor(objRef);
      const char* key = oid.sprint();
      if (!map.containsKey(key)) {
        //objy::data::Variable value(objRef);
        map[key] = objRef;
      }
    }

    /* 
     * Note that the variable we are incrementing is of type 'uint64_t' 
     * Hence we named the function specifically to show that.
     * A better generic impl should check the type and get the value accordingly.
     */
    void incUInt64AttributeValue(objy::data::Object instance,
            const objy::data::Attribute& attribute) const
    {
      objy::data::Variable varValue;
      instance.attributeValue(attribute, varValue);
      varValue.set(varValue.get<uint64_t>() + 1);
    }

    /* 
     * Note that the variable we are adding is of type 'uint64_t' 
     * Hence we named the function specifically to show that.
     * A better generic impl should check the type and get the value accordingly.
     */
    void addToUInt64AttributeValue(objy::data::Object instance,
            const objy::data::Attribute& attribute, uint64_t value) const
    {
      objy::data::Variable varValue;
      instance.attributeValue(attribute, varValue);
      varValue.set(varValue.get<uint64_t>() + value);
    }
    
  private:

    string            _className;
    objy::data::Class _classRef;
    AttributeMap      _attributeMap;

};


class ObjyAccess {
public:
  ObjyAccess();
  virtual ~ObjyAccess();
  bool setupCache();
  
  ClassAccessor* getClassProxy(const string& className) {

    auto itr = _classProxyMap.find(className);
    if (itr == _classProxyMap.end()) {
      ClassAccessor* classAccessor = new ClassAccessor(className);
      classAccessor->init();
      _classProxyMap[className] = classAccessor;
      return classAccessor;
    }
    return itr->second;
  }
  
  
  objydata::Reference createBlock(
        uint64_t id, int version, uint8_t* prevBlockHash, uint8_t* blockMerkleRoot, 
        long blkTime, uint8_t* hash, objydata::Reference& prevBlock);
  objydata::Reference createTransaction(uint64_t id, uint8_t* hash, long blkTime,
                        uint64_t blockId);
  objydata::Reference createInput(
          uint64_t index, uint8_t* upTxHash, ooId& upTrxRef, uint64_t upTrxIndex, 
          objydata::Reference& transaction);
  objydata::Reference createOutput(uint64_t index, 
          uint8_t* address, objydata::Reference& addressRef, 
          uint64_t trxValue, objydata::Reference& transaction);
  objydata::Reference createAddress(uint8_t* hash);

  bool addTransactionToBlock(objydata::Reference& transaction, objydata::Reference& block);
  bool addInputToTransaction(objydata::Reference& input, objydata::Reference& transaction);
  bool addOutputToTransaction(objydata::Reference& output, objydata::Reference& transaction);
  
  bool updateTransactionValues(objydata::Reference& transaction, 
          bool isCoinBase, uint64_t trxInValue, uint64_t trxOutValue, 
          uint64_t numInputs, uint64_t numOutputs);

private:

  ClassAccessorMap      _classProxyMap;
  objydata::Utf8String  _stringVariable;
  
};

#endif /* OBJYSCHEMA_H */

