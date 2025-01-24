/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef avro_Node_hh__
#define avro_Node_hh__

#include "Config.hh"

#include <boost/noncopyable.hpp>
#include <cassert>
#include <memory>
#include <utility>

#include "Exception.hh"
#include "LogicalType.hh"
#include "SchemaResolution.hh"
#include "Types.hh"

namespace avro {

class Node;
class GenericDatum;

using NodePtr = std::shared_ptr<Node>;

class AVRO_DECL Name {
    std::string ns_;
    std::string simpleName_;

public:
    Name() = default;
    explicit Name(const std::string &fullname);
    Name(std::string simpleName, std::string ns) : ns_(std::move(ns)), simpleName_(std::move(simpleName)) { check(); }

    std::string fullname() const;
    const std::string &ns() const { return ns_; }
    const std::string &simpleName() const { return simpleName_; }

    void ns(std::string n) { ns_ = std::move(n); }
    void simpleName(std::string n) { simpleName_ = std::move(n); }
    void fullname(const std::string &n);

    bool operator<(const Name &n) const;
    void check() const;
    bool operator==(const Name &n) const;
    bool operator!=(const Name &n) const { return !((*this) == n); }
    void clear() {
        ns_.clear();
        simpleName_.clear();
    }
    explicit operator std::string() const {
        return fullname();
    }
};

inline std::ostream &operator<<(std::ostream &os, const Name &n) {
    return os << n.fullname();
}

/// Node is the building block for parse trees.  Each node represents an avro
/// type.  Compound types have leaf nodes that represent the types they are
/// composed of.
///
/// The user does not use the Node object directly, they interface with Schema
/// objects.
///
/// The Node object uses reference-counted pointers.  This is so that schemas
/// may be reused in other schemas, without needing to worry about memory
/// deallocation for nodes that are added to multiple schema parse trees.
///
/// Node has minimal implementation, serving as an abstract base class for
/// different node types.
///

class AVRO_DECL Node : private boost::noncopyable {
public:
    explicit Node(Type type) : type_(type),
                               logicalType_(LogicalType::NONE),
                               locked_(false) {}

    virtual ~Node();

    Type type() const {
        return type_;
    }

    LogicalType logicalType() const {
        return logicalType_;
    }

    void setLogicalType(LogicalType logicalType);

    void lock() {
        locked_ = true;
    }

    bool locked() const {
        return locked_;
    }

    virtual bool hasName() const = 0;

    void setName(const Name &name) {
        checkLock();
        checkName(name);
        doSetName(name);
    }
    virtual const Name &name() const = 0;

    virtual const std::string &getDoc() const = 0;
    void setDoc(const std::string &doc) {
        checkLock();
        doSetDoc(doc);
    }

    void addLeaf(const NodePtr &newLeaf) {
        checkLock();
        doAddLeaf(newLeaf);
    }
    virtual size_t leaves() const = 0;
    virtual const NodePtr &leafAt(size_t index) const = 0;
    virtual const GenericDatum &defaultValueAt(size_t index) {
        throw Exception(boost::format("No default value at: %1%") % index);
    }

    void addName(const std::string &name) {
        checkLock();
        checkName(Name(name));
        doAddName(name);
    }
    virtual size_t names() const = 0;
    virtual const std::string &nameAt(size_t index) const = 0;
    virtual bool nameIndex(const std::string &name, size_t &index) const = 0;

    void setFixedSize(size_t size) {
        checkLock();
        doSetFixedSize(size);
    }
    virtual size_t fixedSize() const = 0;

    virtual bool isValid() const = 0;

    virtual SchemaResolution resolve(const Node &reader) const = 0;

    virtual void printJson(std::ostream &os, size_t depth) const = 0;

    virtual void printBasicInfo(std::ostream &os) const = 0;

    virtual void setLeafToSymbolic(size_t index, const NodePtr &node) = 0;

    // Serialize the default value GenericDatum g for the node contained
    // in a record node.
    virtual void printDefaultToJson(const GenericDatum &g, std::ostream &os,
                                    size_t depth) const = 0;

protected:
    void checkLock() const {
        if (locked()) {
            throw Exception("Cannot modify locked schema");
        }
    }

    virtual void checkName(const Name &name) const {
        name.check();
    }

    virtual void doSetName(const Name &name) = 0;
    virtual void doSetDoc(const std::string &name) = 0;

    virtual void doAddLeaf(const NodePtr &newLeaf) = 0;
    virtual void doAddName(const std::string &name) = 0;
    virtual void doSetFixedSize(size_t size) = 0;

private:
    const Type type_;
    LogicalType logicalType_;
    bool locked_;
};

} // namespace avro

namespace std {
inline std::ostream &operator<<(std::ostream &os, const avro::Node &n) {
    n.printJson(os, 0);
    return os;
}
} // namespace std

#endif
