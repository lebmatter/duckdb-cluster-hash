#define DUCKDB_EXTENSION_MAIN

#include "cluster_hash_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// CRC16-XMODEM implementation
static uint16_t crc16xmodem(const char *buf, size_t len) {
  uint16_t crc = 0;
    
  while (len--) {
    crc ^= (uint8_t)*buf++ << 8;
        
    for (int i = 0; i < 8; i++) {
      if (crc & 0x8000) {
        crc = (crc << 1) ^ 0x1021;
      } else {
        crc = crc << 1;
      }
    }
  }
    
  return crc;
}

// Extracts the hash tag from the key if present
static std::string extract_hash_tag(const std::string &key) {
  size_t start = key.find('{');
  if (start == std::string::npos) {
    return key; // No hash tag, use the entire key
  }
    
  size_t end = key.find('}', start + 1);
  if (end == std::string::npos) {
    return key; // Malformed hash tag, use the entire key
  }
    
  std::string hash_tag = key.substr(start + 1, end - start - 1);
  return hash_tag.empty() ? key : hash_tag;
}

// Calculate the hash slot (0-16383) for a given key
static int32_t hash_slot(const std::string &key) {
  std::string hash_tag = extract_hash_tag(key);
  uint16_t hash = crc16xmodem(hash_tag.c_str(), hash_tag.length());
  return int32_t(hash % 16384);
}

// Calculate the node number for a given key and node count
static int32_t node_for_key(const std::string &key, int32_t nodes) {
  if (nodes <= 0) {
    return 0;
  }
  
  int32_t slot = hash_slot(key);
  int32_t slots_per_node = 16384 / nodes;
  return int32_t(slot / slots_per_node);
}

namespace duckdb {

inline void HashSlotFunction(DataChunk &args, ExpressionState &state, Vector &result) {
  auto &input = args.data[0];
    
  UnaryExecutor::Execute<string_t, int32_t>(input, result, args.size(),
    [&](string_t input_val) {
      if (input_val.GetSize() == 0) {
        return int32_t(0);
      }
            
      std::string key(input_val.GetString(), input_val.GetSize());
      return hash_slot(key);
    });
}

inline void NodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
  auto &input = args.data[0];
  auto &node_count = args.data[1];
    
  BinaryExecutor::Execute<string_t, int32_t, int32_t>(input, node_count, result, args.size(),
    [&](string_t input_val, int32_t nodes) {
      if (input_val.GetSize() == 0) {
        return int32_t(0);
      }
            
      std::string key(input_val.GetString(), input_val.GetSize());
      return node_for_key(key, nodes);
    });
}

static void LoadInternal(DatabaseInstance &instance) {
  // Register the hash_slot function
  auto hash_slot_func = ScalarFunction("hash_slot", {LogicalType::VARCHAR}, 
                                      LogicalType::INTEGER, HashSlotFunction);
    
  // Register the node function
  auto node_func = ScalarFunction("node", {LogicalType::VARCHAR, LogicalType::INTEGER}, 
                                 LogicalType::INTEGER, NodeFunction);
                                 
  ExtensionUtil::RegisterFunction(instance, hash_slot_func);
  ExtensionUtil::RegisterFunction(instance, node_func);
}

void ClusterHashExtension::Load(DuckDB &db) { 
  LoadInternal(*db.instance); 
}

std::string ClusterHashExtension::Name() { 
  return "cluster_hash"; 
}

std::string ClusterHashExtension::Version() const {
#ifdef EXT_VERSION_CLUSTER_HASH
  return EXT_VERSION_CLUSTER_HASH;
#else
  return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void cluster_hash_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::ClusterHashExtension>();
}

DUCKDB_EXTENSION_API const char *cluster_hash_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif