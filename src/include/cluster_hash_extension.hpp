#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Main extension initialization function
void cluster_hash_extensions_init(DatabaseInstance &instance);

// C-style API functions for extension loading
extern "C" {
    DUCKDB_EXTENSION_API void cluster_hash_init(duckdb::DatabaseInstance &db);
    DUCKDB_EXTENSION_API const char *cluster_hash_version();
}

// CRC16-XMODEM hash calculation
uint16_t crc16xmodem(const char *buf, size_t len);

// Hash tag extraction helper 
std::string extract_hash_tag(const std::string &key);

// Function to calculate hash slot (0-16383) for a key
void HashSlotFunction(DataChunk &args, ExpressionState &state, Vector &result);

// Function to calculate node assignment for a key
void NodeFunction(DataChunk &args, ExpressionState &state, Vector &result);

} // namespace duckdb