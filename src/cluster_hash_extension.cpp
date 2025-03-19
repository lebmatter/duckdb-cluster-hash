#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <string>

namespace duckdb {

// CRC16-XMODEM implementation
static inline uint16_t crc16xmodem(const char *buf, size_t len) {
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
static void HashSlotFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    
    UnaryExecutor::Execute<string_t, int32_t>(input, result, args.size(),
        [&](string_t input_val) {
            if (input_val.IsNull()) {
                return int32_t(0);
            }
            
            std::string key(input_val.GetString(), input_val.GetSize());
            std::string hash_tag = extract_hash_tag(key);
            
            // Calculate the CRC16-XMODEM hash
            uint16_t hash = crc16xmodem(hash_tag.c_str(), hash_tag.length());
            
            // Return the slot (CRC16 mod 16384)
            return int32_t(hash % 16384);
        });
}

// Calculate the node number for a given key
static void NodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto &node_count = args.data[1];
    
    BinaryExecutor::Execute<string_t, int32_t, int32_t>(input, node_count, result, args.size(),
        [&](string_t input_val, int32_t nodes) {
            if (input_val.IsNull() || nodes <= 0) {
                return int32_t(0);
            }
            
            std::string key(input_val.GetString(), input_val.GetSize());
            std::string hash_tag = extract_hash_tag(key);
            
            // Calculate the CRC16-XMODEM hash
            uint16_t hash = crc16xmodem(hash_tag.c_str(), hash_tag.length());
            
            // Calculate the slot
            int32_t slot = hash % 16384;
            
            // Calculate which node owns this slot
            int32_t slots_per_node = 16384 / nodes;
            return int32_t(slot / slots_per_node);
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    Connection con(instance);
    
    // Register the hash_slot function
    ScalarFunction hash_slot_func = ScalarFunction("hash_slot", {LogicalType::VARCHAR}, 
                                               LogicalType::INTEGER, HashSlotFunction);
    con.CreateScalarFunction(hash_slot_func);
    
    // Register the node function
    ScalarFunction node_func = ScalarFunction("node", {LogicalType::VARCHAR, LogicalType::INTEGER}, 
                                             LogicalType::INTEGER, NodeFunction);
    con.CreateScalarFunction(node_func);
}

void cluster_hash_extensions_init(DatabaseInstance &instance) {
    LoadInternal(instance);
}

// Register the extension as an embedded extension
extern "C" {
    DUCKDB_EXTENSION_API void cluster_hash_init(duckdb::DatabaseInstance &db) {
        LoadInternal(db);
    }
    
    DUCKDB_EXTENSION_API const char *cluster_hash_version() {
        return DuckDB::LibraryVersion();
    }
}

} // namespace duckdb