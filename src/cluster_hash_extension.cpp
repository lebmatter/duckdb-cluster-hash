#define DUCKDB_EXTENSION_MAIN

#include "cluster_hash_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

// CRC16-XMODEM lookup table (matches the Python implementation)
static const uint16_t crc16_lookup_table[256] = {
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823, 0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70, 0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
};

// CRC16-XMODEM implementation using lookup table (matches Python implementation)
static uint16_t crc16xmodem(const char *buf, size_t len) {
    uint16_t crc = 0;
    
    for (size_t i = 0; i < len; i++) {
        uint8_t byte = static_cast<uint8_t>(buf[i]);
        // This is the Python algorithm exactly:
        // crc = ((crc << 8) & 0xff00) ^ lookup[((crc >> 8) & 0xff) ^ byte]
        crc = ((crc << 8) & 0xff00) ^ crc16_lookup_table[((crc >> 8) & 0xff) ^ byte];
    }
    
    return crc & 0xffff;
}

// Extracts the hash tag from the key if present
static std::string extract_hash_tag(const std::string &key) {
    // Direct port of the Python method
    size_t start = key.find("{");
    if (start != std::string::npos) {
        size_t end = key.find("}", start + 1);
        if (end != std::string::npos && end != start + 1) {
            return key.substr(start + 1, end - start - 1);
        }
    }
    return key;
}

// Calculate the hash slot (0-16383) for a given key
static int32_t hash_slot(const std::string &key) {
    // Check for empty key
    if (key.empty()) {
        return 0;
    }
    
    // Get the hash tag (if any)
    std::string hash_tag = extract_hash_tag(key);
    
    // Calculate CRC16
    uint16_t hash = crc16xmodem(hash_tag.c_str(), hash_tag.length());
    
    // Return slot (16384 = CLUSTER_HASH_SLOTS)
    return hash % 16384;
}

// Calculate the node number for a given key and node count
static int32_t node_for_key(const std::string &key, int32_t nodes) {
    // Safety check
    if (nodes <= 0) {
        return 0;
    }
    
    // Get the slot
    int32_t slot = hash_slot(key);
    
    // Calculate which node owns this slot
    int32_t slots_per_node = 16384 / nodes;
    return slot / slots_per_node;
}

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
            if (input_val.GetSize() == 0 || nodes <= 0) {
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