#pragma once
#include <atomic>
#include <iostream>
#include <mutex>
#include <vector>

#include <cassandra.h>

#include "cassandra_guard.h"


class CassandraClient
{
public:
    explicit CassandraClient(const std::string& hostUrl);
    ~CassandraClient();

    void pushInsertBlockStatement(
        const std::string& id,
        std::vector<cass_byte_t> blockNumBuffer,
        cass_uint32_t blockDate,
        std::string&& block);
    void pushInsertTransactionStatement(
        const std::string& id,
        std::string&& transaction);
    void pushInsertTransactionTraceStatement(
        const std::string& id,
        std::vector<cass_byte_t> blockNumBuffer,
        cass_uint32_t blockDate,
        std::string&& transactionTrace);

    void appendStatement(statement_guard&& gStatement, size_t size);
    void appendStatement(const std::vector<statement_guard>& gStatements, size_t size);
    void executeStatement(statement_guard&& gStatement);
    void flushBatch(batch_guard&& gBatch);
    void prepareStatements();


    static const std::string history_keyspace;
    static const std::string block_table;
    static const std::string transaction_table;
    static const std::string transaction_trace_table;

private:
    CassandraClient(const CassandraClient& other) = delete;
    CassandraClient& operator=(const CassandraClient& other) = delete;

    cluster_guard gCluster_;
    session_guard gSession_;
    prepared_guard gPreparedInsertBlock_;
    prepared_guard gPreparedInsertTransaction_;
    prepared_guard gPreparedInsertTransactionTrace_;

    const size_t MAX_BATCH_SIZE = 100 * 1024; //in bytes
    size_t totalBatchSize_;
    std::mutex batchMutex_;
    batch_guard gBatch_;
};