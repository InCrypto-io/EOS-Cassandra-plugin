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

    statement_guard createInsertBlockStatement(
        const std::string& id,
        std::vector<cass_byte_t> block_num_buffer,
        cass_uint32_t block_date,
        const std::string& block);
    statement_guard createInsertTransactionStatement(
        const std::string& id,
        const std::string& transaction);

    void appendStatement(statement_guard&& gStatement);
    void appendStatement(const std::vector<statement_guard>& gStatements);
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

    const size_t MAX_BATCH_SIZE = 10;
    size_t totalBatchSize_;
    std::mutex batchMutex_;
    batch_guard gBatch_;
};