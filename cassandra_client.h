#pragma once
#include <atomic>
#include <iostream>
#include <mutex>
#include <vector>

#include <cassandra.h>
#include <eosio/chain/action.hpp>
#include <eosio/chain/block_timestamp.hpp>
#include <eosio/chain/contract_types.hpp>
#include <fc/time.hpp>

#include "cassandra_guard.h"


class CassandraClient
{
public:
    explicit CassandraClient(const std::string& hostUrl);
    ~CassandraClient();

    void prepareStatements();

    void insertAccount(
        const eosio::chain::newaccount& newacc,
        const eosio::chain::block_timestamp_type& blockTime);
    void deleteAccountAuth(
        const eosio::chain::deleteauth& del);
    void updateAccountAuth(
        const eosio::chain::updateauth& update);
    void updateAccountAbi(
        const eosio::chain::setabi& setabi);
    
    void insertAccountActionTrace(
        const std::string& account,
        uint32_t shardId,
        std::vector<cass_byte_t> globalSeq,
        fc::time_point blockTime);
    void insertAccountActionTraceShard(
        const std::string& account,
        uint32_t shardId);
    void insertActionTrace(
        std::vector<cass_byte_t> globalSeq,
        fc::time_point blockTime,
        std::string&& actionTrace);
    void insertBlock(
        const std::string& id,
        std::vector<cass_byte_t> blockNumBuffer,
        fc::time_point blockTime,
        std::string&& block);
    void insertTransaction(
        const std::string& id,
        std::string&& transaction);
    void insertTransactionTrace(
        const std::string& id,
        std::vector<cass_byte_t> blockNumBuffer,
        fc::time_point blockTime,
        std::string&& transactionTrace);
    //TODO: onIrreversible, insertAccount, updateAccountAuth, deleteAccountAuth, updateAccountAbi


    static const std::string history_keyspace;
    static const std::string account_table;
    static const std::string account_public_key_table;
    static const std::string account_controlling_account_table;
    static const std::string account_action_trace_table;
    static const std::string account_action_trace_shard_table;
    static const std::string action_trace_table;
    static const std::string block_table;
    static const std::string transaction_table;
    static const std::string transaction_trace_table;

private:
    CassandraClient(const CassandraClient& other) = delete;
    CassandraClient& operator=(const CassandraClient& other) = delete;

    void appendStatement(statement_guard&& gStatement, size_t size);
    void appendStatement(const std::vector<statement_guard>& gStatements, size_t size);
    void executeStatement(statement_guard&& gStatement);
    void flushBatch(batch_guard&& gBatch);


    cluster_guard gCluster_;
    session_guard gSession_;
    prepared_guard gPreparedDeleteAccountPublicKeys_;
    prepared_guard gPreparedDeleteAccountControls_;
    prepared_guard gPreparedInsertAccount_;
    prepared_guard gPreparedInsertAccountAbi_;
    prepared_guard gPreparedInsertAccountPublicKeys_;
    prepared_guard gPreparedInsertAccountControls_;
    prepared_guard gPreparedInsertAccountActionTrace_;
    prepared_guard gPreparedInsertAccountActionTraceShard_;
    prepared_guard gPreparedInsertActionTrace_;
    prepared_guard gPreparedInsertBlock_;
    prepared_guard gPreparedInsertTransaction_;
    prepared_guard gPreparedInsertTransactionTrace_;

    const size_t MAX_BATCH_SIZE = 100 * 1024; //in bytes
    size_t totalBatchSize_;
    std::mutex batchMutex_;
    batch_guard gBatch_;
};