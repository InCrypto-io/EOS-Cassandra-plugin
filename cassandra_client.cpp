#include "cassandra_client.h"

#include <algorithm>
#include <bitset>
#include <cstddef>
#include <ctime>
#include <memory>
#include <type_traits>

#include <appbase/application.hpp>


const std::string CassandraClient::history_keyspace                 = "eos_history_test";
const std::string CassandraClient::account_table                    = "account";
const std::string CassandraClient::account_public_keys_table        = "account_public_keys";
const std::string CassandraClient::account_controls_table           = "account_controls";
const std::string CassandraClient::account_action_trace_table       = "account_action_trace";
const std::string CassandraClient::account_action_trace_shard_table = "account_action_trace_shard";
const std::string CassandraClient::action_trace_table               = "action_trace";
const std::string CassandraClient::block_table                      = "block";
const std::string CassandraClient::transaction_table                = "transaction";
const std::string CassandraClient::transaction_trace_table          = "transaction_trace";


CassandraClient::CassandraClient(const std::string& hostUrl)
    : gCluster_(nullptr, cass_cluster_free),
    gSession_(nullptr, cass_session_free),
    gPreparedDeleteAccountPublicKeys_(nullptr, cass_prepared_free),
    gPreparedDeleteAccountControls_(nullptr, cass_prepared_free),
    gPreparedInsertAccount_(nullptr, cass_prepared_free),
    gPreparedInsertAccountPublicKeys_(nullptr, cass_prepared_free),
    gPreparedInsertAccountControls_(nullptr, cass_prepared_free),
    gPreparedInsertAccountActionTrace_(nullptr, cass_prepared_free),
    gPreparedInsertAccountActionTraceShard_(nullptr, cass_prepared_free),
    gPreparedInsertActionTrace_(nullptr, cass_prepared_free),
    gPreparedInsertBlock_(nullptr, cass_prepared_free),
    gPreparedInsertTransaction_(nullptr, cass_prepared_free),
    gPreparedInsertTransactionTrace_(nullptr, cass_prepared_free),
    totalBatchSize_(0),
    gBatch_(cass_batch_new(CASS_BATCH_TYPE_LOGGED), cass_batch_free)
{
    CassCluster* cluster;
    CassSession* session;
    CassFuture* connectFuture;
    CassError err;

    cluster = cass_cluster_new();
    gCluster_.reset(cluster);
    cass_cluster_set_contact_points(cluster, hostUrl.c_str());

    session = cass_session_new();
    gSession_.reset(session);
    connectFuture = cass_session_connect_keyspace(session, cluster, history_keyspace.c_str());
    auto g_future = future_guard(connectFuture, cass_future_free);
    err = cass_future_error_code(connectFuture);

    if (err == CASS_OK)
    {
        std::cout << "Connected" << std::endl;
        prepareStatements();
    }
    else
    {
        std::cout << "Not connected" << std::endl;
    }
}

CassandraClient::~CassandraClient()
{
    if (totalBatchSize_ != 0)
    {
        flushBatch(std::move(gBatch_));
    }
}


void CassandraClient::prepareStatements()
{
    std::string deleteAccountPublicKeysQuery = "DELETE FROM " + account_public_keys_table +
        " WHERE name=? and permission=?";
    std::string deleteAccountControlsQuery = "DELETE FROM " + account_controls_table +
        " WHERE name=? and permission=?";
    std::string insertAccountQuery = "INSERT INTO " + account_table +
        " (name, creator, account_create_time) VALUES(?, ?, ?)";
    std::string insertAccountPublicKeysQuery = "INSERT INTO " + account_public_keys_table +
        " (name, permission, key) VALUES(?, ?, ?)";
    std::string insertAccountControlsQuery = "INSERT INTO " + account_controls_table +
        " (name, controlling_name, permission) VALUES(?, ?, ?)";
    std::string insertAccountActionTraceQuery = "INSERT INTO " + account_action_trace_table +
        " (account_name, shard_id, global_seq, block_time) VALUES(?, ?, ?, ?)";
    std::string insertAccountActionTraceShardQuery = "INSERT INTO " + account_action_trace_shard_table +
        " (account_name, shard_id) VALUES(?, ?)";
    std::string insertActionTraceQuery = "INSERT INTO " + action_trace_table +
        " (global_seq, block_date, block_time, doc) VALUES(?, ?, ?, ?)";
    std::string insertBlockQuery = "INSERT INTO " + block_table +
        " (id, block_num, block_date, doc) VALUES(?, ?, ?, ?)";
    std::string insertTransactionQuery = "INSERT INTO " + transaction_table +
        " (id, doc) VALUES(?, ?)";
    std::string insertTransactionTraceQuery = "INSERT INTO " + transaction_trace_table +
        " (id, block_num, block_date, doc) VALUES(?, ?, ?, ?)";

    auto prepare = [this](const std::string& query, prepared_guard* prepared) -> bool
    {
        auto prepareFuture = cass_session_prepare(gSession_.get(), query.c_str());
        future_guard gFuture(prepareFuture, cass_future_free);
        auto rc = cass_future_error_code(prepareFuture);
        printf("Prepare result: %s\n", cass_error_desc(rc));
        prepared->reset(cass_future_get_prepared(prepareFuture));
        return (rc == CASS_OK);
    };

    bool ok = true;
    ok &= prepare(deleteAccountPublicKeysQuery,       &gPreparedDeleteAccountPublicKeys_);
    ok &= prepare(deleteAccountControlsQuery,         &gPreparedDeleteAccountControls_);
    ok &= prepare(insertAccountQuery,                 &gPreparedInsertAccount_);
    ok &= prepare(insertAccountPublicKeysQuery,       &gPreparedInsertAccountPublicKeys_);
    ok &= prepare(insertAccountControlsQuery,         &gPreparedInsertAccountControls_);
    ok &= prepare(insertAccountActionTraceQuery,      &gPreparedInsertAccountActionTrace_);
    ok &= prepare(insertAccountActionTraceShardQuery, &gPreparedInsertAccountActionTraceShard_);
    ok &= prepare(insertActionTraceQuery,             &gPreparedInsertActionTrace_);
    ok &= prepare(insertBlockQuery,                   &gPreparedInsertBlock_);
    ok &= prepare(insertTransactionQuery,             &gPreparedInsertTransaction_);
    ok &= prepare(insertTransactionTraceQuery,        &gPreparedInsertTransactionTrace_);

    if (!ok)
    {
        appbase::app().quit();
    }
}

void CassandraClient::insertAccount(
    const eosio::chain::newaccount& newacc,
    const eosio::chain::block_timestamp_type& blockTime)
{
    size_t stmtSize = 0; //TODO: calculate this

    auto statement = cass_prepared_bind(gPreparedInsertAccount_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    int64_t msFromEpoch = (int64_t)blockTime.to_time_point().time_since_epoch().count() / 1000;
    cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "creator", newacc.creator.to_string().c_str());
    cass_statement_bind_int64_by_name(statement, "account_create_time", msFromEpoch);
    appendStatement(std::move(gStatement), stmtSize);

    for( const auto& account : newacc.owner.accounts ) {
        statement = cass_prepared_bind(gPreparedInsertAccountControls_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "controlling_name", account.permission.actor.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "owner");
        appendStatement(std::move(gStatement), stmtSize);
    }
    for( const auto& account : newacc.active.accounts ) {
        statement = cass_prepared_bind(gPreparedInsertAccountControls_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "controlling_name", account.permission.actor.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "active");
        appendStatement(std::move(gStatement), stmtSize);
    }

    for( const auto& pub_key_weight : newacc.owner.keys ) {
        statement = cass_prepared_bind(gPreparedInsertAccountPublicKeys_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "owner");
        cass_statement_bind_string_by_name(statement, "key", pub_key_weight.key.operator std::string().c_str());
        appendStatement(std::move(gStatement), stmtSize);
    }

    for( const auto& pub_key_weight : newacc.active.keys ) {
        statement = cass_prepared_bind(gPreparedInsertAccountPublicKeys_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "active");
        cass_statement_bind_string_by_name(statement, "key", pub_key_weight.key.operator std::string().c_str());
        appendStatement(std::move(gStatement), stmtSize);
    }
}

void CassandraClient::deleteAccountAuth(
    const eosio::chain::deleteauth& del)
{
    size_t stmtSize = 0; //TODO: calculate this

    auto statement = cass_prepared_bind(gPreparedDeleteAccountPublicKeys_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "name", del.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", del.permission.to_string().c_str());
    appendStatement(std::move(gStatement), stmtSize);

    statement = cass_prepared_bind(gPreparedDeleteAccountControls_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", del.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", del.permission.to_string().c_str());
    appendStatement(std::move(gStatement), stmtSize);
}

void CassandraClient::updateAccountAuth(
    const eosio::chain::updateauth& update)
{
    size_t stmtSize = 0; //TODO: calculate this

    auto statement = cass_prepared_bind(gPreparedDeleteAccountPublicKeys_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
    appendStatement(std::move(gStatement), stmtSize);

    statement = cass_prepared_bind(gPreparedDeleteAccountControls_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
    appendStatement(std::move(gStatement), stmtSize);

    for( const auto& pub_key_weight : update.auth.keys ) {
        statement = cass_prepared_bind(gPreparedInsertAccountPublicKeys_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "key", pub_key_weight.key.operator std::string().c_str());
        appendStatement(std::move(gStatement), stmtSize);
    }

    for( const auto& account : update.auth.accounts ) {
        statement = cass_prepared_bind(gPreparedInsertAccountControls_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "controlling_name", account.permission.actor.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
        appendStatement(std::move(gStatement), stmtSize);
    }
}

void CassandraClient::insertAccountActionTrace(
    const std::string& account,
    uint32_t shardId,
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime)
{
    size_t stmtSize = account.size() * sizeof(std::remove_reference<decltype(account)>::type::value_type) +
        sizeof(int64_t) * 2 +
        globalSeq.size() * sizeof(std::remove_reference<decltype(globalSeq)>::type::value_type);

    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", account.c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", (int64_t)shardId * 1000);
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    appendStatement(std::move(gStatement), stmtSize);
}

void CassandraClient::insertAccountActionTraceShard(
    const std::string& account,
    uint32_t shardId)
{
    size_t stmtSize = account.size() * sizeof(std::remove_reference<decltype(account)>::type::value_type) +
        sizeof(shardId);

    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTraceShard_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", account.c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", (int64_t)shardId * 1000);
    appendStatement(std::move(gStatement), stmtSize);
}

void CassandraClient::insertActionTrace(
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime,
    std::string&& actionTrace)
{
    //TODO: calculate statement size
    size_t stmtSize = globalSeq.size() * sizeof(std::remove_reference<decltype(globalSeq)>::type::value_type) +
        sizeof(cass_uint32_t) +
        sizeof(int64_t) +
        actionTrace.size() * sizeof(std::remove_reference<decltype(actionTrace)>::type::value_type);

    auto statement = cass_prepared_bind(gPreparedInsertActionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    cass_statement_bind_string_by_name(statement, "doc", actionTrace.c_str());
    appendStatement(std::move(gStatement), stmtSize);
}

void CassandraClient::insertBlock(
    const std::string& id,
    std::vector<cass_byte_t> blockNumBuffer,
    fc::time_point blockTime,
    std::string&& block)
{
    size_t stmtSize = id.size() * sizeof(std::remove_reference<decltype(id)>::type::value_type) +
        blockNumBuffer.size() * sizeof(std::remove_reference<decltype(blockNumBuffer)>::type::value_type) +
        sizeof(cass_uint32_t) +
        block.size() * sizeof(std::remove_reference<decltype(block)>::type::value_type);
    auto statement = cass_prepared_bind(gPreparedInsertBlock_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);

    cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_bytes_by_name(statement, "block_num", blockNumBuffer.data(), blockNumBuffer.size());
    cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
    cass_statement_bind_string_by_name(statement, "doc", block.c_str());
    appendStatement(std::move(gStatement), stmtSize);
}

void CassandraClient::insertTransaction(
    const std::string& id,
    std::string&& transaction)
{
    size_t stmtSize = id.size() * sizeof(std::remove_reference<decltype(id)>::type::value_type) +
        transaction.size() * sizeof(std::remove_reference<decltype(transaction)>::type::value_type);
    auto statement = cass_prepared_bind(gPreparedInsertTransaction_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);

    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_string_by_name(statement, "doc", transaction.c_str());
    appendStatement(std::move(gStatement), stmtSize);
}

void CassandraClient::insertTransactionTrace(
    const std::string& id,
    std::vector<cass_byte_t> blockNumBuffer,
    fc::time_point blockTime,
    std::string&& transactionTrace)
{
    size_t stmtSize = id.size() * sizeof(std::remove_reference<decltype(id)>::type::value_type) +
        blockNumBuffer.size() * sizeof(std::remove_reference<decltype(blockNumBuffer)>::type::value_type) +
        sizeof(cass_uint32_t) +
        transactionTrace.size() * sizeof(std::remove_reference<decltype(transactionTrace)>::type::value_type);
    auto statement = cass_prepared_bind(gPreparedInsertTransactionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);


    cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_bytes_by_name(statement, "block_num", blockNumBuffer.data(), blockNumBuffer.size());
    cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
    cass_statement_bind_string_by_name(statement, "doc", transactionTrace.c_str());
    appendStatement(std::move(gStatement), stmtSize);
}


void CassandraClient::appendStatement(statement_guard&& gStatement, size_t size)
{
    bool needFlush = false;
    batch_guard tmp = batch_guard(cass_batch_new(CASS_BATCH_TYPE_LOGGED), cass_batch_free);
    {
        std::lock_guard<std::mutex> guard(batchMutex_);
        cass_batch_add_statement(gBatch_.get(), gStatement.get());
        totalBatchSize_ += size;
        if (totalBatchSize_ > MAX_BATCH_SIZE)
        {
            needFlush = true;
            gBatch_.swap(tmp);
            totalBatchSize_ = 0;
        }
    }
    if (needFlush) {
        flushBatch(std::move(tmp));
    }
}

void CassandraClient::appendStatement(const std::vector<statement_guard>& gStatements, size_t size)
{
    bool needFlush = false;
    batch_guard tmp = batch_guard(cass_batch_new(CASS_BATCH_TYPE_LOGGED), cass_batch_free);
    {
        std::lock_guard<std::mutex> guard(batchMutex_);
        for (int i = 0; i < gStatements.size(); i++)
        {
            cass_batch_add_statement(gBatch_.get(), gStatements[i].get());
        }
        totalBatchSize_ += size;
        if (totalBatchSize_ > MAX_BATCH_SIZE)
        {
            needFlush = true;
            gBatch_.swap(tmp);
            totalBatchSize_ = 0;
        }
    }
    if (needFlush) {
        flushBatch(std::move(tmp));
    }
}

void CassandraClient::executeStatement(statement_guard&& gStatement)
{
    auto statement = gStatement.get();
    auto resultFuture = cass_session_execute(gSession_.get(), statement);
    auto gFuture = future_guard(resultFuture, cass_future_free);
    if(cass_future_error_code(resultFuture) == CASS_OK) {
        std::cout << "Success!" << std::endl;
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(resultFuture, &message, &message_length);
        fprintf(stderr, "Unable to run query: '%.*s'\n",
            (int)message_length, message);
        appbase::app().quit();
    }
}

void CassandraClient::flushBatch(batch_guard&& gBatch)
{
    CassFuture* batchFuture = cass_session_execute_batch(gSession_.get(), gBatch.get());
    auto gFuture = future_guard(batchFuture, cass_future_free);
    if(cass_future_error_code(batchFuture) == CASS_OK) {
        //std::cout << "Success!" << std::endl;
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(batchFuture, &message, &message_length);
        fprintf(stderr, "Unable to run query: '%.*s'\n",
            (int)message_length, message);
        appbase::app().quit();
    }
}