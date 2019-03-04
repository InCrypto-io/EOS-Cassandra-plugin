#include "cassandra_client.h"

#include <algorithm>
#include <bitset>
#include <cstddef>
#include <ctime>
#include <memory>
#include <type_traits>

#include <appbase/application.hpp>
#include <fc/io/json.hpp>
#include <fc/variant.hpp>


const std::string CassandraClient::history_keyspace                  = "eos_history_test";
const std::string CassandraClient::account_table                     = "account";
const std::string CassandraClient::account_public_key_table          = "account_public_key";
const std::string CassandraClient::account_controlling_account_table = "account_controlling_account";
const std::string CassandraClient::account_action_trace_table        = "account_action_trace";
const std::string CassandraClient::account_action_trace_shard_table  = "account_action_trace_shard";
const std::string CassandraClient::action_trace_table                = "action_trace";
const std::string CassandraClient::block_table                       = "block";
const std::string CassandraClient::lib_table                         = "lib";
const std::string CassandraClient::transaction_table                 = "transaction";
const std::string CassandraClient::transaction_trace_table           = "transaction_trace";


CassandraClient::CassandraClient(const std::string& hostUrl)
    : gCluster_(nullptr, cass_cluster_free),
    gSession_(nullptr, cass_session_free),
    gPreparedDeleteAccountPublicKeys_(nullptr, cass_prepared_free),
    gPreparedDeleteAccountControls_(nullptr, cass_prepared_free),
    gPreparedInsertAccount_(nullptr, cass_prepared_free),
    gPreparedInsertAccountAbi_(nullptr, cass_prepared_free),
    gPreparedInsertAccountPublicKeys_(nullptr, cass_prepared_free),
    gPreparedInsertAccountControls_(nullptr, cass_prepared_free),
    gPreparedInsertAccountActionTrace_(nullptr, cass_prepared_free),
    gPreparedInsertAccountActionTraceWithParent_(nullptr, cass_prepared_free),
    gPreparedInsertAccountActionTraceShard_(nullptr, cass_prepared_free),
    gPreparedInsertActionTrace_(nullptr, cass_prepared_free),
    gPreparedInsertActionTraceWithParent_(nullptr, cass_prepared_free),
    gPreparedInsertBlock_(nullptr, cass_prepared_free),
    gPreparedInsertIrreversibleBlock_(nullptr, cass_prepared_free),
    gPreparedInsertTransaction_(nullptr, cass_prepared_free),
    gPreparedInsertTransactionTrace_(nullptr, cass_prepared_free),
    gPreparedUpdateIrreversible_(nullptr, cass_prepared_free)
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
}


void CassandraClient::prepareStatements()
{
    std::string deleteAccountPublicKeysQuery = "DELETE FROM " + account_public_key_table +
        " WHERE name=? and permission=?";
    std::string deleteAccountControlsQuery = "DELETE FROM " + account_controlling_account_table +
        " WHERE name=? and permission=?";
    std::string insertAccountQuery = "INSERT INTO " + account_table +
        " (name, creator, account_create_time) VALUES(?, ?, ?)";
    std::string insertAccountAbiQuery = "INSERT INTO " + account_table +
        " (name, abi) VALUES (?, ?);";
    std::string insertAccountPublicKeysQuery = "INSERT INTO " + account_public_key_table +
        " (name, permission, key) VALUES(?, ?, ?)";
    std::string insertAccountControlsQuery = "INSERT INTO " + account_controlling_account_table +
        " (name, controlling_name, permission) VALUES(?, ?, ?)";
    std::string insertAccountActionTraceQuery = "INSERT INTO " + account_action_trace_table +
        " (account_name, shard_id, global_seq, block_time) VALUES(?, ?, ?, ?)";
    std::string insertAccountActionTraceWithParentQuery = "INSERT INTO " + account_action_trace_table +
        " (account_name, shard_id, global_seq, block_time, parent) VALUES(?, ?, ?, ?, ?)";
    std::string insertAccountActionTraceShardQuery = "INSERT INTO " + account_action_trace_shard_table +
        " (account_name, shard_id) VALUES(?, ?)";
    std::string insertActionTraceQuery = "INSERT INTO " + action_trace_table +
        " (global_seq, block_date, block_time, doc) VALUES(?, ?, ?, ?)";
    std::string insertActionTraceWithParentQuery = "INSERT INTO " + action_trace_table +
        " (global_seq, block_date, block_time, parent) VALUES(?, ?, ?, ?)";
    std::string insertBlockQuery = "INSERT INTO " + block_table +
        " (id, block_num, doc) VALUES(?, ?, ?)";
    std::string insertIrreversibleBlockQuery = "INSERT INTO " + block_table +
        " (id, block_num, irreversible, doc) VALUES(?, ?, true, ?)";
    std::string insertTransactionQuery = "INSERT INTO " + transaction_table +
        " (id, doc) VALUES(?, ?)";
    std::string insertTransactionTraceQuery = "INSERT INTO " + transaction_trace_table +
        " (id, block_num, block_date, doc) VALUES(?, ?, ?, ?)";
    std::string updateIrreversibleQuery = "UPDATE " + lib_table +
        " SET block_num=? where part_key=0 IF block_num<?";

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
    ok &= prepare(deleteAccountPublicKeysQuery,            &gPreparedDeleteAccountPublicKeys_);
    ok &= prepare(deleteAccountControlsQuery,              &gPreparedDeleteAccountControls_);
    ok &= prepare(insertAccountQuery,                      &gPreparedInsertAccount_);
    ok &= prepare(insertAccountAbiQuery,                   &gPreparedInsertAccountAbi_);
    ok &= prepare(insertAccountPublicKeysQuery,            &gPreparedInsertAccountPublicKeys_);
    ok &= prepare(insertAccountControlsQuery,              &gPreparedInsertAccountControls_);
    ok &= prepare(insertAccountActionTraceQuery,           &gPreparedInsertAccountActionTrace_);
    ok &= prepare(insertAccountActionTraceWithParentQuery, &gPreparedInsertAccountActionTraceWithParent_);
    ok &= prepare(insertAccountActionTraceShardQuery,      &gPreparedInsertAccountActionTraceShard_);
    ok &= prepare(insertActionTraceQuery,                  &gPreparedInsertActionTrace_);
    ok &= prepare(insertActionTraceWithParentQuery,        &gPreparedInsertActionTraceWithParent_);
    ok &= prepare(insertBlockQuery,                        &gPreparedInsertBlock_);
    ok &= prepare(insertIrreversibleBlockQuery,            &gPreparedInsertIrreversibleBlock_);
    ok &= prepare(insertTransactionQuery,                  &gPreparedInsertTransaction_);
    ok &= prepare(insertTransactionTraceQuery,             &gPreparedInsertTransactionTrace_);
    ok &= prepare(updateIrreversibleQuery,                 &gPreparedUpdateIrreversible_);

    if (!ok)
    {
        appbase::app().quit();
    }
}

void CassandraClient::insertAccount(
    const eosio::chain::newaccount& newacc,
    const eosio::chain::block_timestamp_type& blockTime)
{
    CassStatement* statement = nullptr;
    CassBatch*         batch = nullptr;
    CassFuture*       future = nullptr;
    statement_guard gStatement(nullptr, cass_statement_free);
    batch_guard gBatch(nullptr, cass_batch_free);

    statement = cass_prepared_bind(gPreparedInsertAccount_.get());
    gStatement.reset(statement);
    int64_t msFromEpoch = (int64_t)blockTime.to_time_point().time_since_epoch().count() / 1000;
    cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "creator", newacc.creator.to_string().c_str());
    cass_statement_bind_int64_by_name(statement, "account_create_time", msFromEpoch);
    auto gInsertAccountFuture = executeStatement(std::move(gStatement));

    batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    gBatch.reset(batch);
    for( const auto& account : newacc.owner.accounts ) {
        statement = cass_prepared_bind(gPreparedInsertAccountControls_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "controlling_name", account.permission.actor.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "owner");
        cass_batch_add_statement(batch, statement);
    }
    for( const auto& account : newacc.active.accounts ) {
        statement = cass_prepared_bind(gPreparedInsertAccountControls_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "controlling_name", account.permission.actor.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "active");
        cass_batch_add_statement(batch, statement);
    }
    future = cass_session_execute_batch(gSession_.get(), batch);
    future_guard gInsertAccountControlsFuture(future, cass_future_free);

    batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    gBatch.reset(batch);
    for( const auto& pub_key_weight : newacc.owner.keys ) {
        statement = cass_prepared_bind(gPreparedInsertAccountPublicKeys_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "owner");
        cass_statement_bind_string_by_name(statement, "key", pub_key_weight.key.operator std::string().c_str());
        cass_batch_add_statement(batch, statement);
    }
    for( const auto& pub_key_weight : newacc.active.keys ) {
        statement = cass_prepared_bind(gPreparedInsertAccountPublicKeys_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "active");
        cass_statement_bind_string_by_name(statement, "key", pub_key_weight.key.operator std::string().c_str());
        cass_batch_add_statement(batch, statement);
    }
    future = cass_session_execute_batch(gSession_.get(), batch);
    future_guard gInsertAccountPublicKeysFuture(future, cass_future_free);

    waitFuture(std::move(gInsertAccountFuture));
    waitFuture(std::move(gInsertAccountControlsFuture));
    waitFuture(std::move(gInsertAccountPublicKeysFuture));
}

void CassandraClient::deleteAccountAuth(
    const eosio::chain::deleteauth& del)
{
    CassStatement* statement = nullptr;
    statement_guard gStatement(nullptr, cass_statement_free);

    statement = cass_prepared_bind(gPreparedDeleteAccountPublicKeys_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", del.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", del.permission.to_string().c_str());
    auto gDeleteAccountPublicKeysFuture = executeStatement(std::move(gStatement));

    statement = cass_prepared_bind(gPreparedDeleteAccountControls_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", del.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", del.permission.to_string().c_str());
    auto gDeleteAccountControlsFuture = executeStatement(std::move(gStatement));

    waitFuture(std::move(gDeleteAccountPublicKeysFuture));
    waitFuture(std::move(gDeleteAccountControlsFuture));
}

void CassandraClient::updateAccountAuth(
    const eosio::chain::updateauth& update)
{
    CassStatement* statement = nullptr;
    CassBatch*         batch = nullptr;
    CassFuture*       future = nullptr;
    statement_guard gStatement(nullptr, cass_statement_free);
    batch_guard gBatch(nullptr, cass_batch_free);

    statement = cass_prepared_bind(gPreparedDeleteAccountPublicKeys_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
    auto gDeleteAccountPublicKeysFuture = executeStatement(std::move(gStatement));

    statement = cass_prepared_bind(gPreparedDeleteAccountControls_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
    auto gDeleteAccountControlsFuture = executeStatement(std::move(gStatement));

    batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    gBatch.reset(batch);
    for( const auto& pub_key_weight : update.auth.keys ) {
        statement = cass_prepared_bind(gPreparedInsertAccountPublicKeys_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "key", pub_key_weight.key.operator std::string().c_str());
        cass_batch_add_statement(batch, statement);
    }
    future = cass_session_execute_batch(gSession_.get(), batch);
    future_guard gInsertAccountPublicKeysFuture(future, cass_future_free);

    batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    gBatch.reset(batch);
    for( const auto& account : update.auth.accounts ) {
        statement = cass_prepared_bind(gPreparedInsertAccountControls_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "controlling_name", account.permission.actor.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
        cass_batch_add_statement(batch, statement);
    }
    future = cass_session_execute_batch(gSession_.get(), batch);
    future_guard gInsertAccountControlsFuture(future, cass_future_free);

    waitFuture(std::move(gDeleteAccountPublicKeysFuture));
    waitFuture(std::move(gDeleteAccountControlsFuture));
    waitFuture(std::move(gInsertAccountPublicKeysFuture));
    waitFuture(std::move(gInsertAccountControlsFuture));
}

void CassandraClient::updateAccountAbi(
    const eosio::chain::setabi& setabi)
{
    auto statement = cass_prepared_bind(gPreparedInsertAccountAbi_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "name", setabi.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "abi", fc::json::to_string(fc::variant(setabi.abi)).c_str());
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}

void CassandraClient::insertAccountActionTrace(
    const std::string& account,
    int64_t shardId,
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime)
{
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", account.c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}

void CassandraClient::insertAccountActionTraceWithParent(
    const std::string& account,
    int64_t shardId,
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime,
    std::vector<cass_byte_t> parent)
{
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTraceWithParent_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", account.c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}

void CassandraClient::insertAccountActionTraceShard(
    const std::string& account,
    int64_t shardId)
{
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTraceShard_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", account.c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}

void CassandraClient::insertActionTrace(
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime,
    std::string&& actionTrace)
{
    auto statement = cass_prepared_bind(gPreparedInsertActionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    cass_statement_bind_string_by_name(statement, "doc", actionTrace.c_str());
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}

void CassandraClient::insertActionTraceWithParent(
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime,
    std::vector<cass_byte_t> parent)
{
    auto statement = cass_prepared_bind(gPreparedInsertActionTraceWithParent_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}

void CassandraClient::insertBlock(
    const std::string& id,
    std::vector<cass_byte_t> blockNumBuffer,
    std::string&& block,
    bool irreversible)
{
    CassStatement* statement = nullptr;
    if (irreversible) {
        statement = cass_prepared_bind(gPreparedInsertIrreversibleBlock_.get());
    }
    else {
        statement = cass_prepared_bind(gPreparedInsertBlock_.get());
    }
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_bytes_by_name(statement, "block_num", blockNumBuffer.data(), blockNumBuffer.size());
    cass_statement_bind_string_by_name(statement, "doc", block.c_str());
    auto gFuture = executeStatement(std::move(gStatement));
    if (irreversible) {
        statement = cass_prepared_bind(gPreparedUpdateIrreversible_.get());
        gStatement.reset(statement);
        cass_statement_bind_bytes_by_name(statement, "block_num", blockNumBuffer.data(), blockNumBuffer.size());
        auto gFuture = executeStatement(std::move(gStatement));
        waitFuture(std::move(gFuture));
    }
    waitFuture(std::move(gFuture));
}

void CassandraClient::insertTransaction(
    const std::string& id,
    std::string&& transaction)
{
    auto statement = cass_prepared_bind(gPreparedInsertTransaction_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_string_by_name(statement, "doc", transaction.c_str());
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}

void CassandraClient::insertTransactionTrace(
    const std::string& id,
    std::vector<cass_byte_t> blockNumBuffer,
    fc::time_point blockTime,
    std::string&& transactionTrace)
{
    auto statement = cass_prepared_bind(gPreparedInsertTransactionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_bytes_by_name(statement, "block_num", blockNumBuffer.data(), blockNumBuffer.size());
    cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
    cass_statement_bind_string_by_name(statement, "doc", transactionTrace.c_str());
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}


void CassandraClient::truncateTable(const std::string& table)
{
    auto statement = cass_statement_new(("TRUNCATE " + table + ";").c_str(), 0);
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_set_request_timeout(statement, 0);
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}
    
void CassandraClient::truncateTables()
{
    truncateTable(account_table);
    truncateTable(account_public_key_table);
    truncateTable(account_controlling_account_table);
    truncateTable(account_action_trace_table);
    truncateTable(account_action_trace_shard_table);
    truncateTable(action_trace_table);
    truncateTable(block_table);
    truncateTable(lib_table);
    truncateTable(transaction_table);
    truncateTable(transaction_trace_table);

    auto statement = cass_statement_new(("INSERT INTO " + lib_table + "(part_key, block_num) VALUES(0, 0);").c_str(), 0);
    auto gStatement = statement_guard(statement, cass_statement_free);
    auto gFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gFuture));
}


future_guard CassandraClient::executeStatement(statement_guard&& gStatement)
{
    auto statement = gStatement.get();
    auto resultFuture = cass_session_execute(gSession_.get(), statement);
    return future_guard(resultFuture, cass_future_free);
}

void CassandraClient::waitFuture(future_guard&& gFuture)
{
    auto cassFuture = gFuture.get();
    if(cass_future_error_code(cassFuture) == CASS_OK) {
        //std::cout << "Success!" << std::endl;
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(cassFuture, &message, &message_length);
        fprintf(stderr, "Unable to run query: '%.*s'\n",
            (int)message_length, message);
        appbase::app().quit();
    }
}