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
#include <fc/variant_object.hpp>


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


CassandraClient::CassandraClient(const std::string& hostUrl, const std::string& keyspace)
    : failed(appbase::app().data_dir() / "cass_failed", eosio::chain::database::read_write, 4096*1024*1024ll),
    gCluster_(nullptr, cass_cluster_free),
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
    failed.add_index<eosio::upsert_account_multi_index>();
    failed.add_index<eosio::insert_account_action_trace_multi_index>();
    failed.add_index<eosio::insert_account_action_trace_shard_multi_index>();
    failed.add_index<eosio::insert_action_trace_multi_index>();
    
    CassCluster* cluster;
    CassSession* session;
    CassFuture* connectFuture;
    CassError err;

    cluster = cass_cluster_new();
    gCluster_.reset(cluster);
    cass_cluster_set_contact_points(cluster, hostUrl.c_str());

    session = cass_session_new();
    gSession_.reset(session);
    connectFuture = cass_session_connect_keyspace(session, cluster, keyspace.c_str());
    auto gFuture = future_guard(connectFuture, cass_future_free);
    err = cass_future_error_code(connectFuture);

    if (err == CASS_OK)
    {
        ilog("Connected to Apache Cassandra");
        prepareStatements();
        initLib();
    }
    else
    {
        const char* message;
        size_t message_length;
        cass_future_error_message(connectFuture, &message, &message_length);
        elog("Unable to connect to Apache Cassandra: ${desc}", ("desc", std::string(message, message_length)));
        appbase::app().quit();
    }
}

CassandraClient::~CassandraClient()
{
    std::cout << "countAcc: " << countAcc << std::endl;
    std::cout << "countAccAcTrace: " << countAccAcTrace << std::endl;
    std::cout << "countAccAcTraceShard: " << countAccAcTraceShard << std::endl;
    std::cout << "countAcTrace: " << countAcTrace << std::endl;
    std::cout << "countAcTraceWP: " << countAcTraceWP << std::endl;
}


void CassandraClient::initLib()
{
    auto statement = cass_statement_new(("INSERT INTO " + lib_table + " (part_key, block_num) VALUES(0, 0) IF NOT EXISTS;").c_str(), 0);
    statement_guard gStatement(statement, cass_statement_free);
    auto gInitLIBFuture = executeStatement(std::move(gStatement));
    waitFuture(std::move(gInitLIBFuture));
}

void CassandraClient::insertFailed()
{
    const auto& accountUpsertsIdx = failed.get_index<eosio::upsert_account_multi_index, eosio::by_id>();
    const auto& insertAccountActionTraceIdx = failed.get_index<eosio::insert_account_action_trace_multi_index, eosio::by_account_shard_id>();
    const auto& insertAccountActionTraceShardIdx = failed.get_index<eosio::insert_account_action_trace_shard_multi_index, eosio::by_account_shard_id>();
    const auto& insertActionTraceIdx = failed.get_index<eosio::insert_action_trace_multi_index, eosio::by_id>();
    int cAcc = 0;
    int cAccAcTrace = 0;
    int cAccAcTraceShard = 0;
    int cAcTrace = 0;
    int cAcTraceWP = 0;
    for (auto it = std::begin(accountUpsertsIdx); it != std::end(accountUpsertsIdx); it++)
    {
        cAcc++;
        std::cout << "Action names:" << std::endl;
        std::cout << it->name << std::endl;
        if( it->name == eosio::chain::newaccount::get_name() ) {
            eosio::chain::newaccount newacc;
            fc::datastream<const char*> ds( it->data.data(), it->data.size() );
            fc::raw::unpack( ds, newacc );
        }
        else if( it->name == eosio::chain::updateauth::get_name() ) {
            eosio::chain::updateauth update;
            fc::datastream<const char*> ds( it->data.data(), it->data.size() );
            fc::raw::unpack( ds, update );
        }
        else if( it->name == eosio::chain::deleteauth::get_name() ) {
            eosio::chain::deleteauth del;
            fc::datastream<const char*> ds( it->data.data(), it->data.size() );
            fc::raw::unpack( ds, del );
        }
        else if( it->name == eosio::chain::setabi::get_name() ) {
            eosio::chain::setabi setabi;
            fc::datastream<const char*> ds( it->data.data(), it->data.size() );
            fc::raw::unpack( ds, setabi );
        }
    }
    for (auto it = std::begin(insertAccountActionTraceIdx); it != std::end(insertAccountActionTraceIdx); it++)
    {
        cAccAcTrace++;
        std::cout << "insertAccountActionTraceIdx:" << std::endl;
        std::cout << it->account << "\t" << it->shardId << std::endl;
    }
    for (auto it = std::begin(insertAccountActionTraceShardIdx); it != std::end(insertAccountActionTraceShardIdx); it++)
    {
        cAccAcTraceShard++;
        std::cout << "insertAccountActionTraceShardIdx:" << std::endl;
        std::cout << it->account << "\t" << it->shardId << std::endl;
    }
    for (auto it = std::begin(insertActionTraceIdx); it != std::end(insertActionTraceIdx); it++)
    {
        std::cout << "insertActionTraceIdx:" << std::endl;
        if (!it->actionTrace.empty())
        {
            std::cout << "Insert full action" << std::endl;
            cAcTrace++;
            std::string s;
            // s.resize(it->actionTrace.size());
            // std::copy(it->actionTrace.begin(), it->actionTrace.end()/* + it->actionTrace.size()*/, s.begin());
            s = it->actionTrace.data();
            std::cout << s << std::endl;
        }
        else
        {
            std::cout << "Insert action with parent" << std::endl;
            cAcTraceWP++;
        }
        std::vector<cass_byte_t> gs;
        std::vector<cass_byte_t> parent;
        gs.resize( it->globalSeq.size() );
        for (int i = 0; i < it->globalSeq.size(); i++)
        {
            gs[i] = it->globalSeq[i];
        }
        parent.resize( it->parent.size() );
        for (int i = 0; i < it->parent.size(); i++)
        {
            parent[i] = it->parent[i];
        }
        std::cout << "Block time: " << fc::string(it->blockTime) << std::endl;
        std::cout << "Global seq" << std::endl;
        for (auto& b : gs)
        {
            std::cout << (int)b;
        }
        std::cout << std::endl;
        std::cout << "Parent" << std::endl;
        for (auto& b : parent)
        {
            std::cout << (int)b;
        }
        std::cout << std::endl;
    }
    std::cout << "cAcc C: " << cAcc << std::endl;
    std::cout << "cAccAcTrace C: " << cAccAcTrace << std::endl;
    std::cout << "cAccAcTraceShard C: " << cAccAcTraceShard << std::endl;
    std::cout << "cAcTrace C: " << cAcTrace << std::endl;
    std::cout << "cAcTraceWP C: " << cAcTraceWP << std::endl;
    std::cout << "Free memory: " << failed.get_free_memory() << std::endl; //4294965120

    while(!accountUpsertsIdx.empty()) {
        failed.remove(*accountUpsertsIdx.begin());
    }
    std::cout << "accountUpsertsIdx deleted" << std::endl;
    while(!insertAccountActionTraceIdx.empty()) {
        failed.remove(*insertAccountActionTraceIdx.begin());
    }
    std::cout << "insertAccountActionTraceIdx deleted" << std::endl;
    while(!insertAccountActionTraceShardIdx.empty()) {
        failed.remove(*insertAccountActionTraceShardIdx.begin());
    }
    std::cout << "insertAccountActionTraceShardIdx deleted" << std::endl;
    while(!insertActionTraceIdx.empty()) {
        failed.remove(*insertActionTraceIdx.begin());
    }
    std::cout << "insertActionTraceIdx deleted" << std::endl;
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
        if (rc != CASS_OK) {
            elog("Prepare query error: ${desc}, query: ${query}",
                ("desc", cass_error_desc(rc))("query", query));
        }
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

void CassandraClient::batchInsertActionTraceWithParent(
    const std::vector<std::tuple<std::vector<cass_byte_t>, fc::time_point, std::vector<cass_byte_t>>>& data)
{
    CassStatement* statement = nullptr;
    auto batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
    batch_guard gBatch(batch, cass_batch_free);

    for (const auto& val : data)
    {
        std::vector<cass_byte_t> globalSeq;
        fc::time_point blockTime;
        std::vector<cass_byte_t> parent;
        std::tie(globalSeq, blockTime, parent) = val;
        statement = cass_prepared_bind(gPreparedInsertActionTraceWithParent_.get());
        auto gStatement = statement_guard(statement, cass_statement_free);
        cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
        int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
        cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
        cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
        cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
        cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
        cass_batch_add_statement(batch, statement);
    }
    auto future = cass_session_execute_batch(gSession_.get(), batch);
    future_guard gFuture(future, cass_future_free);

    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;

        std::lock_guard<std::mutex> lock(db_mtx);
        for (const auto& val : data)
        {
            std::vector<cass_byte_t> globalSeq;
            fc::time_point blockTime;
            std::vector<cass_byte_t> parent;
            std::tie(globalSeq, blockTime, parent) = val;
            countAcTraceWP++;

            failed.create<eosio::insert_action_trace_object>([&]( auto& obj ) {
                obj.setGlobalSeq(globalSeq);
                obj.blockTime = blockTime;
                obj.setParent(parent);
            });
        }
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gFuture), f);
}

void CassandraClient::insertAccount(
    const eosio::chain::newaccount& newacc,
    fc::time_point blockTime)
{
    CassStatement* statement = nullptr;
    CassBatch*         batch = nullptr;
    CassFuture*       future = nullptr;
    statement_guard gStatement(nullptr, cass_statement_free);
    batch_guard gBatch(nullptr, cass_batch_free);

    statement = cass_prepared_bind(gPreparedInsertAccount_.get());
    gStatement.reset(statement);
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
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

    bool errorHandled = false;
    auto f = [this, &newacc, &blockTime, &errorHandled]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);
        countAcc++;

        failed.create<eosio::upsert_account_object>([&]( auto& obj ) {
            obj.name = eosio::chain::newaccount::get_name();
            obj.blockTime = blockTime;
            obj.data.resize( fc::raw::pack_size( newacc ) );
            fc::datastream<char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::pack( ds, newacc );
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gInsertAccountFuture), f);
    waitFuture(std::move(gInsertAccountControlsFuture), f);
    waitFuture(std::move(gInsertAccountPublicKeysFuture), f);
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

    bool errorHandled = false;
    auto f = [this, &del, &errorHandled]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);
        countAcc++;

        failed.create<eosio::upsert_account_object>([&]( auto& obj ) {
            obj.name = eosio::chain::deleteauth::get_name();
            obj.data.resize( fc::raw::pack_size( del ) );
            fc::datastream<char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::pack( ds, del );
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gDeleteAccountPublicKeysFuture), f);
    waitFuture(std::move(gDeleteAccountControlsFuture), f);
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

    bool errorHandled = false;
    auto f = [this, &update, &errorHandled]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);
        countAcc++;

        failed.create<eosio::upsert_account_object>([&]( auto& obj ) {
            obj.name = eosio::chain::updateauth::get_name();
            obj.data.resize( fc::raw::pack_size( update ) );
            fc::datastream<char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::pack( ds, update );
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gDeleteAccountPublicKeysFuture), f);
    waitFuture(std::move(gDeleteAccountControlsFuture), f);
    waitFuture(std::move(gInsertAccountPublicKeysFuture), f);
    waitFuture(std::move(gInsertAccountControlsFuture), f);
}

void CassandraClient::updateAccountAbi(
    const eosio::chain::setabi& setabi)
{
    auto statement = cass_prepared_bind(gPreparedInsertAccountAbi_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "name", setabi.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "abi", fc::json::to_string(fc::variant(setabi.abi)).c_str());
    auto gFuture = executeStatement(std::move(gStatement));

    bool errorHandled = false;
    auto f = [this, &setabi, &errorHandled]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);
        countAcc++;

        failed.create<eosio::upsert_account_object>([&]( auto& obj ) {
            obj.name = eosio::chain::setabi::get_name();
            obj.data.resize( fc::raw::pack_size( setabi ) );
            fc::datastream<char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::pack( ds, setabi );
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gFuture), f);
}

void CassandraClient::insertAccountActionTrace(
    const eosio::chain::account_name& account,
    int64_t shardId,
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime)
{
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    auto gFuture = executeStatement(std::move(gStatement));

    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);
        countAccAcTrace++;

        failed.create<eosio::insert_account_action_trace_object>([&]( auto& obj ) {
            obj.account = account;
            obj.shardId = shardId;
            obj.setGlobalSeq(globalSeq);
            obj.blockTime = blockTime;
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gFuture), f);
}

void CassandraClient::insertAccountActionTraceWithParent(
    const eosio::chain::account_name& account,
    int64_t shardId,
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime,
    std::vector<cass_byte_t> parent)
{
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTraceWithParent_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
    auto gFuture = executeStatement(std::move(gStatement));

    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);
        countAccAcTrace++;

        failed.create<eosio::insert_account_action_trace_object>([&]( auto& obj ) {
            obj.account = account;
            obj.shardId = shardId;
            obj.setGlobalSeq(globalSeq);
            obj.blockTime = blockTime;
            obj.setParent(parent);
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gFuture), f);
}

void CassandraClient::insertAccountActionTraceShard(
    const eosio::chain::account_name& account,
    int64_t shardId)
{
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTraceShard_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    auto gFuture = executeStatement(std::move(gStatement));

    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);
        countAccAcTraceShard++;

        failed.create<eosio::insert_account_action_trace_shard_object>([&]( auto& obj ) {
            obj.account = account;
            obj.shardId = shardId;
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gFuture), f);
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

    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);
        countAcTrace++;

        std::cout << "Block time: " << fc::string(blockTime) << std::endl;
        std::cout << "Global seq" << std::endl;
        for (auto& b : globalSeq)
        {
            std::cout << (int)b;
        }
        std::cout << std::endl;

        failed.create<eosio::insert_action_trace_object>([&]( auto& obj ) {
            obj.setGlobalSeq(globalSeq);
            obj.blockTime = blockTime;
            obj.actionTrace.resize(actionTrace.size());
            std::copy(actionTrace.begin(), actionTrace.end(),
                obj.actionTrace.begin());
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gFuture), f);
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

    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);
        countAcTraceWP++;

        failed.create<eosio::insert_action_trace_object>([&]( auto& obj ) {
            obj.setGlobalSeq(globalSeq);
            obj.blockTime = blockTime;
            obj.setParent(parent);
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    waitFuture(std::move(gFuture), f);
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


statement_guard CassandraClient::createInsertAccountActionTraceStatement(
    const eosio::chain::account_name& account,
    int64_t shardId,
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime) const
{
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    return gStatement;
}

statement_guard CassandraClient::createInsertAccountActionTraceWithParentStatement(
    const eosio::chain::account_name& account,
    int64_t shardId,
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime,
    std::vector<cass_byte_t> parent) const
{
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTraceWithParent_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
    return gStatement;
}

statement_guard CassandraClient::createInsertAccountActionTraceShardStatement(
    const eosio::chain::account_name& account,
    int64_t shardId) const
{
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTraceShard_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    return gStatement;
}

future_guard CassandraClient::executeBatch(batch_guard&& b)
{
    return future_guard(cass_session_execute_batch(gSession_.get(), b.get()), cass_future_free);
}

batch_guard CassandraClient::createLoggedBatch()
{
    return batch_guard(cass_batch_new(CASS_BATCH_TYPE_LOGGED), cass_batch_free);
}

batch_guard CassandraClient::createUnloggedBatch()
{
    return batch_guard(cass_batch_new(CASS_BATCH_TYPE_UNLOGGED), cass_batch_free);
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
    if(cass_future_error_code(cassFuture) != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(cassFuture, &message, &message_length);
        elog("Unable to run query: ${desc}", ("desc", std::string(message, message_length)));
        appbase::app().quit();
    }
}


void CassandraClient::waitFuture(future_guard&& gFuture, const std::function<void()>& onError)
{
    auto cassFuture = gFuture.get();
    if(cass_future_error_code(cassFuture) != CASS_OK) {
        onError();
        
        const char* message;
        size_t message_length;
        cass_future_error_message(cassFuture, &message, &message_length);
        elog("Unable to run query: ${desc}", ("desc", std::string(message, message_length)));
        appbase::app().quit();
    }
}