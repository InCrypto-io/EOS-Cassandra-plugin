#include "cassandra_client.h"

#include <algorithm>
#include <bitset>
#include <cstddef>
#include <memory>
#include <type_traits>

#include <appbase/application.hpp>


const std::string CassandraClient::history_keyspace        = "eos_history_test";
const std::string CassandraClient::block_table             = "block";
const std::string CassandraClient::transaction_table       = "transaction";
const std::string CassandraClient::transaction_trace_table = "transaction_trace";


CassandraClient::CassandraClient(const std::string& hostUrl)
    : gCluster_(nullptr, cass_cluster_free),
    gSession_(nullptr, cass_session_free),
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
    connectFuture = cass_session_connect(session, cluster);
    auto g_future = future_guard(connectFuture, cass_future_free);
    err = cass_future_error_code(connectFuture);

    if (err == CASS_OK)
    {
        std::cout << "Connected" << std::endl;
        std::string useQuery = "USE " + history_keyspace;
        auto use_statement = cass_statement_new(useQuery.c_str(), 0);
        auto g_statement = statement_guard(use_statement, cass_statement_free);
        auto use_future = cass_session_execute(session, use_statement);
        g_future = future_guard(use_future, cass_future_free);
        cass_future_wait(use_future);

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


void CassandraClient::pushInsertBlockStatement(
    const std::string& id,
    std::vector<cass_byte_t> blockNumBuffer,
    cass_uint32_t blockDate,
    std::string&& block)
{
    size_t stmtSize = id.size() * sizeof(std::remove_reference<decltype(id)>::type::value_type) +
        blockNumBuffer.size() * sizeof(std::remove_reference<decltype(blockNumBuffer)>::type::value_type) +
        sizeof(blockDate) +
        block.size() * sizeof(std::remove_reference<decltype(block)>::type::value_type);
    auto statement = cass_prepared_bind(gPreparedInsertBlock_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);

    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_bytes_by_name(statement, "block_num", blockNumBuffer.data(), blockNumBuffer.size());
    cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
    cass_statement_bind_string_by_name(statement, "doc", block.c_str());
    appendStatement(std::move(gStatement), stmtSize);
}

void CassandraClient::pushInsertTransactionStatement(
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

void CassandraClient::pushInsertTransactionTraceStatement(
    const std::string& id,
    std::vector<cass_byte_t> blockNumBuffer,
    cass_uint32_t blockDate,
    std::string&& transactionTrace)
{
    size_t stmtSize = id.size() * sizeof(std::remove_reference<decltype(id)>::type::value_type) +
        blockNumBuffer.size() * sizeof(std::remove_reference<decltype(blockNumBuffer)>::type::value_type) +
        sizeof(blockDate) +
        transactionTrace.size() * sizeof(std::remove_reference<decltype(transactionTrace)>::type::value_type);
    auto statement = cass_prepared_bind(gPreparedInsertTransactionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);

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
        std::cout << "Success!" << std::endl;
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(batchFuture, &message, &message_length);
        fprintf(stderr, "Unable to run query: '%.*s'\n",
            (int)message_length, message);
        appbase::app().quit();
    }
}

void CassandraClient::prepareStatements()
{
    bool ok = true;
    CassError rc;
    CassFuture* prepareFuture = nullptr;
    future_guard gFuture(nullptr, cass_future_free);

    std::string insertBlockQuery = "INSERT INTO " + block_table + " (id, block_num, block_date, doc) VALUES(?, ?, ?, ?)";
    std::string insertTransactionQuery = "INSERT INTO " + transaction_table + " (id, doc) VALUES(?, ?)";
    std::string insertTransactionTraceQuery = "INSERT INTO " + transaction_trace_table + " (id, block_num, block_date, doc) VALUES(?, ?, ?, ?)";

    //insert int block
    prepareFuture = cass_session_prepare(gSession_.get(), insertBlockQuery.c_str());
    gFuture.reset(prepareFuture);
    rc = cass_future_error_code(prepareFuture);
    printf("Prepare result: %s\n", cass_error_desc(rc));
    ok &= (rc == CASS_OK);
    gPreparedInsertBlock_.reset(cass_future_get_prepared(prepareFuture));
    //insert into transaction
    prepareFuture = cass_session_prepare(gSession_.get(), insertTransactionQuery.c_str());
    gFuture.reset(prepareFuture);
    rc = cass_future_error_code(prepareFuture);
    printf("Prepare result: %s\n", cass_error_desc(rc));
    ok &= (rc == CASS_OK);
    gPreparedInsertTransaction_.reset(cass_future_get_prepared(prepareFuture));
    //insert into transaction_trace
    prepareFuture = cass_session_prepare(gSession_.get(), insertTransactionTraceQuery.c_str());
    gFuture.reset(prepareFuture);
    rc = cass_future_error_code(prepareFuture);
    printf("Prepare result: %s\n", cass_error_desc(rc));
    ok &= (rc == CASS_OK);
    gPreparedInsertTransactionTrace_.reset(cass_future_get_prepared(prepareFuture));

    if (!ok)
    {
        appbase::app().quit();
    }
}