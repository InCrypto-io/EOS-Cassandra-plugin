/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/cassandra_history_plugin/cassandra_history_plugin.hpp>
#include <eosio/cassandra_history_plugin/account_action_trace_shard_object.hpp>
#include <eosio/chain/exceptions.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/algorithm/string.hpp>

#include <ctime>
#include <memory>
#include <set>

#include "cassandra_client.h"


namespace eosio {
   static appbase::abstract_plugin& _cassandra_history_plugin = app().register_plugin<cassandra_history_plugin>();

struct filter_entry {
   name receiver;
   name action;
   name actor;

   friend bool operator<( const filter_entry& a, const filter_entry& b ) {
      return std::tie( a.receiver, a.action, a.actor ) < std::tie( b.receiver, b.action, b.actor );
   }

   //            receiver          action       actor
   bool match( const name& rr, const name& an, const name& ar ) const {
      return (receiver.value == 0 || receiver == rr) &&
             (action.value == 0 || action == an) &&
             (actor.value == 0 || actor == ar);
   }
};

class cassandra_history_plugin_impl {
   public:
   cassandra_history_plugin_impl();
   ~cassandra_history_plugin_impl();

   fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
   fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
   fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
   fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

   std::set<account_name> account_set( const chain::base_action_trace& act ) {
      std::set<account_name> result;

      result.insert( act.receipt.receiver );
      for( const auto& a : act.act.authorization ) {
         /*if( bypass_filter ||
               filter_on.find({ act.receipt.receiver, 0, 0}) != filter_on.end() ||
               filter_on.find({ act.receipt.receiver, 0, a.actor}) != filter_on.end() ||
               filter_on.find({ act.receipt.receiver, act.act.name, 0}) != filter_on.end() ||
               filter_on.find({ act.receipt.receiver, act.act.name, a.actor }) != filter_on.end() ) {
            if ((filter_out.find({ act.receipt.receiver, 0, 0 }) == filter_out.end()) &&
               (filter_out.find({ act.receipt.receiver, 0, a.actor }) == filter_out.end()) &&
               (filter_out.find({ act.receipt.receiver, act.act.name, 0 }) == filter_out.end()) &&
               (filter_out.find({ act.receipt.receiver, act.act.name, a.actor }) == filter_out.end())) {*/
               result.insert( a.actor );
            /*}
         }*/
      }
      return result;
   }

   void accepted_block(const chain::block_state_ptr&);
   void applied_irreversible_block(const chain::block_state_ptr&);
   void accepted_transaction(const chain::transaction_metadata_ptr&);
   void applied_transaction(const chain::transaction_trace_ptr&);

   void process_accepted_block( chain::block_state_ptr );
   //void _process_accepted_block( chain::block_state_ptr );
   void process_irreversible_block( chain::block_state_ptr );
   //void _process_irreversible_block( chain::block_state_ptr );
   void process_accepted_transaction(chain::transaction_metadata_ptr);
   //void _process_accepted_transaction(chain::transaction_metadata_ptr);
   void process_applied_transaction(chain::transaction_trace_ptr);
   //void _process_applied_transaction(chain::transaction_trace_ptr);

   void upsertAccount(
      const chain::action& act,
      const chain::block_timestamp_type& block_time);

   bool filter_include( const account_name& receiver, const action_name& act_name,
                        const vector<chain::permission_level>& authorization ) const;
   bool filter_include( const transaction& trx ) const;

   void init();

   bool filter_on_star = true;
   std::set<filter_entry> filter_on;
   std::set<filter_entry> filter_out;


   chain_plugin* chain_plug = nullptr;
   boost::atomic<bool> done{false};
   boost::atomic<bool> startup{true};
   fc::optional<chain::chain_id_type> chain_id;
   fc::microseconds abi_serializer_max_time_ms;

   std::unique_ptr<CassandraClient> cas_client;
};

bool cassandra_history_plugin_impl::filter_include( const account_name& receiver, const action_name& act_name,
                                           const vector<chain::permission_level>& authorization ) const
{
   bool include = false;
   if( filter_on_star ) {
      include = true;
   } else {
      auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name]( const auto& filter ) {
         return filter.match( receiver, act_name, 0 );
      } );
      if( itr != filter_on.cend() ) {
         include = true;
      } else {
         for( const auto& a : authorization ) {
            auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
               return filter.match( receiver, act_name, a.actor );
            } );
            if( itr != filter_on.cend() ) {
               include = true;
               break;
            }
         }
      }
   }

   if( !include ) { return false; }
   if( filter_out.empty() ) { return true; }

   auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name]( const auto& filter ) {
      return filter.match( receiver, act_name, 0 );
   } );
   if( itr != filter_out.cend() ) { return false; }

   for( const auto& a : authorization ) {
      auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
         return filter.match( receiver, act_name, a.actor );
      } );
      if( itr != filter_out.cend() ) { return false; }
   }

   return true;
}

bool cassandra_history_plugin_impl::filter_include( const transaction& trx ) const
{
   if( !filter_on_star || !filter_out.empty() ) {
      bool include = false;
      for( const auto& a : trx.actions ) {
         if( filter_include( a.account, a.name, a.authorization ) ) {
            include = true;
            break;
         }
      }
      if( !include ) {
         for( const auto& a : trx.context_free_actions ) {
            if( filter_include( a.account, a.name, a.authorization ) ) {
               include = true;
               break;
            }
         }
      }
      return include;
   }
   return true;
}


cassandra_history_plugin_impl::cassandra_history_plugin_impl()
{
}

cassandra_history_plugin_impl::~cassandra_history_plugin_impl()
{
   if (!startup) {
      try {
         ilog( "cassandra_history_plugin shutdown in process please be patient this can take a few minutes" );
         done = true;
         //condition.notify_one();

         //consume_thread.join();
      } catch( std::exception& e ) {
         elog( "Exception on cassandra_history_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
   }
}

void cassandra_history_plugin_impl::init() {

   startup = false;
}


void cassandra_history_plugin_impl::accepted_block( const chain::block_state_ptr& bs ) {
   process_accepted_block(bs);
}

void cassandra_history_plugin_impl::applied_irreversible_block( const chain::block_state_ptr& bs ) {
   
}

void cassandra_history_plugin_impl::accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   process_accepted_transaction(t);
}

void cassandra_history_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t ) {
   if( !t->producer_block_id.valid() )
      return;
   
   process_applied_transaction(t);
}


void cassandra_history_plugin_impl::process_accepted_block(chain::block_state_ptr bs) {
   auto block_num = bs->block_num;
   if( block_num % 2000 == 0 )
   {
      ilog( "block_num: ${b}", ("b", block_num) );
      appbase::app().quit();
   }

   const auto block_id = bs->id;
   const auto block_id_str = block_id.str();
   std::vector<uint8_t> buffer;
   buffer.reserve(sizeof(block_num));
   for (int i = sizeof(block_num) - 1; i >= 0; --i)
   {
      uint8_t byte = (block_num >> 8 * i);
      if (buffer.empty() && !byte)
      {
         continue;
      }
      if (buffer.empty() && byte & 0x80)
      {
         buffer.push_back(0x0);
      }
      buffer.push_back(byte & 0xFF);
   }
   auto block_time_point = bs->header.timestamp.to_time_point();
   fc::variant bs_doc(bs);
   auto json_block = fc::prune_invalid_utf8(fc::json::to_string(bs_doc));

   cas_client->insertBlock(block_id_str, buffer, block_time_point, std::move(json_block));
}

void cassandra_history_plugin_impl::process_accepted_transaction(chain::transaction_metadata_ptr t) {
   const auto& trx = t->trx;
   if( !filter_include( trx ) ) return;

   const auto& trx_id = t->id;
   const auto trx_id_str = trx_id.str();

   fc::variant trx_doc(trx);
   auto json_trx = fc::prune_invalid_utf8(fc::json::to_string(trx_doc));
   cas_client->insertTransaction(trx_id_str, std::move(json_trx));
}

void cassandra_history_plugin_impl::process_applied_transaction(chain::transaction_trace_ptr t) {

   std::vector<std::reference_wrapper<chain::base_action_trace>> base_action_traces; // without inline action traces

   bool executed = t->receipt.valid() && t->receipt->status == chain::transaction_receipt_header::executed;

   std::stack<std::reference_wrapper<chain::action_trace>> stack;
   for( auto& atrace : t->action_traces ) {
      stack.emplace(atrace);

      while ( !stack.empty() )
      {
         auto &atrace = stack.top().get();
         stack.pop();

         if (atrace.except.valid())
         {
            continue;
         }

         if(executed && atrace.receipt.receiver == chain::config::system_account_name) {
            upsertAccount(atrace.act, t->block_time);
         }

         if (filter_include(atrace.receipt.receiver, atrace.act.name, atrace.act.authorization)) {
            base_action_traces.emplace_back( atrace );
         }

         auto &inline_traces = atrace.inline_traces;
         for( auto it = inline_traces.rbegin(); it != inline_traces.rend(); ++it ) {
            stack.emplace(*it);
         }
      }
   }
   if( base_action_traces.empty() ) return;

   auto block_time = t->block_time;
   auto block_time_ms = (int64_t)block_time.to_time_point().time_since_epoch().count() / 1000;

   auto& chain = chain_plug->chain();
   chainbase::database& db = const_cast<chainbase::database&>( chain.db() );
   const auto& idx = db.get_index<account_action_trace_shard_multi_index, by_account>();

   for (auto& atrace : base_action_traces)
   {
      chain::base_action_trace &base = atrace.get();
      fc::variant atrace_doc = chain.to_variant_with_abi(base, abi_serializer_max_time_ms);

      std::vector<uint8_t> global_seq_buffer;
      global_seq_buffer.reserve(sizeof(base.receipt.global_sequence));
      for (int i = sizeof(base.receipt.global_sequence) - 1; i >= 0; --i)
      {
         uint8_t byte = (base.receipt.global_sequence >> 8 * i);
         if (global_seq_buffer.empty() && !byte)
         {
            continue;
         }
         if (global_seq_buffer.empty() && byte & 0x80)
         {
            global_seq_buffer.push_back(0x0);
         }
         global_seq_buffer.push_back(byte & 0xFF);
      }

      auto json_atrace = fc::prune_invalid_utf8(fc::json::to_string(atrace_doc));
      cas_client->insertActionTrace(global_seq_buffer, block_time, std::move(json_atrace));
      auto aset = account_set(atrace);
      for (auto a : aset)
      {
         int64_t shardId = 0;
         auto itr = idx.find(a);
         if (itr == idx.end())
         {
            shardId = block_time_ms;
            db.create<account_action_trace_shard_object>([&]( auto& obj ) {
               obj.account = a;
               obj.timestamp = block_time_ms;
               obj.counter = 1;
            });
            cas_client->insertAccountActionTraceShard(std::string(a), shardId);
         }
         else if (itr->counter == 10000)
         {
            db.modify<account_action_trace_shard_object>(*itr, [&](auto& obj) {
               obj.timestamp = block_time_ms;
               obj.counter = 1;
            });
            shardId = itr->timestamp;
            cas_client->insertAccountActionTraceShard(std::string(a), shardId);
         }
         else
         {
            db.modify<account_action_trace_shard_object>(*itr, [&](auto& obj) {
               obj.counter = obj.counter + 1;
            });
            shardId = itr->timestamp;
         }
         cas_client->insertAccountActionTrace(std::string(a), shardId, global_seq_buffer, block_time);
      }
   }

   const auto trx_id = t->id;
   const auto trx_id_str = trx_id.str();
   auto block_num = t->block_num;
   std::vector<uint8_t> buffer;
   buffer.reserve(sizeof(block_num));
   for (int i = sizeof(block_num) - 1; i >= 0; --i)
   {
      uint8_t byte = (block_num >> 8 * i);
      if (buffer.empty() && !byte)
      {
         continue;
      }
      if (buffer.empty() && byte & 0x80)
      {
         buffer.push_back(0x0);
      }
      buffer.push_back(byte & 0xFF);
   }
   fc::variant trx_trace_doc(t);
   auto json_trx_trace = fc::prune_invalid_utf8(fc::json::to_string(trx_trace_doc));

   cas_client->insertTransactionTrace(trx_id_str, buffer, block_time, std::move(json_trx_trace));
}

void cassandra_history_plugin_impl::upsertAccount(
      const chain::action& act,
      const chain::block_timestamp_type& block_time)
{
   if (act.account != chain::config::system_account_name)
      return;
   
   try {
      if( act.name == chain::newaccount::get_name() ) {
         auto newacc = act.data_as<chain::newaccount>();
         cas_client->insertAccount(newacc, block_time);
      }
      else if( act.name == chain::updateauth::get_name() ) {
         const auto update = act.data_as<chain::updateauth>();
         cas_client->updateAccountAuth(update);
      }
      else if( act.name == chain::deleteauth::get_name() ) {
         const auto del = act.data_as<chain::deleteauth>();
         cas_client->deleteAccountAuth(del);
      }
      else if( act.name == chain::setabi::get_name() ) {
         auto setabi = act.data_as<chain::setabi>();
         cas_client->updateAccountAbi(setabi);
      }
   } catch( fc::exception& e ) {
      // if unable to unpack native type, skip account creation
   }
}


cassandra_history_plugin::cassandra_history_plugin():my(new cassandra_history_plugin_impl()){}
cassandra_history_plugin::~cassandra_history_plugin(){}

void cassandra_history_plugin::set_program_options(options_description&, options_description& cfg) {
   cfg.add_options()
         ("cassandra-url", bpo::value<std::string>(),
          "cassandra URL connection string If not specified then plugin is disabled.")
         ("cassandra-wipe", bpo::bool_switch()->default_value(false),
         "Only used with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks to wipe cassandra db.")
         ("cassandra-filter-on", bpo::value<vector<string>>()->composing(),
          "Track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to include all. i.e. eosio:: or :transfer:  Use * or leave unspecified to include all.")
         ("cassandra-filter-out", bpo::value<vector<string>>()->composing(),
          "Do not track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to exclude all.")
         ;
}

void cassandra_history_plugin::plugin_initialize(const variables_map& options) {
   try {
      if( options.count( "cassandra-url" )) {
         ilog( "initializing cassandra_history_plugin" );

         if( options.count( "abi-serializer-max-time-ms" )) {
            uint32_t max_time = options.at( "abi-serializer-max-time-ms" ).as<uint32_t>();
            EOS_ASSERT(max_time > chain::config::default_abi_serializer_max_time_ms,
                       chain::plugin_config_exception, "--abi-serializer-max-time-ms required as default value not appropriate for parsing full blocks");
            fc::microseconds abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();
            my->abi_serializer_max_time_ms = abi_serializer_max_time;
         }

         if( options.count( "cassandra-filter-on" )) {
            auto fo = options.at( "cassandra-filter-on" ).as<vector<string>>();
            my->filter_on_star = false;
            for( auto& s : fo ) {
               if( s == "*" ) {
                  my->filter_on_star = true;
                  break;
               }
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --cassandra-filter-on", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_on.insert( fe );
            }
         } else {
            my->filter_on_star = true;
         }
         if( options.count( "cassandra-filter-out" )) {
            auto fo = options.at( "cassandra-filter-out" ).as<vector<string>>();
            for( auto& s : fo ) {
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               std::cout << v[0] << v[1] << v[2] << std::endl;
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --cassandra-filter-out", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_out.insert( fe );
            }
         }
         
         std::string url_str = options.at( "cassandra-url" ).as<std::string>();
         my->cas_client.reset( new CassandraClient(url_str) );

         if(options.at( "replay-blockchain" ).as<bool>() ||
            options.at( "hard-replay-blockchain" ).as<bool>() ||
            options.at( "delete-all-blocks" ).as<bool>()) {
            if( options.at( "cassandra-wipe" ).as<bool>()) {
               ilog( "Wiping cassandra on startup" );
               my->cas_client->truncateTables();
            }
         }

         my->chain_plug = app().find_plugin<chain_plugin>();
         EOS_ASSERT(my->chain_plug, chain::missing_chain_plugin_exception, "");
         auto& chain = my->chain_plug->chain();
         my->chain_id.emplace( chain.get_chain_id());

         chainbase::database& db = const_cast<chainbase::database&>( chain.db() );
         db.add_index<account_action_trace_shard_multi_index>();
         
         my->accepted_block_connection.emplace(
            chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
               my->accepted_block( bs );
         } ));
         my->irreversible_block_connection.emplace(
            chain.irreversible_block.connect( [&]( const chain::block_state_ptr& bs ) {
               //my->applied_irreversible_block( bs );
            } ));
         my->accepted_transaction_connection.emplace(
            chain.accepted_transaction.connect( [&]( const chain::transaction_metadata_ptr& t ) {
               my->accepted_transaction( t );
            } ));
         my->applied_transaction_connection.emplace(
            chain.applied_transaction.connect( [&]( const chain::transaction_trace_ptr& t ) {
               my->applied_transaction( t );
            } ));

         my->init();
      } else {
         wlog( "eosio::cassandra_history_plugin configured, but no --cassandra-url specified." );
         wlog( "cassandra_history_plugin disabled." );
      }
   }
   FC_LOG_AND_RETHROW()
}

void cassandra_history_plugin::plugin_startup() {
   // Make the magic happen
}

void cassandra_history_plugin::plugin_shutdown() {
   my->accepted_block_connection.reset();
   my->irreversible_block_connection.reset();
   my->accepted_transaction_connection.reset();
   my->applied_transaction_connection.reset();

   my.reset();
}

}
