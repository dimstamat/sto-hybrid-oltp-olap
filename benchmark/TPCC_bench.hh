#pragma once

#include <iostream>
#include <iomanip>
#include <random>
#include <string>
#include <thread>

#include <vector>
#include <math.h>

#include <mutex>


#include <map>
#include <atomic>

#include "../util/measure_latencies.hh"

#define THREADS_MAX 80 // the total number of threads on our machine

// 1 for TPCH on same node (MVCC)
// 2 for TPCH on a replica on separate NUMA node (replicated scheme)
#define RUN_TPCH 2
#define TPCH_SINGLE_NODE (RUN_TPCH == 1)
#define TPCH_REPLICA (RUN_TPCH == 2)
#define RUN_TPCC 1
#define TEST_HASHTABLE 0

#define MEASURE_DB_SZ 1

#if MEASURE_LATENCIES && TEST_HASHTABLE
static double latencies_hashtable_lookup[1][2];
#endif

#if TPCH_SINGLE_NODE || TPCH_REPLICA
constexpr unsigned tpch_thrs_size = 10; // the number of TPCH runner threads
#endif


#define MEASURE_QUERY_LATENCY 1

#if MEASURE_QUERY_LATENCY
static uint64_t q_latencies[tpch_thrs_size][2];
static uint64_t drain_latencies[tpch_thrs_size][2];
#endif

#if TPCH_SINGLE_NODE
#define LOGGING 0

#if TPCH_SINGLE_NODE && MEASURE_LATENCIES
static uint64_t latencies_sec_ord_scan[tpch_thrs_size][2];
static uint64_t latencies_orderline_scan[tpch_thrs_size][2];
#endif


#elif TPCH_REPLICA
// Dimos - enable logging
// 0: no logging
// 1: default log - one log per thread
// 2: one log per thread per table
// 3: map for log (std::unordered_map, or STO Uindex (hashtable))
#define LOGGING 2
// 1: log orders/orderlines tables
// 2: log orders/orderlines/customer/neworders tables
#define LOG_LEVEL 1
// number of DB tables - partition the log
#define LOG_NTABLES 9
// how to store records in the log
// 1 for default (storing the actual key/vals: memcpy(buf, struct, sizeof(struct)))
// 2 for converting integers to strings with sprintf (calling key.to_str(), val.to_str())
// 3 for converting integers to strings (digit by digit) and storing to log one by one: buf[pos++] = digit
#define LOG_RECS 1
#define LOGREC_OVERWRITE 0
#else
#define LOGGING 0
#endif

#define REPLAY_LOG 1
#define LOG_STATS 0 // display stats for the log by adding log records in a map <key, count>

#define VERIFY_OLAP_DB 0 // verify whether OLTP and OLAP DBs are identical.

#define LOG_DRAIN_REQUEST 1 // the log drainer threads will set a boolean flag in the log they request to drain

// 1: change epoch every X ms
// 2: change epoch every X committed transactions in TPC-C
#define CHANGE_EPOCH 2

#define MEASURE_TPCH_PER_EPOCH 1

#define NLOGGERS 10
#define MAX_TPCC_THREADS 10
#define MAX_TPCH_THREADS 10
// 1: std::unordered_map at OLTP side
// 2: STO uindex at OLTP side
// 3: std::unordered_map at OLAP side
// 4: STO uindex at OLAP side
#define DICTIONARY 0
#define LARGE_DUMMY_COLS 2
// the initial size that we allocate for the dictionary (number of buckets in the map)
#define INIT_DICT_SZ 2000000
// the maximum size for the dictionary (the size of the static array)
#define MAX_DICT_SZ 4000000
// when to add to log
// 1 for cleanup, 2 for install
#define ADD_TO_LOG (LOGGING>0? 2 : 0)

#include "barrier.hh" // pthread_barrier_t

#include "compiler.hh"
#include "clp.h"
#include "SystemProfiler.hh"
#include "DB_structs.hh"
#include "TPCC_structs.hh"
#include "TPCC_commutators.hh"

#if TABLE_FINE_GRAINED
#include "TPCC_selectors.hh"
#endif

#include "DB_index.hh"
#include "DB_params.hh"
#include "DB_profiler.hh"
#include "PlatformFeatures.hh"

#include "log.hh"

#define A_GEN_CUSTOMER_ID           1023
#define A_GEN_ITEM_ID               8191

#define C_GEN_CUSTOMER_ID           259
#define C_GEN_ITEM_ID               7911

using namespace std;

namespace tpcc {
    enum class db_type : int {
        simple = 1, // used by tpcc only
        hybrid = 2  // used by tpcc and tpch
    };
};

#include "TPCH_runner.hh"

extern kvepoch_t global_log_epoch;

static bool start_logdrain; // we need to make sure no log drain threads attempt to perform a log drain if the rest threads don't! Otherwise they will get stuck in the barrier. That's why we let only the designated thread to decide!
static bool tpch_done;

using tpch::tpch_db;


// #include "TPCH_queries.hh" at the bottom of the file so that to see the types of the structs used!!
// the other solution is to include TPCH_queries.hh in all tpcc_d.cc, tpcc_dc.cc, etc! This one looks nicer :)

// @section: clp parser definitions
enum {
    opt_dbid = 1, opt_nwhs, opt_nthrs, opt_db_numa, opt_run_numa, opt_txns_per_epoch, opt_sibling, opt_time, opt_perf, opt_pfcnt, opt_gc,
    opt_gr, opt_node, opt_comm, opt_verb, opt_mix
};

extern const char* workload_mix_names[];
extern const Clp_Option options[];
extern const size_t noptions;
extern void print_usage(const char *);
// @endsection: clp parser definitions

extern int tpcc_d(int, char const* const*);
extern int tpcc_dc(int, char const* const*);
extern int tpcc_dn(int, char const* const*);
extern int tpcc_dcn(int, char const* const*);

extern int tpcc_o(int, char const* const*);
extern int tpcc_oc(int, char const* const*);

extern int tpcc_m(int, char const* const*);
extern int tpcc_mc(int, char const* const*);
extern int tpcc_mn(int, char const* const*);
extern int tpcc_mcn(int, char const* const*);

extern int tpcc_s(int, char const* const*);

extern int tpcc_t(int, char const* const*);
extern int tpcc_tc(int, char const* const*);
extern int tpcc_tn(int, char const* const*);
extern int tpcc_tcn(int, char const* const*);

// required for NUMA-aware placement
extern TopologyInfo topo_info;

namespace tpcc {

#if TPCH_REPLICA && LOG_DRAIN_REQUEST
static pthread_barrier_t dbsync_barrier; // used to make sure all TPCH threads stop executing OLAP queries and perform a log drain!
static logmem_replay* logreplays[tpch_thrs_size][LOG_NTABLES];
#endif

static std::mutex print_mutex;

using namespace db_params;
using bench::db_profiler;

class tpcc_input_generator {
public:
    static const char * last_names[];

    tpcc_input_generator(int id, int num_whs)
            : gen(id), num_whs_(uint64_t(num_whs)) {}
    explicit tpcc_input_generator(int num_whs)
            : gen(0), num_whs_(uint64_t(num_whs)) {}

    uint64_t nurand(uint64_t a, uint64_t c, uint64_t x, uint64_t y) {
        uint64_t r1 = (random(0, a) | random(x, y)) + c;
        return (r1 % (y - x + 1)) + x;
    }
    uint64_t random(uint64_t x, uint64_t y) {
        std::uniform_int_distribution<uint64_t> dist(x, y);
        return dist(gen);
    }
    uint64_t num_warehouses() const {
        return num_whs_;
    }

    uint64_t gen_warehouse_id() {
        return random(1, num_whs_);
    }
    uint64_t gen_customer_id() {
        return nurand(A_GEN_CUSTOMER_ID, C_GEN_CUSTOMER_ID, 1, NUM_CUSTOMERS_PER_DISTRICT);
    }

    int gen_customer_last_name_num(bool run) {
        return (int)nurand(255,
                           run ? 223 : 157/* XXX 4.3.2.3 magic C number */,
                           0, 999);
    }

    std::string to_last_name(int gen_n) {
        assert(gen_n <= 999 && gen_n >= 0);
        std::string str;

        int q = gen_n / 100;
        int r = gen_n % 100;
        assert(q < 10);
        str += std::string(last_names[q]);

        q = r / 10;
        r = r % 10;
        assert(r < 10 && q < 10);
        str += std::string(last_names[q]) + std::string(last_names[r]);

        return str;
    }

    std::string gen_customer_last_name_run() {
        return to_last_name(gen_customer_last_name_num(true));
    }

    uint64_t gen_item_id() {
        return nurand(A_GEN_ITEM_ID, C_GEN_ITEM_ID, 1, NUM_ITEMS);
    }
    uint32_t gen_date() {
        return (uint32_t)random(min_date, max_date);
    }
    /* Generate a date between start and end */
    uint32_t gen_date(uint32_t start, uint32_t end){
        return (uint32_t)random(start, end);
    }
    /* Returns the number of distinct dates */
    uint32_t get_total_dates(){
        return max_date - min_date +1;
    }
    uint32_t get_min_date(){
        return min_date;
    }

    std::mt19937& random_generator() {
        return gen;
    }

private:
    std::mt19937 gen;
    uint64_t num_whs_;
    static constexpr uint32_t min_date = 1505244122;
    static constexpr uint32_t max_date = 1599938522;
};

template <typename DBParams>
class tpcc_access;

#ifndef TPCC_HASH_INDEX
#define TPCC_HASH_INDEX 1
#endif

//TODO: make a tpcc_db_single which will contain secondary indexes required for TPC-H!    
    

template <typename DBParams>
class tpcc_db {
public:
    #if DICTIONARY == 1
    using dict_t = std::unordered_map<std::string, int>;
    static dict_t dict [MAX_TPCC_THREADS];
    //std::string index [MAX_TPCC_THREADS][MAX_DICT_SZ]; // cannot have such large static array! Segfault!
    static std::vector<const char*> index [MAX_TPCC_THREADS];
    //std::vector<int> vec;
    #elif DICTIONARY == 2
    struct StrId {
        enum class NamedColumn : int { 
            id = 0
        };
        StrId(int id) : id_(id) {}
        int id_;
    };
    using dict_t = unordered_index<std::string, StrId, db_params::db_default_params>;
    dict_t dict[MAX_TPCC_THREADS];
    #endif

    #if DICTIONARY > 0
    static uint32_t encode(uint32_t dict_index, std::string& str){
        uint32_t code;
        auto & dct = dict[dict_index];
        #if DICTIONARY == 1
        auto elem = dct.find(str);
        if (elem == dct.end()){ // not found
            assert(dct.size() < MAX_DICT_SZ);
            dct[str] = code = dct.size();
            auto e = dct.find(str);
            index[dict_index].push_back(e->first.c_str());
            //assert(strcmp(index[dict_index].at(code), str.c_str()) == 0 );
        }
        else {
            code = elem->second;
        }
        #elif DICTIONARY == 2
        auto * elem = dct.nontrans_get(str);
        if(!elem){
            dct.nontrans_put(str, typename tpcc_db<DBParams>::StrId(1));
        }
        code = 1; // TODO: fix!
        #endif
        // include the dict_index in the 4 most significant bits.
        assert(dict_index < 16);
        code = (code | (dict_index << 28));
        return code;
    }

    static const char* decode(uint32_t code){
        uint32_t dict_index = (code >> 28);
        assert(dict_index < 16);
        uint32_t indx = (code &  ~(0xF << 28));
        return index[dict_index].at(indx);
    }
    #endif

    template <typename K, typename V>
    using OIndex = typename std::conditional<DBParams::MVCC,
          mvcc_ordered_index<K, V, DBParams>,
          ordered_index<K, V, DBParams>>::type;

    template <typename K, typename V>
    using OIndexLog = typename std::conditional<DBParams::MVCC,
          mvcc_ordered_index<K, V, DBParams>,
          ordered_index<K, V, DBParams, LOGGING>>::type;

#if TPCC_HASH_INDEX
    template <typename K, typename V>
    using UIndex = typename std::conditional<DBParams::MVCC,
          mvcc_unordered_index<K, V, DBParams>,
          unordered_index<K, V, DBParams>>::type;

    template <typename K, typename V>
    using UIndexLog = typename std::conditional<DBParams::MVCC,
          mvcc_unordered_index<K, V, DBParams>,
          unordered_index<K, V, DBParams, LOGGING>>::type;
#else
    template <typename K, typename V>
    using UIndex = OIndex<K, V>;
#endif

//#if RUN_TPCH
// is phantom is true when using MVCC only on Orders table and OCC on other tables. We must use the same among all tables.
//template <typename K, typename V>
//using MVCC_OIndex = mvcc_ordered_index<K, V, db_params::db_mvcc_params>;
//#endif


    // partitioned according to warehouse id
#if TPCC_SPLIT_TABLE
    typedef UIndex<warehouse_key, warehouse_const_value> wc_table_type;
    typedef UIndex<warehouse_key, warehouse_comm_value>  wm_table_type;
    typedef UIndex<district_key, district_const_value>   dc_table_type;
    typedef UIndex<district_key, district_comm_value>    dm_table_type;
    typedef UIndex<customer_key, customer_const_value>   cc_table_type;
    typedef UIndex<customer_key, customer_comm_value>    cm_table_type;
    typedef OIndex<order_key, order_const_value>         oc_table_type;
    typedef OIndex<order_key, order_comm_value>          om_table_type;
    typedef OIndex<orderline_key, orderline_const_value> lc_table_type;
    typedef OIndex<orderline_key, orderline_comm_value>  lm_table_type;
    typedef UIndex<stock_key, stock_const_value>         sc_table_type;
    typedef UIndex<stock_key, stock_comm_value>          sm_table_type;
#else
    typedef UIndex<warehouse_key, warehouse_value>       wh_table_type;
    typedef UIndex<district_key, district_value>         dt_table_type;
    #if LOG_LEVEL == 1
    typedef UIndex<customer_key, customer_value>      cu_table_type;
    #else
    typedef UIndexLog<customer_key, customer_value>      cu_table_type;
    #endif
    typedef OIndexLog<order_key, order_value>            od_table_type; // if LOGGING is false, then the OIndex won't perform any logging!
    typedef OIndexLog<orderline_key, orderline_value>    ol_table_type;
    #if TEST_HASHTABLE
    typedef UIndex<orderline_key, orderline_value_deliv_d> ol_hashtable_type;
    #endif
    typedef UIndex<stock_key, stock_value>               st_table_type;
#endif
    typedef UIndex<customer_idx_key, customer_idx_value> ci_table_type;
    typedef OIndex<order_cidx_key, bench::dummy_row>     oi_table_type;
    #if LOG_LEVEL == 1
    typedef OIndex<order_key, bench::dummy_row>          no_table_type;
    #else
    typedef OIndexLog<order_key, bench::dummy_row>          no_table_type;
    #endif
    typedef UIndex<item_key, item_value>                 it_table_type;
    typedef OIndex<history_key, history_value>           ht_table_type;

    explicit inline tpcc_db(int num_whs);
    explicit inline tpcc_db(int num_whs, db_type type);
    explicit inline tpcc_db(const std::string& db_file_name) = delete;
    inline ~tpcc_db();
    // runner_num: the number of the runner thread: [0-num_runners)
    void thread_init_all(int runner_num);

    int num_warehouses() const {
        return static_cast<int>(num_whs_);
    }
#if TPCC_SPLIT_TABLE
    wc_table_type& tbl_warehouses_const() {
        return tbl_whs_const_;
    }
    wm_table_type& tbl_warehouses_comm() {
        return tbl_whs_comm_;
    }
    dc_table_type& tbl_districts_const(uint64_t w_id) {
        return tbl_dts_const_[w_id - 1];
    }
    dm_table_type& tbl_districts_comm(uint64_t w_id) {
        return tbl_dts_comm_[w_id - 1];
    }
    cc_table_type& tbl_customers_const(uint64_t w_id) {
        return tbl_cus_const_[w_id - 1];
    }
    cm_table_type& tbl_customers_comm(uint64_t w_id) {
        return tbl_cus_comm_[w_id - 1];
    }
    oc_table_type& tbl_orders_const(uint64_t w_id) {
        return tbl_ods_const_[w_id - 1];
    }
    om_table_type& tbl_orders_comm(uint64_t w_id) {
        return tbl_ods_comm_[w_id - 1];
    }
    lc_table_type& tbl_orderlines_const(uint64_t w_id) {
        return tbl_ols_const_[w_id - 1];
    }
    lm_table_type& tbl_orderlines_comm(uint64_t w_id) {
        return tbl_ols_comm_[w_id - 1];
    }
    sc_table_type& tbl_stocks_const(uint64_t w_id) {
        return tbl_sts_const_[w_id - 1];
    }
    sm_table_type& tbl_stocks_comm(uint64_t w_id) {
        return tbl_sts_comm_[w_id - 1];
    }
#else
    wh_table_type& tbl_warehouses() {
        return tbl_whs_;
    }
    dt_table_type& tbl_districts(uint64_t w_id) {
        return tbl_dts_[w_id - 1];
    }
    cu_table_type& tbl_customers(uint64_t w_id) {
        return tbl_cus_[w_id - 1];
    }
    od_table_type& tbl_orders(uint64_t w_id) {
        return tbl_ods_[w_id - 1];
    }
    ol_table_type& tbl_orderlines(uint64_t w_id) {
        return tbl_ols_[w_id - 1];
    }
    #if TEST_HASHTABLE
    ol_hashtable_type& tbl_hash_orderlines(uint64_t w_id) {
        return tbl_hash_ols_[w_id - 1];
    }
    #endif
    st_table_type& tbl_stocks(uint64_t w_id) {
        return tbl_sts_[w_id - 1];
    }
#endif
    ci_table_type& tbl_customer_index(uint64_t w_id) {
        return tbl_cni_[w_id - 1];
    }
    oi_table_type& tbl_order_customer_index(uint64_t w_id) {
        return tbl_oci_[w_id - 1];
    }
    no_table_type& tbl_neworders(uint64_t w_id) {
        return tbl_nos_[w_id - 1];
    }
    it_table_type& tbl_items() {
        return *tbl_its_;
    }
    ht_table_type& tbl_histories(uint64_t w_id) {
        return tbl_hts_[w_id - 1];
    }
    tpcc_oid_generator& oid_generator() {
        return oid_gen_;
    }
    tpcc_delivery_queue& delivery_queue() {
        return dlvy_queue_;
    }

    db_type& get_type(){
        return type_;
    }

    void set_type(db_type t){
        type_ = t;
    }

protected:
    size_t num_whs_;
    it_table_type *tbl_its_;
    db_type type_;

#if TPCC_SPLIT_TABLE
    wc_table_type tbl_whs_const_;
    wm_table_type tbl_whs_comm_;
    std::vector<dc_table_type> tbl_dts_const_;
    std::vector<dm_table_type> tbl_dts_comm_;
    std::vector<cc_table_type> tbl_cus_const_;
    std::vector<cm_table_type> tbl_cus_comm_;
    std::vector<oc_table_type> tbl_ods_const_;
    std::vector<om_table_type> tbl_ods_comm_;
    std::vector<lc_table_type> tbl_ols_const_;
    std::vector<lm_table_type> tbl_ols_comm_;
    std::vector<sc_table_type> tbl_sts_const_;
    std::vector<sm_table_type> tbl_sts_comm_;
#else
    wh_table_type tbl_whs_;
    std::vector<dt_table_type> tbl_dts_;
    std::vector<cu_table_type> tbl_cus_;
    std::vector<od_table_type> tbl_ods_;
    std::vector<ol_table_type> tbl_ols_;
    #if TEST_HASHTABLE
    std::vector<ol_hashtable_type> tbl_hash_ols_;
    #endif
    std::vector<st_table_type> tbl_sts_;
#endif

    std::vector<ci_table_type> tbl_cni_;
    std::vector<oi_table_type> tbl_oci_;
    std::vector<no_table_type> tbl_nos_;
    std::vector<ht_table_type> tbl_hts_;

    tpcc_oid_generator oid_gen_;
    tpcc_delivery_queue dlvy_queue_;

    friend class tpcc_access<DBParams>;
};

#if DICTIONARY == 1
template <typename DBParams>
typename tpcc_db<DBParams>::dict_t tpcc_db<DBParams>::dict [MAX_TPCC_THREADS];
template <typename DBParams>
typename std::vector<const char*> tpcc_db<DBParams>::index [MAX_TPCC_THREADS];
#endif



// That's the DB used both by TPC-C and TPC-H when using TPCH_SINGLE approach (same NUMA node).
template <typename DBParams>
class hybrid_db : public tpcc_db<DBParams> {
    template <typename K, typename V>
    using OIndex = typename std::conditional<DBParams::MVCC,
          mvcc_ordered_index<K, V, DBParams>,
          ordered_index<K, V, DBParams>>::type;
          
    // secondary index for <o_entry_d, order_value*>
    typedef OIndex<order_sec_key, order_sec_value>       od_sec_entry_d_type;
    typedef OIndex<orderline_sec_key, orderline_sec_value> ol_sec_deliv_d_type;

    od_sec_entry_d_type tbl_sec_ods_;
    ol_sec_deliv_d_type tbl_sec_ols_;

    public:
    od_sec_entry_d_type& tbl_sec_orders(){
        return tbl_sec_ods_;
    }
    ol_sec_deliv_d_type& tbl_sec_orderlines(){
        return tbl_sec_ols_;
    }

    hybrid_db(int num_whs);

    void thread_init_tpch();

    void thread_init_all_plus_sec(int runner_num);
};

template <typename DBParams>
hybrid_db<DBParams>::hybrid_db(int num_whs)
    : tpcc_db<DBParams>(num_whs),
    tbl_sec_ods_(num_whs * 999983/*num_customers * 10 * 2*/),
    tbl_sec_ols_(num_whs * 999983/*num_customers * 100 * 2*/)
    {
        tpcc_db<DBParams>::set_type(db_type::hybrid);
    }

template <typename DBParams>
void hybrid_db<DBParams>::thread_init_all_plus_sec(int runner_num) {
    tpcc_db<DBParams>::thread_init_all(runner_num);
    tbl_sec_ods_.thread_init();
    tbl_sec_ols_.thread_init();
}

template <typename DBParams>
void hybrid_db<DBParams>::thread_init_tpch(){ // call thread init only for the tables required for TPCH
    for (auto& t : tpcc_db<DBParams>::tbl_ods_)
        t.thread_init(); // why do we need logging for TPC-H?? We don't :)
    tbl_sec_ods_.thread_init();
    tbl_sec_ols_.thread_init();
    for (auto& t : tpcc_db<DBParams>::tbl_ols_)
        t.thread_init();
}

template <typename DBParams>
class tpcc_runner {
public:
    static constexpr bool Commute = DBParams::Commute;
    enum class txn_type : int {
        new_order = 1,
        payment,
        order_status,
        delivery,
        stock_level
    };

    static constexpr bool hybrid = TPCH_SINGLE_NODE;

    tpcc_runner(int id, tpcc_db<DBParams>& database, uint64_t w_start, uint64_t w_end, uint64_t w_own, int mix)
        : ig(id, database.num_warehouses()), db(database), mix(mix), runner_id(id),
          w_id_start(w_start), w_id_end(w_end), w_id_owned(w_own) {}

    inline txn_type next_transaction() {
        uint64_t x = ig.random(1, 100);
        if (mix == 0) {
            if (x <= 45)
                return txn_type::new_order;
            else if (x <= 88)
                return txn_type::payment;
            else if (x <= 92)
                return txn_type::order_status;
            else if (x <= 96)
                return txn_type::delivery;
            else
                return txn_type::stock_level;
        } else if (mix == 1) {
            return txn_type::new_order;
        } else if (mix == 2){
            if (x <= 51)
                return txn_type::new_order;
            else
                return txn_type::payment;
        }
        assert(mix == 3);
        if(x <= 50)
            return txn_type::order_status;
        else
            return txn_type::stock_level;
    }

    inline void run_txn_neworder();
    inline void run_txn_payment();
    inline void run_txn_orderstatus();
    inline void run_txn_delivery(uint64_t wid,
        std::array<uint64_t, NUM_DISTRICTS_PER_WAREHOUSE>& last_delivered);
    inline void run_txn_stocklevel();

    inline uint64_t owned_warehouse() const {
        return w_id_owned;
    }

private:
    tpcc_input_generator ig;
    tpcc_db<DBParams>& db;
    int mix;
    int runner_id;
    uint64_t w_id_start;
    uint64_t w_id_end;
    uint64_t w_id_owned;

    friend class tpcc_access<DBParams>;
};

template <typename DBParams>
class tpcc_prepopulator {
public:
    static pthread_barrier_t sync_barrier;

    tpcc_prepopulator(int id, int w_id, int filler_id, tpcc_db<DBParams>& database)
        : ig(id, database.num_warehouses()), db(database), worker_id(id), warehouse_id(w_id), filler_thread(filler_id) {}

    inline void fill_items(uint64_t iid_begin, uint64_t iid_xend);
    inline void fill_warehouses();
    inline void expand_warehouse(uint64_t wid);
    inline void expand_districts(uint64_t wid);
    inline void expand_customers(uint64_t wid);

    inline void run();

private:
    inline std::string random_a_string(int x, int y);
    inline std::string random_n_string(int x, int y);
    inline std::string random_state_name();
    inline std::string random_zip_code();
    inline void random_shuffle(std::vector<uint64_t>& v);

    tpcc_input_generator ig;
    tpcc_db<DBParams>& db;
    // if we choose to use NUMA-aware placement of DB, the worker_id (==thread id+1) must be different than the warehouse_id.
    int worker_id;    // This will be one of the thread in the selected NUMA node (db_numa_node)
    int warehouse_id; // This will be the warehouse id to expand, like before
    int filler_thread; // This is the thread that will create the warehouse table and it must live in the specified NUMA node (1 for default / NUMA-unaware)
};

template <typename DBParams>
pthread_barrier_t tpcc_prepopulator<DBParams>::sync_barrier;

template <typename DBParams>
tpcc_db<DBParams>::tpcc_db(int num_whs)
    : num_whs_(num_whs),
    type_(db_type::simple),
#if TPCC_SPLIT_TABLE
      tbl_whs_const_(256),
      tbl_whs_comm_(256),
#else
      tbl_whs_(256),
#endif
      oid_gen_()
       {
        // dictionary
        #if DICTIONARY == 1
        for(int i=0; i<MAX_TPCC_THREADS; i++){
            dict[i] = std::unordered_map<std::string, int> (INIT_DICT_SZ);
            index[i] = std::vector<const char*>();
            //index[i] = std::vector<const char*>(INIT_DICT_SZ);
            // use .reserve for the vector!! If we pass the number in the constructor, then it's size() will be equal to that number! (And push_back() will insert right after that number!!)
        }
        #elif DICTIONARY == 2
        for(int i=0; i<MAX_TPCC_THREADS; i++)
            dict[i] = dict_t (INIT_DICT_SZ);
        #endif
    //constexpr size_t num_districts = NUM_DISTRICTS_PER_WAREHOUSE;
    //constexpr size_t num_customers = NUM_CUSTOMERS_PER_DISTRICT * NUM_DISTRICTS_PER_WAREHOUSE;

    tbl_its_ = new it_table_type(999983/*NUM_ITEMS * 2*/);
    for (auto i = 0; i < num_whs; ++i) {
#if TPCC_SPLIT_TABLE
        tbl_dts_const_.emplace_back(32/*num_districts * 2*/);
        tbl_dts_comm_.emplace_back(32);
        tbl_cus_const_.emplace_back(999983/*num_customers * 2*/);
        tbl_cus_comm_.emplace_back(999983);
        tbl_ods_const_.emplace_back(999983/*num_customers * 10 * 2*/);
        tbl_ods_comm_.emplace_back(999983);
        tbl_ols_const_.emplace_back(999983/*num_customers * 100 * 2*/);
        tbl_ols_comm_.emplace_back(999983);
        tbl_sts_const_.emplace_back(999983/*NUM_ITEMS * 2*/);
        tbl_sts_comm_.emplace_back(999983/*NUM_ITEMS * 2*/);
#else
        tbl_dts_.emplace_back(32/*num_districts * 2*/);
        #if LOG_LEVEL == 2
        tbl_cus_.emplace_back(999983/*num_customers * 2*/, 2);
        #else
        tbl_cus_.emplace_back(999983/*num_customers * 2*/);
        #endif
        tbl_ods_.emplace_back(999983/*num_customers * 10 * 2*/, 3); // enable logging for orders table. Its index in the log is 3
        tbl_ols_.emplace_back(999983/*num_customers * 100 * 2*/, 4); // enable logging for orderlines table. Its index in the log is 4
        #if TEST_HASHTABLE
        tbl_hash_ols_.emplace_back(999983/*num_customers * 100 * 2*/);
        #endif
        tbl_sts_.emplace_back(999983/*NUM_ITEMS * 2*/);
#endif
        tbl_cni_.emplace_back(999983/*num_customers * 2*/);
        tbl_oci_.emplace_back(999983/*num_customers * 2*/);
        #if LOG_LEVEL == 2
        tbl_nos_.emplace_back(999983/*num_customers * 10 * 2*/, 6);
        #else
        tbl_nos_.emplace_back(999983/*num_customers * 10 * 2*/);
        #endif
        tbl_hts_.emplace_back(999983/*num_customers * 2*/);
    }
}

template <typename DBParams>
tpcc_db<DBParams>::~tpcc_db() {
    delete tbl_its_;
}

template <typename DBParams>
void tpcc_db<DBParams>::thread_init_all(int runner_num) {
    tbl_its_->thread_init();
#if TPCC_SPLIT_TABLE
    tbl_whs_const_.thread_init();
    tbl_whs_comm_.thread_init();
    for (auto& t : tbl_dts_const_)
        t.thread_init();
    for (auto& t : tbl_dts_comm_)
        t.thread_init();
    for (auto& t : tbl_cus_const_)
        t.thread_init();
    for (auto& t : tbl_cus_comm_)
        t.thread_init();
    for (auto& t : tbl_ods_const_)
        t.thread_init();
    for (auto& t : tbl_ods_comm_)
        t.thread_init();
    for (auto& t : tbl_ols_const_)
        t.thread_init();
    for (auto& t : tbl_ols_comm_)
        t.thread_init();
    for (auto& t : tbl_sts_const_)
        t.thread_init();
    for (auto& t : tbl_sts_comm_)
        t.thread_init();
#else
    tbl_whs_.thread_init();
    for (auto& t : tbl_dts_)
        t.thread_init();
    for (auto& t : tbl_cus_)
    #if LOG_LEVEL == 2
        t.thread_init(runner_num);
    #else
        t.thread_init();
    #endif
    for (auto& t : tbl_ods_)
        t.thread_init(runner_num); // enable logging (as a template parameter earlier) and set runner_num
    for (auto& t : tbl_ols_)
        t.thread_init(runner_num); // enable logging (as a template parameter earlier) and set runner_num
    for (auto& t : tbl_sts_)
        t.thread_init();
#endif
    for (auto& t : tbl_cni_)
        t.thread_init();
    for (auto& t : tbl_oci_)
        t.thread_init();
    for (auto& t : tbl_nos_)
    #if LOG_LEVEL == 2
        t.thread_init(runner_num);
    #else
        t.thread_init();
    #endif
    for (auto& t : tbl_hts_)
        t.thread_init();
}

// @section: db prepopulation functions
template<typename DBParams>
void tpcc_prepopulator<DBParams>::fill_items(uint64_t iid_begin, uint64_t iid_xend) {
    for (auto iid = iid_begin; iid < iid_xend; ++iid) {
        item_key ik(iid);
        item_value iv;

        iv.i_im_id = ig.random(1, 10000);
        iv.i_price = ig.random(100, 10000);
        iv.i_name = random_a_string(14, 24);
        iv.i_data = random_a_string(26, 50);
        if (ig.random(1, 100) <= 10) {
            auto pos = ig.random(0, iv.i_data.length() - 8);
            auto placed = iv.i_data.place("ORIGINAL", pos);
            assert(placed);
            (void)placed;
        }

        db.tbl_items().nontrans_put(ik, iv);
    }
}

template<typename DBParams>
void tpcc_prepopulator<DBParams>::fill_warehouses() {
    for (uint64_t wid = 1; wid <= ig.num_warehouses(); ++wid) {
        warehouse_key wk(wid);
#if TPCC_SPLIT_TABLE
        warehouse_const_value wcv {};
        warehouse_comm_value wmv {};
        wcv.w_name = random_a_string(6, 10);
        wcv.w_street_1 = random_a_string(10, 20);
        wcv.w_street_2 = random_a_string(10, 20);
        wcv.w_city = random_a_string(10, 20);
        wcv.w_state = random_state_name();
        wcv.w_zip = random_zip_code();
        wcv.w_tax = ig.random(0, 2000);
        wmv.w_ytd = 30000000;

        db.tbl_warehouses_const().nontrans_put(wk, wcv);
        db.tbl_warehouses_comm().nontrans_put(wk, wmv);
#else
        warehouse_value wv {};
        wv.w_name = random_a_string(6, 10);
        wv.w_street_1 = random_a_string(10, 20);
        wv.w_street_2 = random_a_string(10, 20);
        wv.w_city = random_a_string(10, 20);
        wv.w_state = random_state_name();
        wv.w_zip = random_zip_code();
        wv.w_tax = ig.random(0, 2000);
        wv.w_ytd = 30000000;

        db.tbl_warehouses().nontrans_put(wk, wv);
#endif
    }
}

template<typename DBParams>
void tpcc_prepopulator<DBParams>::expand_warehouse(uint64_t wid) {
    for (uint64_t iid = 1; iid <= NUM_ITEMS; ++iid) {
        stock_key sk(wid, iid);
#if TPCC_SPLIT_TABLE
        stock_const_value scv {};
        stock_comm_value smv {};

        smv.s_quantity = ig.random(10, 100);
        for (auto i = 0; i < NUM_DISTRICTS_PER_WAREHOUSE; ++i)
            scv.s_dists[i] = random_a_string(24, 24);
        smv.s_ytd = 0;
        smv.s_order_cnt = 0;
        smv.s_remote_cnt = 0;
        scv.s_data = random_a_string(26, 50);
        if (ig.random(1, 100) <= 10) {
            auto pos = ig.random(0, scv.s_data.length() - 8);
            auto placed = scv.s_data.place("ORIGINAL", pos);
            assert(placed);
            (void)placed;
        }

        db.tbl_stocks_const(wid).nontrans_put(sk, scv);
        db.tbl_stocks_comm(wid).nontrans_put(sk, smv);
#else
        stock_value sv;

        sv.s_quantity = ig.random(10, 100);
        for (auto i = 0; i < NUM_DISTRICTS_PER_WAREHOUSE; ++i){
            #if DICTIONARY == 1 || DICTIONARY == 2
            std::string s(random_a_string(24, 24));
            sv.s_dists[i] = db.encode(wid-1, s);
            #else
            sv.s_dists[i] = random_a_string(24, 24);
            #endif
        }
        sv.s_ytd = 0;
        sv.s_order_cnt = 0;
        sv.s_remote_cnt = 0;
        sv.s_data = random_a_string(26, 50);
        if (ig.random(1, 100) <= 10) {
            auto pos = ig.random(0, sv.s_data.length() - 8);
            auto placed = sv.s_data.place("ORIGINAL", pos);
            assert(placed);
            (void)placed;
        }

        db.tbl_stocks(wid).nontrans_put(sk, sv);
#endif
    }

    for (uint64_t did = 1; did <= NUM_DISTRICTS_PER_WAREHOUSE; ++did) {
        district_key dk(wid, did);
#if TPCC_SPLIT_TABLE
        district_const_value dcv;
        district_comm_value dmv;

        dcv.d_name = random_a_string(6, 10);
        dcv.d_street_1 = random_a_string(10, 20);
        dcv.d_street_2 = random_a_string(10, 20);
        dcv.d_city = random_a_string(10, 20);
        dcv.d_state = random_state_name();
        dcv.d_zip = random_zip_code();
        dcv.d_tax = ig.random(0, 2000);
        dmv.d_ytd = 3000000;
        //dv.d_next_o_id = 3001;

        db.tbl_districts_const(wid).nontrans_put(dk, dcv);
        db.tbl_districts_comm(wid).nontrans_put(dk, dmv);
#else
        district_value dv;

        dv.d_name = random_a_string(6, 10);
        dv.d_street_1 = random_a_string(10, 20);
        dv.d_street_2 = random_a_string(10, 20);
        dv.d_city = random_a_string(10, 20);
        dv.d_state = random_state_name();
        dv.d_zip = random_zip_code();
        dv.d_tax = ig.random(0, 2000);
        dv.d_ytd = 3000000;
        //dv.d_next_o_id = 3001;

        db.tbl_districts(wid).nontrans_put(dk, dv);
#endif

    }
}

template<typename DBParams>
void tpcc_prepopulator<DBParams>::expand_districts(uint64_t wid) {
    for (uint64_t did = 1; did <= NUM_DISTRICTS_PER_WAREHOUSE; ++did) {
        for (uint64_t cid = 1; cid <= NUM_CUSTOMERS_PER_DISTRICT; ++cid) {
            int last_name_num = (cid <= 1000) ? int(cid - 1)
                                              : ig.gen_customer_last_name_num(false/*run time*/);
            customer_key ck(wid, did, cid);
#if TPCC_SPLIT_TABLE
            customer_const_value ccv;
            customer_comm_value cmv;

            ccv.c_last = ig.to_last_name(last_name_num);
            ccv.c_middle = "OE";
            ccv.c_first = random_a_string(8, 16);
            ccv.c_street_1 = random_a_string(10, 20);
            ccv.c_street_2 = random_a_string(10, 20);
            ccv.c_city = random_a_string(10, 20);
            ccv.c_zip = random_zip_code();
            ccv.c_phone = random_n_string(16, 16);
            ccv.c_since = ig.gen_date();
            ccv.c_credit = (ig.random(1, 100) <= 10) ? "BC" : "GC";
            ccv.c_credit_lim = 5000000;
            ccv.c_discount = ig.random(0, 5000);
            cmv.c_balance = -1000;
            cmv.c_ytd_payment = 1000;
            cmv.c_payment_cnt = 1;
            cmv.c_delivery_cnt = 0;
            cmv.c_data = random_a_string(300, 500);

            db.tbl_customers_const(wid).nontrans_put(ck, ccv);
            db.tbl_customers_comm(wid).nontrans_put(ck, cmv);

            customer_idx_key cik(wid, did, ccv.c_last);
#else
            customer_value cv;

            cv.c_last = ig.to_last_name(last_name_num);
            cv.c_middle = "OE";
            cv.c_first = random_a_string(8, 16);
            cv.c_street_1 = random_a_string(10, 20);
            cv.c_street_2 = random_a_string(10, 20);
            cv.c_city = random_a_string(10, 20);
            cv.c_zip = random_zip_code();
            cv.c_phone = random_n_string(16, 16);
            cv.c_since = ig.gen_date();
            cv.c_credit = (ig.random(1, 100) <= 10) ? "BC" : "GC";
            cv.c_credit_lim = 5000000;
            cv.c_discount = ig.random(0, 5000);
            cv.c_balance = -1000;
            cv.c_ytd_payment = 1000;
            cv.c_payment_cnt = 1;
            cv.c_delivery_cnt = 0;
            cv.c_data = random_a_string(300, 500);

            db.tbl_customers(wid).nontrans_put(ck, cv);

            customer_idx_key cik(wid, did, cv.c_last);
#endif
            auto civ = db.tbl_customer_index(wid).nontrans_get(cik);
            if (civ == nullptr) {
                db.tbl_customer_index(wid).nontrans_put(cik, customer_idx_value());
                civ = db.tbl_customer_index(wid).nontrans_get(cik);
                assert(civ);
            }
            civ->c_ids.push_front(cid);
        }
    }
}

template<typename DBParams>
void tpcc_prepopulator<DBParams>::expand_customers(uint64_t wid) {
    for (uint64_t did = 1; did <= NUM_DISTRICTS_PER_WAREHOUSE; ++did) {
        for (uint64_t cid = 1; cid <= NUM_CUSTOMERS_PER_DISTRICT; ++cid) {
            history_value hv;

            hv.h_c_id = cid;
            hv.h_c_d_id = did;
            hv.h_c_w_id = wid;
            hv.h_date = ig.gen_date();
            hv.h_amount = 1000;
            hv.h_data = random_a_string(12, 24);

#if HISTORY_SEQ_INSERT
            history_key hk(db.tbl_histories(wid).gen_key());
#else
            history_key hk(wid, did, cid, db.tbl_histories(wid).gen_key());
#endif
            db.tbl_histories(wid).nontrans_put(hk, hv);
        }
    }

    hybrid_db<DBParams> &db_h = reinterpret_cast<hybrid_db<DBParams>&>(db);
    bool hybrid = false; // hybrid is when we run both TPC-C and TPC-H on the same node
    if(db.get_type() == db_type::hybrid)
        hybrid = true;

    for (uint64_t did = 1; did <= NUM_DISTRICTS_PER_WAREHOUSE; ++did) {
        std::vector<uint64_t> cid_perm;
        for (uint64_t n = 1; n <= NUM_CUSTOMERS_PER_DISTRICT; ++n)
            cid_perm.push_back(n);
        random_shuffle(cid_perm);

        for (uint64_t i = 1; i <= NUM_CUSTOMERS_PER_DISTRICT; ++i) {
            uint64_t oid = i;
            order_key ok(wid, did, oid);
            auto ol_count = (uint32_t) ig.random(5, 15);
            auto entry_date = ig.gen_date();
#if TPCC_SPLIT_TABLE
            order_const_value ocv;
            order_comm_value omv;


            ocv.o_c_id = cid_perm[i - 1];
            omv.o_carrier_id = (oid < 2101) ? ig.random(1, 10) : 0;
            ocv.o_entry_d = entry_date;
            ocv.o_ol_cnt = ol_count;
            ocv.o_all_local = 1;

            order_cidx_key ock(wid, did, ocv.o_c_id, oid);

            db.tbl_orders_const(wid).nontrans_put(ok, ocv);
            db.tbl_orders_comm(wid).nontrans_put(ok, omv);
#else
            order_value ov;

            ov.o_c_id = cid_perm[i - 1];
            ov.o_carrier_id = (oid < 2101) ? ig.random(1, 10) : 0;
            ov.o_entry_d = entry_date;
            ov.o_ol_cnt = ol_count;
            ov.o_all_local = 1;

            order_cidx_key ock(wid, did, ov.o_c_id, oid);

        if(hybrid){
            order_sec_value val;
            auto val_p = db_h.tbl_orders(wid).nontrans_put_el(ok, ov);
            // store the internal_elem
            val.o_c_entry_d_p = val_p;
            // put into the secondary index as well
            db_h.tbl_sec_orders().nontrans_put(order_sec_key(entry_date, wid, did, oid), val);
        }
        else {
            db.tbl_orders(wid).nontrans_put(ok, ov);
        }
#endif
            db.tbl_order_customer_index(wid).nontrans_put(ock, {});

            for (uint64_t on = 1; on <= ol_count; ++on) {
                orderline_key olk(wid, did, oid, on);
#if TPCC_SPLIT_TABLE
                orderline_const_value lcv;
                orderline_comm_value lmv;

                lcv.ol_i_id = ig.random(1, 100000);
                lcv.ol_supply_w_id = wid;
                lmv.ol_delivery_d = (oid < 2101) ? entry_date : 0;
                lcv.ol_quantity = 5;
                lcv.ol_amount = (oid < 2101) ? 0 : (int) ig.random(1, 999999);
                lcv.ol_dist_info = random_a_string(24, 24);

                db.tbl_orderlines_const(wid).nontrans_put(olk, lcv);
                db.tbl_orderlines_comm(wid).nontrans_put(olk, lmv);
#else
                orderline_value olv;

                olv.ol_i_id = ig.random(1, 100000);
                olv.ol_supply_w_id = wid;
                olv.ol_delivery_d = (oid < 2101) ? entry_date : 0;
                olv.ol_quantity = 5;
                olv.ol_amount = (oid < 2101) ? 0 : (int) ig.random(1, 999999);
                #if DICTIONARY == 1 || DICTIONARY == 2
                auto ol_dist_info_str = random_a_string(24, 24);
                std::string s(ol_dist_info_str);
                olv.ol_dist_info = db.encode(wid-1, s);
                #if LARGE_DUMMY_COLS > 0
                auto ol_dummy_str_s = random_a_string(72, 72);
                std::string s1(ol_dummy_str_s);
                olv.ol_dummy_str1 = db.encode(wid-1, s1);
                ol_dummy_str_s = random_a_string(72, 72);
                std::string s2(ol_dummy_str_s);
                olv.ol_dummy_str2 = db.encode(wid-1, s2);
                ol_dummy_str_s = random_a_string(72, 72);
                std::string s3(ol_dummy_str_s);
                olv.ol_dummy_str3 = db.encode(wid-1, s3);
                ol_dummy_str_s = random_a_string(72, 72);
                std::string s4(ol_dummy_str_s);
                olv.ol_dummy_str4 = db.encode(wid-1, s4);
                #endif
                #if LARGE_DUMMY_COLS == 2
                ol_dummy_str_s = random_a_string(72, 72);
                std::string s5(ol_dummy_str_s);
                olv.ol_dummy_str5 = db.encode(wid-1, s5);
                ol_dummy_str_s = random_a_string(72, 72);
                std::string s6(ol_dummy_str_s);
                olv.ol_dummy_str6 = db.encode(wid-1, s6);
                ol_dummy_str_s = random_a_string(72, 72);
                std::string s7(ol_dummy_str_s);
                olv.ol_dummy_str7 = db.encode(wid-1, s7);
                ol_dummy_str_s = random_a_string(72, 72);
                std::string s8(ol_dummy_str_s);
                olv.ol_dummy_str8 = db.encode(wid-1, s8);
                #endif

                #else
                olv.ol_dist_info = random_a_string(24, 24);
                #if LARGE_DUMMY_COLS > 0
                olv.ol_dummy_str1 = random_a_string(72, 72);
                olv.ol_dummy_str2 = random_a_string(72, 72);
                olv.ol_dummy_str3 = random_a_string(72, 72);
                olv.ol_dummy_str4 = random_a_string(72, 72);
                #endif
                #if LARGE_DUMMY_COLS == 2
                olv.ol_dummy_str5 = random_a_string(72, 72);
                olv.ol_dummy_str6 = random_a_string(72, 72);
                olv.ol_dummy_str7 = random_a_string(72, 72);
                olv.ol_dummy_str8 = random_a_string(72, 72);
                #endif
                #endif
            #if 0 // no need for secondary index in orderlines for Q.4!
                auto val_p = db.tbl_orderlines(wid).nontrans_put(olk, olv);
                orderline_sec_value val;
                // store the location of the value
                val.ol_c_delivery_d_p = reinterpret_cast<uint64_t*>(val_p);
                // put into the secondary index as well
                db.tbl_sec_orderlines().nontrans_put(orderline_sec_key(olv.ol_delivery_d, wid, did, oid, on), val);
            #endif
                db.tbl_orderlines(wid).nontrans_put(olk, olv);
                #if TEST_HASHTABLE
                orderline_value_deliv_d olvv;
                olvv.ol_delivery_d = (oid < 2101) ? entry_date : 0;
                db.tbl_hash_orderlines(wid).nontrans_put(olk, olvv);
                #endif
#endif
            }

            if (oid >= 2101) {
                order_key nok(wid, did, oid);
                db.tbl_neworders(wid).nontrans_put(nok, {});
            }
        }
    }
}
// @endsection: db prepopulation functions

template<typename DBParams>
void tpcc_prepopulator<DBParams>::run() {
    int r;

    always_assert(warehouse_id >= 1, "prepopulator warehouse id range error");
    // set affinity so that the warehouse is filled at the corresponding numa node
    //set_affinity(worker_id - 1);
    // Dimos - for NUMA-aware placement we want to have different worker_id than warehouse id! Now we could selectively choose in which NUMA node to place the DB!
    set_affinity(worker_id);

    if (worker_id == filler_thread) {
        fill_items(1, 100001);
        fill_warehouses();
    }

    //std::cout<<"Running prepopulator on CPU "<< worker_id<<std::endl;

    // barrier
    r = pthread_barrier_wait(&sync_barrier);
    always_assert(r == PTHREAD_BARRIER_SERIAL_THREAD || r == 0, "pthread_barrier_wait");

    expand_warehouse((uint64_t) warehouse_id);

    // barrier
    r = pthread_barrier_wait(&sync_barrier);
    always_assert(r == PTHREAD_BARRIER_SERIAL_THREAD || r == 0, "pthread_barrier_wait");

    expand_districts((uint64_t) warehouse_id);

    // barrier
    r = pthread_barrier_wait(&sync_barrier);
    always_assert(r == PTHREAD_BARRIER_SERIAL_THREAD || r == 0, "pthread_barrier_wait");

    expand_customers((uint64_t) warehouse_id);
}

// @section: prepopulation string generators
template<typename DBParams>
std::string tpcc_prepopulator<DBParams>::random_a_string(int x, int y) {
    size_t len = ig.random(x, y);
    std::string str;
    str.reserve(len);

    for (auto i = 0u; i < len; ++i) {
        auto n = ig.random(0, 61);
        char c = (n < 26) ? char('a' + n) :
                 ((n < 52) ? char('A' + (n - 26)) : char('0' + (n - 52)));
        str.push_back(c);
    }
    return str;
}

template<typename DBParams>
std::string tpcc_prepopulator<DBParams>::random_n_string(int x, int y) {
    size_t len = ig.random(x, y);
    std::string str;
    str.reserve(len);

    for (auto i = 0u; i < len; ++i) {
        auto n = ig.random(0, 9);
        str.push_back(char('0' + n));
    }
    return str;
}

template<typename DBParams>
std::string tpcc_prepopulator<DBParams>::random_state_name() {
    std::string str = "AA";
    for (auto &c : str)
        c += ig.random(0, 25);
    return str;
}

template<typename DBParams>
std::string tpcc_prepopulator<DBParams>::random_zip_code() {
    std::stringstream ss;
    ss << std::setfill('0') << std::setw(4) << ig.random(0, 9999);
    return ss.str() + "11111";
}

template<typename DBParams>
void tpcc_prepopulator<DBParams>::random_shuffle(std::vector<uint64_t> &v) {
    std::shuffle(v.begin(), v.end(), ig.random_generator());
}
// @endsection: prepopulation string generators



template <typename DBParams>
class tpcc_access {
// class tpcc_access will also contain the tpch_runner_thread because the tpch runner threads have access to the same tpcc DB.

public:
    static void prepopulation_worker(tpcc_db<DBParams> &db, int worker_id, int worker_num, int warehouse_id, int filler_id) {
        tpcc_prepopulator<DBParams> pop(worker_id, warehouse_id, filler_id, db);
        // Dimos - set affinity on the CPU of the specified NUMA node, in order for the DB to live in that node!
        set_affinity(worker_id);
        // must be worker_num to initialize the corresponding log!
        if(db.get_type() == db_type::hybrid){
            hybrid_db<DBParams>& db_h = reinterpret_cast<hybrid_db<DBParams>&>(db);
            db_h.thread_init_all_plus_sec(worker_num);
        }
        else
            db.thread_init_all(worker_num);
        pop.run();
    }

    static void prepopulate_db(tpcc_db<DBParams> &db, int db_numa_node) {
        int r;
        r = pthread_barrier_init(&tpcc_prepopulator<DBParams>::sync_barrier, nullptr, db.num_warehouses());
        always_assert(r == 0, "pthread_barrier_init failed");

        std::vector<std::thread> prepop_thrs;
        if(db_numa_node == -1){ // normal operation (NUMA unaware): use worker id == warehouse id and filler id == 1
            for (int i = 1; i <= db.num_warehouses(); ++i)
                prepop_thrs.emplace_back(prepopulation_worker, std::ref(db), i-1, /*worker_num*/ (i-1), i, 0);
        }
        else { // NUMA aware: Create DB only in specified NUMA node: worker id != warehouse id
            auto & cpus = topo_info.cpu_id_list[db_numa_node];
            for (int i = 1; i <= db.num_warehouses(); ++i){
                prepop_thrs.emplace_back(prepopulation_worker, std::ref(db), cpus[(i-1) % cpus.size()], /*worker_num*/ (i-1),  i, cpus[0]); // filler will be the first cpu of the specified NUMA node (db_numa_node)
            }
            
        }
        for (auto &t : prepop_thrs)
            t.join();

        r = pthread_barrier_destroy(&tpcc_prepopulator<DBParams>::sync_barrier);
        always_assert(r == 0, "pthread_barrier_destroy failed");
    }

    /**  TPCH  **
     **        **/
    static void tpch_single_runner_thread(hybrid_db<DBParams>& db, db_profiler& prof, int runner_id, int runner_num, double time_limit, uint64_t& q_cnt){
        (void)runner_num;
        INIT_COUNTING
        tpcc::tpcc_input_generator ig(runner_id, db.num_warehouses());
        tpch::tpch_runner<DBParams> runner(runner_id, ig, &db);
        ::TThread::set_id(runner_id);
        set_affinity(runner_id);
        db.thread_init_tpch();

        time_limit-= (time_limit * 0.1) ; // Run for 10% less time than TPC-C (1 second for 10 seconds of TPC-C, which is default for REPL.)
        
        uint64_t tsc_diff = (uint64_t)(time_limit * constants::processor_tsc_frequency * constants::billion);
        auto start_t = prof.start_timestamp();
        while(true){
            // measure latency
            START_COUNTING
            runner.run_next_query();
            STOP_COUNTING(q_latencies, runner_num)
            q_cnt++;
            if ((read_tsc() - start_t) >= tsc_diff) { // done
                break;
            }
        }
    }

    // we need to know the number of queries per epoch for all threads, so that to print throughput per epoch!
    // if CHANGE EPOCH == 2: change epoch every X committed transactions
    static void tpch_replica_runner_thread(void* olap_db, db_profiler& prof, int runner_id, int runner_num, double time_limit, uint64_t& q_cnt, std::vector<uint64_t>& q_cnts_per_epoch, std::vector<uint64_t>& txn_cnts, const uint64_t txns_per_epoch){
        INIT_COUNTING
        (void)runner_num;
        #if CHANGE_EPOCH == 1
        (void)txn_cnts;
        (void)txns_per_epoch;
        #endif
        tpch_db * dbp = reinterpret_cast<tpch_db*>(olap_db);
        tpcc::tpcc_input_generator ig(runner_id, dbp->num_warehouses());
        tpch::tpch_runner<DBParams> runner(runner_id, ig, olap_db);
        ::TThread::set_id(runner_id);
        set_affinity(runner_id);
        dbp->thread_init_all();
        time_limit-=1; // run for half second less than TPC-C
        uint64_t tsc_diff = (uint64_t)(time_limit * constants::processor_tsc_frequency * constants::billion);
        auto start_t = prof.start_timestamp();
        #if TPCH_REPLICA && LOG_DRAIN_REQUEST
        #if CHANGE_EPOCH == 1
        int div = 50; // perform a DB sync every time_limit / div : with 10 secs time_limit and div 50, it will perform DB sync every 200ms
        //int div = 20; //500ms
        //int div = 10; //1000ms
        uint64_t last_drain = start_t;
        #endif
        uint64_t epoch_change_t = start_t;
        #endif

        uint64_t q_cnt_lcl = 0;
        uint64_t q_cnt_per_epoch = 0;

        if(runner_num == 0){
            tpch_done = false;
            start_logdrain = false;
        }

        #if TPCH_REPLICA && LOG_DRAIN_REQUEST
        if(runner_num == 0)
            std::cout<<"Epoch #\tDB size(MB)\tDB #elems\tq latency\tq per epoch\ttime\tdrain latency\tq/s\n";
        #endif
        double q_latency_sum=0; // q latency in ms
        uint64_t q_latency_num=0;
        double drain_latency=0;

        while(true){
            auto curr_t = read_tsc();
            // only the designated thread should decide whether TPC-H is done or not. If not, it could be that some threads wait for the barrier to perform log drain wile other threads decided to be done!
            #if CHANGE_EPOCH == 1
            if (runner_num == 0 && (curr_t - start_t) >= tsc_diff) { // done
                tpch_done = true;
                q_cnt = q_cnt_lcl;
                break;
            }
            #elif CHANGE_EPOCH == 2
            bool wait_for_txns = false;
            //std::vector<uint64_t> max_q_per_epoch ( {200, 70, 50, 35, 25, 20, 18, 18, 10, 10, 10, 10, 8, 8, 8, 8} );
            // for larger DB + decompress/processing
            std::vector<uint64_t> max_q_per_epoch ( {150, 50, 40, 20, 20, 15, 15, 12, 12, 12, 12, 10, 10, 10, 8, 8} );
            if (runner_num == 0 && ( (curr_t - start_t) >= tsc_diff || global_log_epoch.value() > max_q_per_epoch.size() )){
                tpch_done = true;
                q_cnt = q_cnt_lcl;
                break;
            }
            #endif

                
            if(tpch_done){
                q_cnt = q_cnt_lcl;
                break;
            }
            START_COUNTING
            auto q_start_time = read_tsc();
            runner.run_next_query();
            if(runner_num==0){
                q_latency_sum+= (double)(read_tsc()-q_start_time) / constants::million / constants::processor_tsc_frequency; // latency in ms
                q_latency_num++;
            }
            //STOP_COUNTING(q_latencies, runner_num)
            q_cnt_lcl++;
            q_cnt_per_epoch++;
            #if CHANGE_EPOCH == 2
            q_cnts_per_epoch[runner_num] = q_cnt_per_epoch;
            #endif
            #if TPCH_REPLICA && LOG_DRAIN_REQUEST
            curr_t = read_tsc();
            // if timeout reached in designated thread, trigger log replay to refresh OLAP DB!
            // if we are near the end, do not perform log drain!
            // we need to adjust time_limit (-2, or -3) depending on the log drain frequency.
            // it is okay if we run log drans near the end of TPC-H!
            //bool near_the_end = false; //=  ((curr_t - start_t) >=  (uint64_t)(time_limit-1) * constants::processor_tsc_frequency * constants::billion);
            #if CHANGE_EPOCH == 1
            if(runner_num == 0 && div>0 && (curr_t - last_drain) >= tsc_diff/div /*&&  !near_the_end*/){
                start_logdrain = true;
            }
            #elif CHANGE_EPOCH == 2
            if(runner_num == 0){
                uint64_t txns_total = 0;
                for (const auto& txn_cnt : txn_cnts)
                    txns_total+= txn_cnt;
                if (txns_total >= global_log_epoch.value() * txns_per_epoch){
                    start_logdrain = true;
                }
            }
            // run only a fixed number of queries per epoch!
            if (runner_num == 0 && !start_logdrain) { // done
                uint64_t q_cnts_per_epoch_total = 0;
                for(uint64_t q_cnt_per_e : q_cnts_per_epoch)
                    q_cnts_per_epoch_total += q_cnt_per_e;
                if (q_cnts_per_epoch_total >= max_q_per_epoch[global_log_epoch.value()-1]){
                    start_logdrain = true;
                    wait_for_txns = true;
                }
            }
            #endif
            /*
            if(runner_num == 0 && near_the_end){
                tpch_done = true;
                q_cnt = q_cnt_lcl;
                break;
            }*/
            if(start_logdrain) {
                q_cnt = q_cnt_lcl;
                #if CHANGE_EPOCH == 1
                q_cnts_per_epoch[runner_num] = q_cnt_per_epoch;
                #endif
                /* ================= LOG DRAIN PREPARE ============== */
                int r = pthread_barrier_wait(&dbsync_barrier);
                always_assert(r == PTHREAD_BARRIER_SERIAL_THREAD || r == 0, "Error in pthread_barrier_wait");
                kvepoch_t current_epoch = global_log_epoch;
                // get your log
                auto & log = (reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs))->log(runner_num);
                // advance log epoch and sync DB
                if(runner_num == 0){
                    #if CHANGE_EPOCH == 2
                    curr_t = read_tsc();
                    double elapsed_time = (double)(curr_t - epoch_change_t) / constants::million / constants::processor_tsc_frequency; // elapsed time in ms
                    epoch_change_t = read_tsc();
                    if(wait_for_txns){
                        uint64_t txns_total = 0;
                        do { // wait until we reach the desired number of tpc-c transactions and then start draining the log!
                            txns_total = 0;
                            for (const auto& txn_cnt : txn_cnts)
                                txns_total+= txn_cnt;
                        }while(txns_total < global_log_epoch.value() * txns_per_epoch);
                    }
                    #endif
                    // calculate per epoch Query throughput and other stats!
                    uint64_t q_cnts_per_epoch_total = 0;
                    for(uint64_t q_cnt_per_e : q_cnts_per_epoch)
                        q_cnts_per_epoch_total += q_cnt_per_e;
                    std::cout<< current_epoch.value()<<"\t"<< (double)dbp->db_size()/1024/1024 << "\t"<< dbp->db_num_elems() <<"\t"<< (q_latency_sum / q_latency_num) <<"\t"<< q_cnts_per_epoch_total << "\t"<< elapsed_time <<
                     "\t"<< drain_latency <<"\t"<< (double) q_cnts_per_epoch_total / (elapsed_time / 1000.0) <<std::endl; // q/sec
                    start_logdrain = false;
                    usleep(20); // do not change the global epoch right away, since some threads will miss the log writer's signal before they call wait.
                    global_log_epoch++;
                    //std::cout<<"Set log epoch to "<< global_log_epoch.value()<<std::endl;
                }
                //std::cout<<runner_num <<" : wait to be signaled!\n";
                // await on your log!
                log.wait_to_logdrain();
                q_cnts_per_epoch[runner_num] = q_cnt_per_epoch = 0;
                //std::cout<<runner_num <<" : Signaled!\n";
                
                /* ================= LOG DRAIN START ============== */
                // perform log drain!

                auto callBack = [dbp, runner_num](const Str& key, const Str& val, kvtimestamp_t ts, uint32_t cmd, int tbl) -> bool {
                    //return log_op_apply_tpch(*dbp, false, key, val, ts, cmd, tbl); // why add_to_sec was false????
                    return log_op_apply_tpch(*dbp, runner_num, true, key, val, ts, cmd, tbl);
                };
                int recs_replayed = 0;
                //START_COUNTING_PRINT
                auto logdrain_start = read_tsc();
                for (int tbl=0; tbl<LOG_NTABLES; tbl++){
                    if(log.current_size(tbl)==0)
                        continue;
                    uint64_t recs_n = 0;
                    // The log.current_size(tbl) is concurrently updated by the log writers (tpc-c) so we cannot use it as "end" when replaying (it keeps growing). That's why the replay stops as soon as it encounters the specified epoch.
                    recs_n = logreplays[runner_num][tbl]-> template replay<decltype(callBack)> (current_epoch, log.current_size(tbl), callBack, tbl); // replay up to the epoch before the advance!
                    recs_replayed += recs_n;
                    //std::cout <<"@"<< current_epoch.value() << ": Thread "<< runner_num<< " played "<< recs_n << " from " << tbl<<std::endl;
                }
                // measure drain latency
                if(runner_num==0){
                    drain_latency =  (double)(read_tsc()-logdrain_start) / constants::million / constants::processor_tsc_frequency; // latency in ms
                }
                //STOP_COUNTING_PRINT(print_mutex, runner_num, recs_replayed)
                //std::cout<< "Thread "<< runner_num<<", Replayed "<< recs_replayed<<std::endl;
                /* ================= LOG DRAIN END ============== */
                /* ================= TPC-H QUERIES PREPARE ============== */
                r = pthread_barrier_wait(&dbsync_barrier);
                always_assert(r == PTHREAD_BARRIER_SERIAL_THREAD || r == 0, "Error in pthread_barrier_wait");
                // continue with TPCH queries!
                //STOP_COUNTING(drain_latencies, runner_num)
                #if CHANGE_EPOCH == 1
                last_drain = read_tsc();
                #endif
            }
            // include the log drain latency!
            STOP_COUNTING(q_latencies, runner_num)
            #endif
        }
        // experiment with materialized views
        //if(runner_num == 0)
        //    dbp->q4_stats();
    }

    // runner_id:  the id of the CPU that will run this thread
    // runner_num: the number of this thread [0-num_runners)
    static void tpcc_runner_thread(tpcc_db<DBParams>& db, db_profiler& prof, int runner_id, int runner_num, uint64_t w_start,
                                   uint64_t w_end, uint64_t w_own, double time_limit, uint64_t txns_per_epoch, int mix, uint64_t& txn_cnt) {
        tpcc_runner<DBParams> runner(runner_id, db, w_start, w_end, w_own, mix);
        typedef typename tpcc_runner<DBParams>::txn_type txn_type;

        uint64_t local_cnt = 0;

        #if CHANGE_EPOCH == 1
        (void)txns_per_epoch;
        #elif CHANGE_EPOCH == 2
        uint64_t report_txns = 1; // report the number of committed transactions periodically, so that the TPC-H logdrain will know about it!
        #endif

        std::array<uint64_t, NUM_DISTRICTS_PER_WAREHOUSE> last_delivered;
        if (mix < 3) // no need to execute if mix 3 is selected (read-only)
            std::fill(last_delivered.begin(), last_delivered.end(), 0);

        ::TThread::set_id(runner_id);
        set_affinity(runner_id);

        if(db.get_type() == db_type::hybrid){
            hybrid_db<DBParams>& db_h = reinterpret_cast<hybrid_db<DBParams>&>(db);
            db_h.thread_init_all_plus_sec(runner_num);
        }
        else
            db.thread_init_all(runner_num);

        uint64_t tsc_diff = (uint64_t)(time_limit * constants::processor_tsc_frequency * constants::billion);
        auto start_t = prof.start_timestamp();

        #if LOGGING > 0
            //uint64_t div = 10;
        #endif

        while (true) {
            // Executed enqueued delivery transactions, if any
            if (mix < 3) {  // no need to execute if mix 3 is selected (read-only)
                auto own_w_id = runner.owned_warehouse();
                if (own_w_id != 0) {
                    auto num_to_run = db.delivery_queue().read(own_w_id);
                    uint64_t num_run = 0;
                    bool stop = false;

                    if (num_to_run > 0) {
                        for (num_run = 0; num_run < num_to_run; ++num_run) {
                            runner.run_txn_delivery(own_w_id, last_delivered);
                            if ((read_tsc() - start_t) >= tsc_diff) {
                                stop = true;
                                ++num_run;
                                break;
                            }
                        }
                        local_cnt += num_run;
                        db.delivery_queue().dequeue(own_w_id, num_run);
                        if (stop)
                            break;
                        continue;
                    }
                }
            }

            auto curr_t = read_tsc();
            if ((curr_t - start_t) >= tsc_diff){
                break;
            }
            // advance log epoch periodically
            /*if ( runner_num == 0 && div>0 && (curr_t - start_t) >= tsc_diff/div){
                // slow if we do e.next_nonzero() and then e.value()! Either do e.next_nonzero() and don't get the value, or do e++ and e.value().
                //global_log_epoch = global_log_epoch.next_nonzero();
                global_log_epoch++;
                //std::cout<<"Changed epoch to "<< global_log_epoch.value()<<std::endl;
                div--;
            }*/

            txn_type t = runner.next_transaction();
            switch (t) {
                case txn_type::new_order:
                    runner.run_txn_neworder();
                    break;
                case txn_type::payment:
                    runner.run_txn_payment();
                    break;
                case txn_type::order_status:
                    runner.run_txn_orderstatus();
                    break;
                case txn_type::delivery: {
                    uint64_t q_w_id = runner.ig.random(w_start, w_end);
                    // All warehouse delivery transactions are delegated to
                    // its "owner" thread. This is in line with the actual
                    // TPC-C spec with regard to deferred execution.
                    // Do not count enqueued transactions as executed.
                    db.delivery_queue().enqueue(q_w_id);
                    --local_cnt;
                    break;
                }
                case txn_type::stock_level:
                    runner.run_txn_stocklevel();
                    break;
                default:
                    fprintf(stderr, "r:%d unknown txn type\n", runner_id);
                    assert(false);
                    break;
            };

            ++local_cnt;
            #if CHANGE_EPOCH == 2
            if(local_cnt > report_txns * txns_per_epoch/100){ // let the log drainers know about the number of committed txns
                txn_cnt = local_cnt;
                report_txns++;
            }
            #else
            (void)txns_per_epoch;
            #endif
        }

        txn_cnt = local_cnt;
    }

    static uint64_t run_benchmark(tpcc_db<DBParams>& db, db_profiler& prof, db_profiler& prof_tpch, int num_runners, vector<int> & run_cpus,
                                  double time_limit, int mix, const bool verbose) {
        vector<int> cpus_empty; // that's for TPC-H
        return run_benchmark(db, nullptr, prof, prof_tpch, num_runners, run_cpus, cpus_empty, time_limit, 0, mix, verbose);
    }

    static uint64_t run_benchmark(tpcc_db<DBParams>& db, void* olap_db, db_profiler& prof, db_profiler& prof_tpch, int num_runners, vector<int> & run_cpus, vector<int>& run_tpch_cpus,
                                  double time_limit, const uint64_t txns_per_epoch, int mix, const bool verbose) {

    #if RUN_TPCC
        std::vector<std::thread> runner_thrs;
        std::vector<uint64_t> txn_cnts(size_t(num_runners), 0);
    #else
        (void)prof;
        (void)num_runners;
        (void)verbose;
    #endif
    (void)olap_db;
    #if TPCH_SINGLE_NODE || TPCH_REPLICA
        std::vector<std::thread> tpch_runner_thrs;
        std::vector<uint64_t> q_cnts(size_t(run_tpch_cpus.size()), 0);
        std::vector<uint64_t> q_cnts_per_epoch(size_t(run_tpch_cpus.size()), 0);
        (void)prof_tpch;
    #else
        (void)run_tpch_cpus;
        (void)prof_tpch;
    #endif

        bool numa_aware = (run_cpus.size() > 0);

    #if RUN_TPCC
            int q = db.num_warehouses() / num_runners;
            int r = db.num_warehouses() % num_runners;
            int nwh = db.num_warehouses();
            auto calc_own_w_id = [nwh](int rid) {
                return (rid >= nwh) ? 0 : (rid + 1);
            };

            if(numa_aware)
                always_assert(num_runners == (int)run_cpus.size(), "Selected number of threads is not equal to the number of NUMA-aware CPUs!");

            if (q == 0) {
                q = (num_runners + db.num_warehouses() - 1) / db.num_warehouses();
                int qq = q;
                int wid = 1;
                for (int i = 0; i < num_runners; ++i) {
                    if ((qq--) == 0) {
                        ++wid;
                        qq += q;
                    }
                    if (verbose) {
                        fprintf(stdout, "runner %d: [%d, %d], own: %d\n", (numa_aware? run_cpus[i] : i), wid, wid, calc_own_w_id(i));
                    }
                    runner_thrs.emplace_back(tpcc_runner_thread, std::ref(db), std::ref(prof),
                                            (numa_aware? run_cpus[i] : i), i, wid, wid, calc_own_w_id(i), time_limit, txns_per_epoch, mix, std::ref(txn_cnts[i]));
                }
            } else {
                int last_xend = 1;

                for (int i = 0; i < num_runners; ++i) {
                    int next_xend = last_xend + q;
                    if (r > 0) {
                        ++next_xend;
                        --r;
                    }
                    if (verbose) {
                        fprintf(stdout, "runner %d: [%d, %d], own: %d\n", (numa_aware? run_cpus[i] : i), last_xend, next_xend - 1, calc_own_w_id(i));
                    }
                    runner_thrs.emplace_back(tpcc_runner_thread, std::ref(db), std::ref(prof),
                                            (numa_aware? run_cpus[i] : i), i, last_xend, next_xend - 1, calc_own_w_id(i), time_limit, txns_per_epoch, mix,
                                            std::ref(txn_cnts[i]));
                    last_xend = next_xend;
                }

                always_assert(last_xend == db.num_warehouses() + 1, "warehouse distribution error");
            }
    #endif

    #if MEASURE_LATENCIES && TEST_HASHTABLE
        bzero(latencies_hashtable_lookup, 2 * sizeof(uint64_t));
    #endif

    #if MEASURE_QUERY_LATENCY
    bzero(q_latencies, tpch_thrs_size * 2 * sizeof(uint64_t));
    bzero(drain_latencies, tpch_thrs_size * 2 * sizeof(uint64_t));
    #endif
        
    #if TPCH_SINGLE_NODE
        #if MEASURE_LATENCIES
        bzero(latencies_sec_ord_scan, tpch_thrs_size * 2 * sizeof(uint64_t));
        bzero(latencies_orderline_scan, tpch_thrs_size * 2 * sizeof(uint64_t));
        #endif
        // start tpch threads
        int num_tpch_runners = run_tpch_cpus.size();
        hybrid_db<DBParams>& db_h = reinterpret_cast<hybrid_db<DBParams>&>(db);
        for (int i = 0; i < num_tpch_runners; ++i) {
            tpch_runner_thrs.emplace_back(tpch_single_runner_thread, std::ref(db_h), std::ref(prof_tpch), (numa_aware? run_tpch_cpus[i] : i), i, time_limit, std::ref(q_cnts[i]));
        }
    #elif TPCH_REPLICA
        assert(olap_db);
        // start tpch threads
        int num_tpch_runners = run_tpch_cpus.size();
        #if LOG_DRAIN_REQUEST
        if(num_tpch_runners > 0){
            int r = pthread_barrier_init(&dbsync_barrier, nullptr, num_tpch_runners);
            always_assert(r==0, "Error in pthread_barrier_init");
        }
        // initialize log replay for each thread. We need to know where each thread is left so that to replay from that point, instead of replaying from the beginning.
        for(int i=0; i<num_tpch_runners; i++){
            auto & log = (reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs))->log(i);
            for (int tbl=0; tbl<LOG_NTABLES; tbl++){
                logreplays[i][tbl] = new logmem_replay(log.get_buf(tbl));
            }
        }
        #endif
        for (int i = 0; i < num_tpch_runners; ++i) {
            tpch_runner_thrs.emplace_back(tpch_replica_runner_thread, olap_db, std::ref(prof_tpch), (numa_aware? run_tpch_cpus[i] : i), i, time_limit, std::ref(q_cnts[i]), std::ref(q_cnts_per_epoch), std::ref(txn_cnts), txns_per_epoch);
        }
    #endif
    


    uint64_t total_txn_cnt = 0;

    #if TPCH_SINGLE_NODE || TPCH_REPLICA
    // join tpch threads
        for (auto &t: tpch_runner_thrs)
            t.join();

        if(tpch_runner_thrs.size()>0)
            std::cout<<"TPCH done!\n";
        uint64_t tpch_end = read_tsc();


        #if RUN_TPCC
            for (auto &t : runner_thrs)
                t.join();
            for (auto& cnt : txn_cnts)
                total_txn_cnt += cnt;
            // measure the TPC-C throughput separately from the TPC-H
            prof.finish(total_txn_cnt);
        #endif

        uint64_t q_total_count = 0;
        for (auto& cnt : q_cnts){
            q_total_count+= cnt;
        }
        // measure TPCH DB!
        tpch_db * dbp = reinterpret_cast<tpch_db*>(olap_db);
        prof_tpch.finish_tpch(q_total_count, tpch_end, dbp->db_size());

        // inspect dictionary!
        /*for(int i=0; i<db.num_warehouses(); i++){
            std::cout<<"Dict size: "<< db.tbl_orderlines(i).dict.size()<<std::endl;
            std::cout<<"Vect size: "<< db.tbl_orderlines(i).vec.size()<<std::endl;
        }*/
        
        
        #if TPCH_REPLICA && LOG_DRAIN_REQUEST
        if(num_tpch_runners > 0){
            int r = pthread_barrier_destroy(&dbsync_barrier);
            always_assert(r==0, "Error in pthread_barrier_destroy");
        }
        // delete log replay from each thread.
        for(int i=0; i<num_tpch_runners; i++){
            for (int tbl=0; tbl<LOG_NTABLES; tbl++){
                delete logreplays[i][tbl];
            }
        }
        #endif

        #if MEASURE_QUERY_LATENCY
        uint64_t l_query_total = 0, l_query_measurements_num=0;
        for (uint64_t i=0; i<tpch_thrs_size; i++){
            l_query_total += q_latencies[i][0];
            l_query_measurements_num += q_latencies[i][1];
        }
        std::cout<<"Q.4 Latency: "<< (double) l_query_total / l_query_measurements_num <<std::endl;

        uint64_t l_drain_total = 0, l_drain_measurements_num=0;
        for (uint64_t i=0; i<tpch_thrs_size; i++){
            l_drain_total += drain_latencies[i][0];
            l_drain_measurements_num += drain_latencies[i][1];
        }
        std::cout<<"Log drain Latency: "<< (double) l_drain_total / l_drain_measurements_num <<std::endl;
        #endif

        #if TPCH_SINGLE_NODE && MEASURE_LATENCIES
        uint64_t l_sec_total = 0, l_sec_measurements_num=0, l_orderline_total=0, l_orderline_measurements_num=0;
        for (uint64_t i=0; i<tpch_thrs_size; i++){
            l_sec_total += latencies_sec_ord_scan[i][0];
            l_sec_measurements_num += latencies_sec_ord_scan[i][1];
            l_orderline_total += latencies_orderline_scan[i][0];
            l_orderline_measurements_num += latencies_orderline_scan[i][1];
        }
        cout<<"Latency for scanning the orders secondary index: "<< (double)l_sec_total/l_sec_measurements_num << "("<<l_sec_measurements_num<<")\n";
        cout<<"Latency for scanning the orderline index: "<< (double)l_orderline_total/l_orderline_measurements_num << "("<<l_orderline_measurements_num<<")\n";
        #endif
        return total_txn_cnt;
    #else 
        #if RUN_TPCC
            for (auto &t : runner_thrs)
                t.join();
            for (auto& cnt : txn_cnts)
                total_txn_cnt += cnt;
            return total_txn_cnt;
        #endif

    #endif
    }

    static int parse_numa_nodes(char* opt, int num_threads, vector<int> & run_cpus, vector<int> & run_tpch_cpus, int db_numa_node, bool sibling){
        (void)db_numa_node;
        int numa_nodes_cnt=0;
        char* numa_node_str;
        int cpus_per_node =0;
        unsigned i=0;
        int cpus_total=0;
        #if TPCH_SINGLE_NODE || TPCH_REPLICA
        int tpch_cpus_total=0;
        #endif
        bool done=false;
        vector<int> run_numa_nodes;

        // assume equal number of CPUs in all NUMA nodes
        for (auto node : topo_info.cpu_id_list) {
            for(auto cpu : node){
                (void)cpu;
                cpus_per_node++;
            }
            break;
        }
        std::cout<<"Number of nodes: "<<topo_info.num_nodes<<std::endl;
        std::cout<<cpus_per_node<<" CPUs per node\n";

        numa_node_str = strtok(opt, ",");
        while(numa_node_str){
            run_numa_nodes.push_back(atoi(numa_node_str));
            numa_node_str = strtok(nullptr, ",");
            numa_nodes_cnt++;
        }
        if(numa_nodes_cnt > topo_info.num_nodes){
            std::cout<<"Cannot use "<< numa_nodes_cnt <<" NUMA nodes. System has only "<< topo_info.num_nodes<<std::endl;
            return -1;
        }
        if(num_threads <= (int) ((numa_nodes_cnt-1) * cpus_per_node)  || num_threads> (int) (numa_nodes_cnt * cpus_per_node)){
            std::cout<<"Cannot use " <<numa_nodes_cnt <<" NUMA node(s). Selected number of threads should run in " << (((num_threads-1) / cpus_per_node)+1) << " NUMA node(s)\n";
            return -1;
        }
        int CPUs_num = (int)topo_info.num_nodes * topo_info.cpu_id_list[0].size();
        if(sibling)
            std::cout<<"Pinning threads on sibling cores\n";
        std::cout<<"Running workers in NUMA nodes ";
        for (auto node : run_numa_nodes){
        #if TPCH_SINGLE_NODE
            if(topo_info.cpu_id_list[node].size() - num_threads < tpch_thrs_size ){
                std::cout<<"Error: Cannot run "<< tpch_thrs_size <<" TPCH runner threads on remaining " << (topo_info.cpu_id_list[0].size() - num_threads ) <<" cores\n";
                return -1;
            }
        #endif
            if(done)
                break;
            // get cpus of that NUMA node (up to nthreads total)
            // if sibling: (cpu + 40 ) % 80
            always_assert(node < (int)topo_info.num_nodes, "NUMA node is outside system's node range");
            for(auto cpu : topo_info.cpu_id_list[node]){
                // if done, assign the remaining cores for TPCH
                if(done){
                    #if TPCH_SINGLE_NODE
                    // see the difference when we start tpch threads in a separate NUMA node
                    /*int i=0;
                    for(auto cpu : topo_info.cpu_id_list[1]){
                        run_tpch_cpus.push_back(cpu);
                        if(++i == tpch_thrs_size)
                            break;
                    }
                    break;*/
                    if(!sibling){
                        if(tpch_cpus_total++ == tpch_thrs_size)
                            break;
                        run_tpch_cpus.push_back(cpu);
                    }
                    else{
                        if(tpch_cpus_total++ == tpch_thrs_size)
                            break;
                        run_tpch_cpus.push_back(cpu);
                        if(tpch_cpus_total++ == tpch_thrs_size)
                            break;
                        run_tpch_cpus.push_back( (cpu + CPUs_num/2) % CPUs_num ); // add its sibling thread
                    }
                    continue;
                    #elif TPCH_REPLICA
                    assert(db_numa_node>=0); // we must specify the NUMA node to store the DB, so that to know where to start the OLAP replica!
                    for(auto cpu : topo_info.cpu_id_list[(db_numa_node + 1) % topo_info.num_nodes ]){
                        if(tpch_cpus_total++ == tpch_thrs_size)
                            break;
                        run_tpch_cpus.push_back(cpu);
                    }
                    break;
                    #else
                    break;
                    #endif
                }
                
                if(!sibling){
                    run_cpus.push_back(cpu);
                    cpus_total++;
                }
                else{
                    run_cpus.push_back(cpu);
                    run_cpus.push_back( (cpu + CPUs_num/2) % CPUs_num ); // add its sibling thread
                    cpus_total+=2;
                }
                if(cpus_total == num_threads){
                    done=true;
                }
            }
            i++;
            std::cout<<node << (i==run_numa_nodes.size()? "" : ", ");
        }


        std::cout<<"\n";
        std::cout<<"Running on CPUs: [";
        i=0;
        for (auto cpu : run_cpus)
            std::cout<<cpu<< (++i < run_cpus.size() ? "," : "]\n" );
        if(run_tpch_cpus.size() > 0){
            std::cout<<"Running TPCH on CPUs: [";
            i=0;
            for (auto cpu : run_tpch_cpus)
                std::cout<<cpu<< (++i < run_tpch_cpus.size() ? "," : "]\n" );
        }
        return 0;
    }

    static bool log_op_apply_tpch(tpch_db& db, int runner_num, bool add_to_sec_index, const Str& key, const Str& val, kvtimestamp_t ts, uint32_t cmd, int tbl){
        order_sec_value os_val;
        (void)runner_num;
        if(cmd == logcmd_put || cmd == logcmd_replace){
            if(tbl == 2){ // table customers
                #if LOG_RECS == 1
                customer_key k(key);
                customer_value v(val);
                #endif
                //uint64_t wid = bswap(k.c_w_id);
                // add to OLAP db
            }
            else if(tbl==3) { // table orders
                #if LOG_RECS == 1
                order_key k(key);
                order_value v(val);
                #elif LOG_RECS > 1
                // strtok messes up key.s and also we don't want to make a copy of key.s! Tokenize the key/val manually in the constructor!
                order_key k(key.s);
                order_value v(val.s);
                #else
                order_key k(key);
                order_value v(val);
                #endif
                uint64_t wid = bswap(k.o_w_id);
                if(add_to_sec_index){
                    uint64_t did = bswap(k.o_d_id);
                    uint64_t oid = bswap(k.o_id);
                    order_sec_value os_val;
                    auto val_p = db.tbl_orders(wid).nontrans_put_el_if_ts(k, v, ts);
                    if(!val_p)
                        return true;
                    // store the internal_elem
                    os_val.o_c_entry_d_p = val_p;
                    // put into the secondary index as well
                    db.tbl_sec_orders().nontrans_put(order_sec_key(v.o_entry_d, wid, did, oid), os_val, true /* measure db size */);
                }
                else{
                    db.tbl_orders(wid).nontrans_put_if_ts(k, v, ts);
                }
            }
            else if(tbl==4) { // table orderlines                
                #if LOG_RECS == 1
                orderline_key k(key);
                orderline_value v(val);
                #elif LOG_RECS > 1
                orderline_key k(key.s);
                orderline_value v(val.s);
                #else
                orderline_key k(key);
                orderline_value v(val);
                #endif
                uint64_t wid = bswap(k.ol_w_id);
                #if DICTIONARY == 3
                std::string s = std::string(v.ol_dist_info);
                if(db.dict[runner_num].count(s) == 0){
                    db.dict[runner_num][s] = db.dict[runner_num].size();
                }
                //vec.emplace_back(1);
                #elif DICTIONARY == 4
                std::string s = std::string(v.ol_dist_info);
                auto * elem = db.dict[runner_num].nontrans_get(s);
                if(!elem){
                    db.dict[runner_num].nontrans_put(s, typename tpch_db::StrId(1));
                }
                #endif
                db.tbl_orderlines(wid).nontrans_put_if_ts(k, v, ts);
            }
        }
        else if(cmd == logcmd_remove){
            always_assert(false, "log command delete not supported yet!\n");
        }
        assert(cmd != logcmd_none);
        (void)os_val;
        return true;
    }

    #if REPLAY_LOG
    static void logdrainer_thread(tpch_db& db, bool add_to_secondary_idx, unsigned logdrainer_id, unsigned thread_id, kvepoch_t to_epoch, unsigned* logrecs_replayed){
        unsigned recs_replayed=0;
        std::cout<<"Logdrain thread "<< logdrainer_id <<", CPU: "<< thread_id<<std::endl;
        set_affinity(thread_id);
        db.thread_init_all();
        auto callBack = [&db, add_to_secondary_idx, logdrainer_id] (const Str& key, const Str& val, kvtimestamp_t ts, uint32_t cmd, int tbl) -> bool {
            return log_op_apply_tpch(db, logdrainer_id, add_to_secondary_idx, key, val, ts, cmd, tbl);
        };
        #if LOGGING == 1
            auto & log = (reinterpret_cast<logset*>(logs))->log(logdrainer_id);
            logmem_replay rep(log.get_buf());
            recs_replayed = rep.template replay<decltype(callBack)>(to_epoch, log.current_size(), callBack, -1);
            std::cout<<"Replayed "<< recs_replayed << " log records\n";
        #elif LOGGING == 2
            auto & log = (reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs))->log(logdrainer_id);
            for (int tbl=0; tbl<LOG_NTABLES; tbl++){
                if(log.current_size(tbl)==0)
                    continue;
                logmem_replay rep (log.get_buf(tbl));
                uint64_t recs = rep.template replay<decltype(callBack)>(to_epoch, log.current_size(tbl), callBack, tbl);
                recs_replayed += recs;
                //std::cout<<"Replayed "<< recs  <<" from " << tbl<<std::endl;
            }
        #elif LOGGING == 3
            auto & log = (reinterpret_cast<logset_map<LOG_NTABLES>*>(logs))->log(logdrainer_id);
            for (int tbl=0; tbl<LOG_NTABLES; tbl++){
                if(tbl != 3 && tbl != 4) // we cannot determine the size of the map with unordered_index, since it resizes it from initialization and shows a constant size(). Consider adding a field that measures the number of elements currently in the map
                    continue;
                logmap_replay rep(log.get_map(tbl));
                recs_replayed += rep.template replay<decltype(callBack)>(to_epoch, callBack, tbl);
            }
        #endif
        logrecs_replayed[logdrainer_id] = recs_replayed;
    }

    static unsigned replay_log(tpch_db & db, bool add_to_secondary_idx, int db_numa_node, kvepoch_t to_epoch){
        std::vector<std::thread> logdrainer_thrs;
        // start the logdrainer threads in the NUMA node after the TPC-C run NUMA node
        unsigned logdrainer_numa = (db_numa_node+1) % topo_info.num_nodes;
        std::cout<<"Starting log drainer threads in NUMA node "<< logdrainer_numa<<std::endl;
        auto & cpus = topo_info.cpu_id_list[logdrainer_numa];
        unsigned * logrecs_replayed = new unsigned [cpus.size()];
        unsigned i=0;
        for(auto& cpu : cpus){
            logdrainer_thrs.emplace_back(logdrainer_thread, std::ref(db), add_to_secondary_idx, i++, cpu, to_epoch, logrecs_replayed);
            if (i == NLOGGERS)
                break;
        }

        for (auto& thr : logdrainer_thrs)
            thr.join();

        unsigned log_recs_total=0;
        for(unsigned i=0; i<logdrainer_thrs.size(); i++){
            log_recs_total+= logrecs_replayed[i];
        }

        return log_recs_total;

        #if LOG_STATS
            std::unordered_map<Str, unsigned> ods_keys;
            std::unordered_map<Str, unsigned> odls_keys;

            auto callBackOrders = [&ods_keys] (const Str& key) -> void {
                if(ods_keys.count(key)>0)
                    ods_keys[key]++;
                else
                    ods_keys[key]=1;
            };

            auto callBackOrderlines = [&odls_keys] (const Str& key) -> void {
                if(odls_keys.count(key)>0)
                    odls_keys[key]++;
                else
                    odls_keys[key]=1;
            };

            for(unsigned i=0; i<NLOGGERS; i++){
                auto & log = (reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs))->log(i);
                std::cout<<"Log "<<i<<":\n";
                for (int tbl=0; tbl<LOG_NTABLES; tbl++){
                    if(log.current_size(tbl)==0)
                        continue;
                    std::cout<<"Table "<< tbl<<std::endl;
                    logmem_replay rep (log.get_buf(tbl));
                    unsigned recs_replayed=0;
                    if(tbl==3)
                        recs_replayed = rep.template replay<decltype(callBackOrders)>(to_epoch, log.current_size(tbl), callBackOrders, tbl);
                    else if (tbl==4)
                        recs_replayed = rep.template replay<decltype(callBackOrderlines)>(to_epoch, log.current_size(tbl), callBackOrderlines, tbl);
                    std::cout<<"Replayed "<< recs_replayed << " log records\n";
                }
            }

            auto parseTab = [] (Str tabName, std::unordered_map<Str, unsigned> & tab) -> void {
                std::cout<<"Table "<< tabName <<std::endl;
                std::cout<<"Number of keys: "<< tab.size()<<std::endl;
                uint64_t num_ops=0;
                for (auto & elem : tab){
                    //if(tabName == "Orderlines")
                    //    std::cout<<"Times: "<< elem.second<<std::endl;
                    num_ops+=elem.second;
                }
                std::cout<<"Average number of operations per key: "<< (float) num_ops/tab.size() <<std::endl;
            };

            parseTab("Orders", ods_keys);
            parseTab("Orderlines", odls_keys);
        #endif
    }
    #endif

    static void verify_db_worker(tpcc_db<DBParams> & db, tpch_db& olap_db, int w_id){
        // verify orders table
        {
            int num_orders_tpcc=0, num_orders_tpch=0;
            auto orders_callback_tpcc = [&num_orders_tpcc, &olap_db, &w_id] (const tpcc::order_key&k, const tpcc::order_value&v) -> bool{
                num_orders_tpcc++;
                tpcc::order_value * v_olap = olap_db.tbl_orders(w_id).nontrans_get(k);
                assert(v_olap);
                //assert(Str(v) == Str(*v_olap)); // WARNING: the size of order_value is actually 28 bytes but sizeof(order_value) is 32! This means the memcmp will compare more bytes at the end and it might say the values are not equal!! (Str operator == uses memcmp)
                assert(v == *v_olap);
                return true;
            };
            auto orders_callback_tpch = [&num_orders_tpch] (const tpcc::order_key&k, const tpcc::order_value&v) -> bool{
                (void)k;
                (void)v;
                num_orders_tpch++;
                return true;
            };
            order_key k0(0, 0, 0);
            order_key k1(10, NUM_DISTRICTS_PER_WAREHOUSE, std::numeric_limits<uint64_t>::max());
            bool success = db.tbl_orders(w_id)
                                    .template range_scan<decltype(orders_callback_tpcc), false> (true /*read-only access - no TItems*/, k0, k1, orders_callback_tpcc, tpcc::RowAccess::None, false /*No Phantom protection*/);
            assert(success);
            //std::cout<<"TPCC Warehouse "<< w_id <<" orders elems: " << num_orders_tpcc<<std::endl;
            success = olap_db.tbl_orders(w_id)
                                    .template range_scan<decltype(orders_callback_tpch), false> (true /*read-only access - no TItems*/, k0, k1, orders_callback_tpch, tpcc::RowAccess::None, false /*No Phantom protection*/);
            assert(success);
            //std::cout<<"TPCH Warehouse "<< w_id <<" orders elems: " << num_orders_tpch<<std::endl;
            assert(num_orders_tpcc == num_orders_tpch);
        }
        // verify orderlines table
        {
            int num_orderlines_tpcc=0, num_orderlines_tpch=0;
            auto orderlines_callback_tpcc = [&num_orderlines_tpcc, &olap_db, &w_id] (const orderline_key&k, const orderline_value&v) -> bool{
                num_orderlines_tpcc++;
                const orderline_value * v_olap = olap_db.tbl_orderlines(w_id).nontrans_get(k);
                assert(v_olap);
                assert(v == *v_olap);
                return true;
            };
            auto orderlines_callback_tpch = [&num_orderlines_tpch] (const orderline_key&k, const orderline_value&v) -> bool{
                (void)k;
                (void)v;
                num_orderlines_tpch++;
                return true;
            };
            orderline_key ol_k0(0, 0, 0, 0);
            orderline_key ol_k1(10, NUM_DISTRICTS_PER_WAREHOUSE, std::numeric_limits<uint64_t>::max(), 15);
            bool success = db.tbl_orderlines(w_id)
                                    .template range_scan<decltype(orderlines_callback_tpcc), false> (true /*read-only access - no TItems*/, ol_k0, ol_k1, orderlines_callback_tpcc, tpcc::RowAccess::None, false /*No Phantom protection*/);
            assert(success);
            success = olap_db.tbl_orderlines(w_id)
                                    .template range_scan<decltype(orderlines_callback_tpch), false> (true /*read-only access - no TItems*/, ol_k0, ol_k1, orderlines_callback_tpch, tpcc::RowAccess::None, false /*No Phantom protection*/);
            assert(success);
            assert(num_orderlines_tpcc == num_orderlines_tpch);
        }
    }
    
    static void verify_db(tpcc_db<DBParams> & db, tpch_db& olap_db){
        std::vector<std::thread> verify_thrs;
        for (int i = 1; i <= db.num_warehouses(); ++i)
            verify_thrs.emplace_back(verify_db_worker, std::ref(db), std::ref(olap_db), i);
        for (auto &t : verify_thrs)
            t.join();
    }

    static int execute(int argc, const char *const *argv) {
        std::cout << "*** DBParams::Id = " << DBParams::Id << std::endl;
        std::cout << "*** DBParams::Commute = " << std::boolalpha << DBParams::Commute << std::endl;
        int ret = 0;

        bool spawn_perf = false;
        bool counter_mode = false;
        int num_warehouses = 1;
        int num_threads = 1;
        int db_numa_node=-1;
        vector<int> run_cpus;
        vector<int> run_tpch_cpus;
        int mix = 0;
        double time_limit = 10.0;
        bool enable_gc = false;
        unsigned gc_rate = Transaction::get_epoch_cycle();
        bool verbose = false;
        bool sibling = false; // pin threads on sibling cores (if hyper-threading is on)

        Clp_Parser *clp = Clp_NewParser(argc, argv, noptions, options);

        int opt;
        bool clp_stop = false;
        uint64_t txns_per_epoch = 0;

        while (!clp_stop && ((opt = Clp_Next(clp)) != Clp_Done)) {
            switch (opt) {
                case opt_dbid:
                    break;
                case opt_nwhs:
                    num_warehouses = clp->val.i;
                    break;
                case opt_nthrs:
                    num_threads = clp->val.i;
                    break;
                case opt_db_numa:
                    if(clp->val.i > topo_info.num_nodes-1){
                        std::cout<<"Cannot use "<< clp->val.i <<" NUMA node. System has " <<topo_info.num_nodes<<
                                " nodes. [0-" <<(topo_info.num_nodes-1) <<"]." <<std::endl;
                        ret = -1;
                        clp_stop = true;
                        break;
                    }
                    db_numa_node = clp->val.i;
                    std::cout<<"Storing DB (warehouses) in NUMA node "<< db_numa_node<<std::endl;
                    break;
                case opt_run_numa:
                    if((ret = parse_numa_nodes((char*)clp->val.s, num_threads, run_cpus, run_tpch_cpus, db_numa_node, sibling)) != 0){
                        clp_stop = true;
                        break;
                    }
                    break;
                case opt_txns_per_epoch:
                    txns_per_epoch = clp->val.i;
                    break;

                case opt_sibling:
                    sibling = true;
                    break;
                case opt_time:
                    time_limit = clp->val.d;
                    break;
                case opt_perf:
                    spawn_perf = !clp->negated;
                    break;
                case opt_pfcnt:
                    counter_mode = !clp->negated;
                    break;
                case opt_gc:
                    enable_gc = !clp->negated;
                    break;
                case opt_gr:
                    gc_rate = clp->val.i;
                    break;
                case opt_node:
                    break;
                case opt_comm:
                    break;
                case opt_verb:
                    verbose = !clp->negated;
                    break;
                case opt_mix:
                    mix = clp->val.i;
                    if (mix > 4 || mix < 0) {
                        mix = 0;
                    }
                    break;
                default:
                    ::print_usage(argv[0]);
                    ret = 1;
                    clp_stop = true;
                    break;
            }
        }

        Clp_DeleteParser(clp);
        if (ret != 0)
            return ret;

        // Set thread affinity for main thread 
        if(db_numa_node>=0){ // NUMA-aware
            // get CPUs of the specified NUMA node
            auto & cpus = topo_info.cpu_id_list[db_numa_node];
            set_affinity(cpus[0]); // start filler thread from the first CPU of the desired NUMA node
        }
        else{
            set_affinity(0);
        }

        std::cout << "Selected workload mix: " << std::string(workload_mix_names[mix]) << std::endl;

        auto profiler_mode = counter_mode ?
                             Profiler::perf_mode::counters : Profiler::perf_mode::record;

        if (counter_mode && !spawn_perf) {
            // turns on profiling automatically if perf counters are requested
            spawn_perf = true;
        }

        if (spawn_perf) {
            std::cout << "Info: Spawning perf profiler in "
                      << (counter_mode ? "counter" : "record") << " mode" << std::endl;
        }

        #if TPCH_SINGLE_NODE
        /*if (DBParams::Id != db_params_id::MVCC){
            std::cout<<"Error: Should use MVCC when running TPC-H\n";
            return -1;
        }*/
        #endif

        db_profiler prof(spawn_perf);
        #if TPCH_SINGLE_NODE
        hybrid_db<DBParams> db(num_warehouses);
        #else
        tpcc_db<DBParams> db(num_warehouses);
        #endif

        // Dimos - enable logging
        if(LOGGING == 1) {
            logs = logset::make(NLOGGERS);
            // TODO: initiali_timestamp should be related to the txn commit TID, since qtimes.ts is the commit TID of the current txn.
            initial_timestamp = timestamp();
            global_log_epoch = 1;
        }
        else if(LOGGING == 2){
            // 9 tables here
            std::vector<int> tbl_sizes ({
                1024 * 1024,            // warehouses   0
                1024 * 1024,       // districts    1
                8* 1024 * 1024 * 100,      // customers    2
                1* 1024 * 1024 * 100,   // orders       3
                8 * 1024 * 1024 * 100, // orderlines   4
                1024 * 1024,            // stocks       5
                1024 * 1024 * 20,      // new orders   6
                1024 * 1024 ,           // items        7
                1024 * 1024             // histories    8
            });
            logs = logset_tbl<LOG_NTABLES>::make(NLOGGERS, tbl_sizes);
            initial_timestamp = timestamp();
            global_log_epoch = 1;
        }
        else if(LOGGING == 3){
            logs = logset_map<LOG_NTABLES>::make(NLOGGERS);
        }

        std::cout << "Prepopulating database..." << std::endl;
        prepopulate_db(db, db_numa_node);
        std::cout << "Prepopulation complete." << std::endl;
        
        #if TPCH_REPLICA
        std::cout<<"Copying database to the OLAP replica by replaying the logs...\n";
        // store the OLAP DB in the NUMA node after the TPC-C DB NUMA node
        always_assert(db_numa_node>=0, "Must run NUMA-aware for running the hybrid OLTP/OLAP with replicated DB!\n");
        unsigned logdrainer_numa = (db_numa_node+1) % topo_info.num_nodes;
        std::cout<<"Storing OLAP in NUMA node "<< logdrainer_numa<<std::endl;
        auto & cpus = topo_info.cpu_id_list[logdrainer_numa];
        // set the affinity of this thread to a CPU on the OLAP numa node
        set_affinity(cpus[0]);
        tpch_db olap_db(num_warehouses);
        
        // replicate the DB to the other OLAP node
        if(LOGGING == 2){
            replay_log(olap_db, true /*add to sec indx*/, db_numa_node, 0);
            // reset the logs
            for(unsigned i=0; i<NLOGGERS; i++){
                auto & log = (reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs))->log(i);
                log.reset_all();
            }
        }
        #endif

        #if TPCH_SINGLE_NODE || TPCH_REPLICA
        db_profiler prof_tpch(spawn_perf);
            //std::cout<<"DB size: "<< DB_size[0] / 1024 / 1024 <<"MB\n";
        #endif

        std::thread advancer;
        std::cout << "Garbage collection: ";
        if (enable_gc) {
            std::cout << "enabled, running every " << gc_rate / 1000.0 << " ms";
            Transaction::set_epoch_cycle(gc_rate);
            advancer = std::thread(&Transaction::epoch_advancer, nullptr);
        } else {
            std::cout << "disabled";
        }
        std::cout << std::endl << std::flush;
        

        #if TEST_HASHTABLE
            INIT_COUNTING
            for (uint64_t wid = 1; wid <= db.num_warehouses(); wid++){
                for (uint64_t did = 1; did <= NUM_DISTRICTS_PER_WAREHOUSE; ++did) {
                    for (uint64_t i = 1; i <= NUM_CUSTOMERS_PER_DISTRICT; ++i) {
                        uint64_t oid = i;
                        srand(time(nullptr));
                        auto ol_count = (uint32_t) rand()%11 + 5;

                        for (uint64_t on = 1; on <= ol_count; ++on) {
                            orderline_key olk(wid, did, oid, on);
                            START_COUNTING
                            auto val = db.tbl_hash_orderlines(wid).nontrans_get(olk);
                            STOP_COUNTING(latencies_hashtable_lookup, 0)
                        }
                    }
                }
            }
            cout<<"Hashtable lookup: "<< (double)latencies_hashtable_lookup[0][0]/latencies_hashtable_lookup[0][1] <<"("<<latencies_hashtable_lookup[0][1]<<")"<<endl;
        #endif


        #if RUN_TPCC
        prof.start(profiler_mode);
        #endif
        #if TPCH_SINGLE_NODE
            prof_tpch.start(profiler_mode);
            run_benchmark(db, nullptr /*No dedicated OLAP DB!*/, prof, prof_tpch, num_threads, run_cpus, run_tpch_cpus, time_limit, mix, verbose);
            // measure throughput inside run_benchmark to only take into account the tpcc transactions and not the tpch
        #elif TPCH_REPLICA
            prof_tpch.start(profiler_mode);
            run_benchmark(db, &olap_db, prof, prof_tpch, num_threads, run_cpus, run_tpch_cpus, time_limit, txns_per_epoch, mix, verbose);
            // measure throughput inside run_benchmark to only take into account the tpcc transactions and not the tpch
        #else
            auto txns_total = run_benchmark(db, prof, prof, num_threads, run_cpus, time_limit, mix, verbose);
            prof.finish(txns_total);
        #endif

        //#if LOG_STATS
        float total_log_sz=0;
        int total_log_records = 0;
        if(LOGGING==1){
            // inspect log:
            for(unsigned i=0; i<NLOGGERS; i++){
                auto & log = (reinterpret_cast<logset*>(logs))->log(i);
                //std::cout<<"Log "<<i<<" size: " << log.cur_log_records()<<", " << (float)log.current_size() / 1024 <<"KB\n";
                total_log_sz+= (float)log.current_size() / 1024 / 1024;
                total_log_records+= log.cur_log_records();
            }
        }
        else if(LOGGING == 2){
            for(unsigned i=0; i<NLOGGERS; i++){
                auto & log = (reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs))->log(i);
                //std::cout<<"Log "<<i<<":\n";
                for (int tbl=0; tbl<LOG_NTABLES; tbl++){
                    if(log.current_size(tbl)==0)
                        continue;
                    //std::cout<<"\tTable "<<tbl <<" size: "<< (float)log.current_size(tbl) / 1024 <<"KB\n";
                    total_log_sz+= (float)log.current_size(tbl) / 1024 / 1024;
                }
                total_log_records+= log.cur_log_records();
            }
        }
        else if (LOGGING == 3){
            for(unsigned i=0; i<NLOGGERS; i++){
                auto & log = (reinterpret_cast<logset_map<LOG_NTABLES>*>(logs))->log(i);
                //std::cout<<"Log "<<i<<":\n";
                for (int tbl=0; tbl<LOG_NTABLES; tbl++){
                    if(log.current_size(tbl)==0 && log.cur_log_records(tbl)==0)
                        continue;
                    //std::cout<<"\tTable "<<tbl <<" size: "<< (float)log.current_size(tbl) / 1024 <<"KB\n";
                    total_log_sz+= (float)log.current_size(tbl) / 1024 / 1024;
                    total_log_records+= log.cur_log_records(tbl);
                }
            }
        }
        //std::cout<<"Total log records: "<< total_log_records <<std::endl;
        //std::cout<<"Total log size (MB): "<< total_log_sz <<std::endl;
        //#endif


        // parse the log!
        #if LOGGING > 0 && REPLAY_LOG
        std::cout<<"Replaying log. Number of epochs: "<< global_log_epoch.value() << std::endl;
        log_replay_profiler rep_prof;
        rep_prof.start();
        kvepoch_t to_epoch = 0; // replay the entire log
        unsigned log_recs_replayed = replay_log(olap_db, false, db_numa_node, to_epoch);
        rep_prof.finish(total_log_sz, log_recs_replayed, total_log_records);
        #endif        

        size_t remaining_deliveries = 0;
        for (int wh = 1; wh <= db.num_warehouses(); wh++) {
            remaining_deliveries += db.delivery_queue().read(wh);
        }
        std::cout << "Remaining unresolved deliveries: " << remaining_deliveries << std::endl;

        #if LOGGING > 0
        #if VERIFY_OLAP_DB
        std::cout<<"Verifying OLAP DB...\n";
        verify_db(db, olap_db);
        #endif
        #endif

        if (enable_gc) {
            Transaction::global_epochs.run = false;
            advancer.join();
        }

        //std::cout<<"Cleaning up RCU set items\n";

        // Clean up all remnant RCU set items.
        for (int i = 0; i < num_threads; ++i) {
            Transaction::tinfo[i].rcu_set.release_all();
        }

        #if LOGGING==1 
            logset::free(reinterpret_cast<logset*>(logs));
        #elif LOGGING==2
            logset_tbl<LOG_NTABLES>::free(reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs));
        #elif LOGGING == 3
            logset_map<LOG_NTABLES>::free(reinterpret_cast<logset_map<LOG_NTABLES>*>(logs));
        #endif

        //std::cout<<"Done!\n";
        return 0;
    }
}; // class tpcc_access

};

#include "TPCH_queries.hh"
