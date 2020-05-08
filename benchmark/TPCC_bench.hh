#pragma once

#include <iostream>
#include <iomanip>
#include <random>
#include <string>
#include <thread>

#include <vector>


#include "../util/measure_latencies.hh"

#define RUN_TPCH 0
#define RUN_TPCC 1
#define TEST_HASHTABLE 0

#if MEASURE_LATENCIES && TEST_HASHTABLE
static double latencies_hashtable_lookup[1][2];
#endif

#if RUN_TPCH
constexpr short logging = 0;
constexpr unsigned tpch_thrs_size = 10; // the number of TPCH runner threads

#if MEASURE_LATENCIES
static uint64_t latencies_sec_ord_scan[tpch_thrs_size][2];
static uint64_t latencies_orderline_scan[tpch_thrs_size][2];
#endif
static uint64_t DB_size=0;
#else
// Dimos - enable logging
// 0: no logging
// 1: default log - one log per thread
// 2: one log per thread per table
constexpr short logging = 2;
#endif

constexpr int nlogger = 10;

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

#include "log.hh"

#include "DB_index.hh"
#include "DB_params.hh"
#include "DB_profiler.hh"
#include "PlatformFeatures.hh"

#define A_GEN_CUSTOMER_ID           1023
#define A_GEN_ITEM_ID               8191

#define C_GEN_CUSTOMER_ID           259
#define C_GEN_ITEM_ID               7911

using namespace std;

#include "TPCH_runner.hh"

// #include "TPCH_queries.hh" at the bottom of the file so that to see the types of the structs used!!
// the other solution is to include TPCH_queries.hh in all tpcc_d.cc, tpcc_dc.cc, etc! This one looks nicer :)

// @section: clp parser definitions
enum {
    opt_dbid = 1, opt_nwhs, opt_nthrs, opt_db_numa, opt_run_numa, opt_sibling, opt_time, opt_perf, opt_pfcnt, opt_gc,
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

template <typename DBParams>
class tpcc_db {
public:
    template <typename K, typename V, short LOG=0>
    using OIndex = typename std::conditional<DBParams::MVCC,
          mvcc_ordered_index<K, V, DBParams>,
          ordered_index<K, V, DBParams, LOG>>::type;

#if TPCC_HASH_INDEX
    template <typename K, typename V>
    using UIndex = typename std::conditional<DBParams::MVCC,
          mvcc_unordered_index<K, V, DBParams>,
          unordered_index<K, V, DBParams>>::type;
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
    typedef UIndex<customer_key, customer_value>         cu_table_type;
    typedef OIndex<order_key, order_value, logging>               od_table_type;
#if RUN_TPCH
    // secondary index for <o_entry_d, order_value*>
    typedef OIndex<order_sec_key, order_sec_value>       od_sec_entry_d_type;
    //typedef OIndex<orderline_sec_key, orderline_sec_value> ol_sec_deliv_d_type;
#endif
    typedef OIndex<orderline_key, orderline_value, logging>       ol_table_type;
    #if TEST_HASHTABLE
    typedef UIndex<orderline_key, orderline_value_deliv_d> ol_hashtable_type;
    #endif
    typedef UIndex<stock_key, stock_value>               st_table_type;
#endif
    typedef UIndex<customer_idx_key, customer_idx_value> ci_table_type;
    typedef OIndex<order_cidx_key, bench::dummy_row>     oi_table_type;
    typedef OIndex<order_key, bench::dummy_row>          no_table_type;
    typedef UIndex<item_key, item_value>                 it_table_type;
    typedef OIndex<history_key, history_value>           ht_table_type;

    explicit inline tpcc_db(int num_whs);
    explicit inline tpcc_db(const std::string& db_file_name) = delete;
    inline ~tpcc_db();
    // runner_num: the number of the runner thread: [0-num_runners)
    void thread_init_all(int runner_num);
    #if RUN_TPCH
    void thread_init_tpch();
    #endif

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
    #if RUN_TPCH
    od_sec_entry_d_type& tbl_sec_orders(){
        return tbl_sec_ods_;
    }
    //ol_sec_deliv_d_type& tbl_sec_orderlines(){
    //    return tbl_sec_ols_;
    //}
    #endif
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

private:
    size_t num_whs_;
    it_table_type *tbl_its_;

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
    #if RUN_TPCH
    od_sec_entry_d_type tbl_sec_ods_;
    //ol_sec_deliv_d_type tbl_sec_ols_;
    #endif
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
#if TPCC_SPLIT_TABLE
      tbl_whs_const_(256),
      tbl_whs_comm_(256),
#else
      tbl_whs_(256),
#endif
#if RUN_TPCH
    tbl_sec_ods_(num_whs * 999983/*num_customers * 10 * 2*/),
    //tbl_sec_ols_(num_whs * 999983/*num_customers * 100 * 2*/),
#endif
      oid_gen_() {
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
        tbl_cus_.emplace_back(999983/*num_customers * 2*/);
        tbl_ods_.emplace_back(999983/*num_customers * 10 * 2*/, 3); // enable logging for orders table. Its index in the log is 3
        tbl_ols_.emplace_back(999983/*num_customers * 100 * 2*/, 4); // enable logging for orderlines table. Its index in the log is 4
        #if TEST_HASHTABLE
        tbl_hash_ols_.emplace_back(999983/*num_customers * 100 * 2*/);
        #endif
        tbl_sts_.emplace_back(999983/*NUM_ITEMS * 2*/);
#endif
        tbl_cni_.emplace_back(999983/*num_customers * 2*/);
        tbl_oci_.emplace_back(999983/*num_customers * 2*/);
        tbl_nos_.emplace_back(999983/*num_customers * 10 * 2*/);
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
        t.thread_init();
    for (auto& t : tbl_ods_)
        t.thread_init(runner_num); // enable logging
    #if RUN_TPCH
    tbl_sec_ods_.thread_init();
    //tbl_sec_ols_.thread_init();
    #endif
    for (auto& t : tbl_ols_)
        t.thread_init(runner_num); // enable logging
    for (auto& t : tbl_sts_)
        t.thread_init();
#endif
    for (auto& t : tbl_cni_)
        t.thread_init();
    for (auto& t : tbl_oci_)
        t.thread_init();
    for (auto& t : tbl_nos_)
        t.thread_init();
    for (auto& t : tbl_hts_)
        t.thread_init();
}

#if RUN_TPCH
template <typename DBParams>
void tpcc_db<DBParams>::thread_init_tpch(){ // call thread init only for the tables required for TPCH
    for (auto& t : tbl_ods_)
        t.thread_init(); // why do we need logging for TPC-H??
    tbl_sec_ods_.thread_init();
    //tbl_sec_ols_.thread_init();
    for (auto& t : tbl_ols_)
        t.thread_init();
}
#endif

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
        for (auto i = 0; i < NUM_DISTRICTS_PER_WAREHOUSE; ++i)
            sv.s_dists[i] = random_a_string(24, 24);
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

        #if RUN_TPCH
            order_sec_value val;
            auto val_p = db.tbl_orders(wid).nontrans_put(ok, ov);
            // store the internal_elem
            val.o_c_entry_d_p = val_p;
            // put into the secondary index as well
            db.tbl_sec_orders().nontrans_put(order_sec_key(entry_date, wid, did, oid), val);
        #else
            db.tbl_orders(wid).nontrans_put(ok, ov);
        #endif
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
                olv.ol_dist_info = random_a_string(24, 24);
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
    static void prepopulation_worker(tpcc_db<DBParams> &db, int worker_id, int warehouse_id, int filler_id) {
        tpcc_prepopulator<DBParams> pop(worker_id, warehouse_id, filler_id, db);
        db.thread_init_all(worker_id);
        pop.run();
    }

    static void prepopulate_db(tpcc_db<DBParams> &db, int db_numa_node) {
        int r;
        r = pthread_barrier_init(&tpcc_prepopulator<DBParams>::sync_barrier, nullptr, db.num_warehouses());
        always_assert(r == 0, "pthread_barrier_init failed");

        std::vector<std::thread> prepop_thrs;
        if(db_numa_node == -1){ // normal operation (NUMA unaware): use worker id == warehouse id and filler id == 1
            for (int i = 1; i <= db.num_warehouses(); ++i)
                prepop_thrs.emplace_back(prepopulation_worker, std::ref(db), i-1, i, 0);
        }
        else { // NUMA aware: Create DB only in specified NUMA node: worker id != warehouse id
            auto & cpus = topo_info.cpu_id_list[db_numa_node];
            for (int i = 1; i <= db.num_warehouses(); ++i){
                prepop_thrs.emplace_back(prepopulation_worker, std::ref(db), cpus[(i-1) % cpus.size()], i, cpus[0]); // filler will be the first cpu of the specified NUMA node (db_numa_node)
            }
            
        }
        for (auto &t : prepop_thrs)
            t.join();

        r = pthread_barrier_destroy(&tpcc_prepopulator<DBParams>::sync_barrier);
        always_assert(r == 0, "pthread_barrier_destroy failed");
    }

    /**  TPCH  **
     **        **/
    #if RUN_TPCH
    static void tpch_runner_thread(tpcc_db<DBParams>& db, db_profiler& prof, int runner_id, double time_limit, int mix, uint64_t& q_cnt){
        tpcc::tpcc_input_generator ig(runner_id, db.num_warehouses());
        tpch::tpch_runner<DBParams> runner(runner_id, ig, db, mix);
        ::TThread::set_id(runner_id);
        set_affinity(runner_id);
        db.thread_init_tpch();
        
        uint64_t tsc_diff = (uint64_t)(time_limit * constants::processor_tsc_frequency * constants::billion);
        auto start_t = prof.start_timestamp();

        while(true){
            runner.run_next_query();
            q_cnt++;
            if ((read_tsc() - start_t) >= tsc_diff) { // done
                break;
            }
        }
    }
    #endif

    // runner_id:  the id of the CPU that will run this thread
    // runner_num: the number of this thread [0-num_runners)
    static void tpcc_runner_thread(tpcc_db<DBParams>& db, db_profiler& prof, int runner_id, int runner_num, uint64_t w_start,
                                   uint64_t w_end, uint64_t w_own, double time_limit, int mix, uint64_t& txn_cnt) {
        tpcc_runner<DBParams> runner(runner_id, db, w_start, w_end, w_own, mix);
        typedef typename tpcc_runner<DBParams>::txn_type txn_type;

        uint64_t local_cnt = 0;

        std::array<uint64_t, NUM_DISTRICTS_PER_WAREHOUSE> last_delivered;
        if (mix < 3) // no need to execute if mix 3 is selected (read-only)
            std::fill(last_delivered.begin(), last_delivered.end(), 0);

        ::TThread::set_id(runner_id);
        set_affinity(runner_id);
        db.thread_init_all(runner_num);

        uint64_t tsc_diff = (uint64_t)(time_limit * constants::processor_tsc_frequency * constants::billion);
        auto start_t = prof.start_timestamp();

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
            if ((curr_t - start_t) >= tsc_diff)
                break;

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
        }

        txn_cnt = local_cnt;
    }

    static uint64_t run_benchmark(tpcc_db<DBParams>& db, db_profiler& prof, db_profiler& prof_tpch, int num_runners, vector<int> & run_cpus,
                                  double time_limit, int mix, const bool verbose) {
        vector<int> cpus_empty; // that's for TPC-H
        return run_benchmark(db, prof, prof_tpch, num_runners, run_cpus, cpus_empty, time_limit, mix, verbose);
    }

    static uint64_t run_benchmark(tpcc_db<DBParams>& db, db_profiler& prof, db_profiler& prof_tpch, int num_runners, vector<int> & run_cpus, vector<int>& run_tpch_cpus,
                                  double time_limit, int mix, const bool verbose) {

    #if RUN_TPCC
        std::vector<std::thread> runner_thrs;
        std::vector<uint64_t> txn_cnts(size_t(num_runners), 0);
    #else
        (void)prof;
        (void)num_runners;
        (void)verbose;
    #endif

    #if RUN_TPCH
        std::vector<std::thread> tpch_runner_thrs;
        std::vector<uint64_t> q_cnts(size_t(run_tpch_cpus.size()), 0);
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
                                            (numa_aware? run_cpus[i] : i), i, wid, wid, calc_own_w_id(i), time_limit, mix, std::ref(txn_cnts[i]));
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
                                            (numa_aware? run_cpus[i] : i), i, last_xend, next_xend - 1, calc_own_w_id(i), time_limit, mix,
                                            std::ref(txn_cnts[i]));
                    last_xend = next_xend;
                }

                always_assert(last_xend == db.num_warehouses() + 1, "warehouse distribution error");
            }
    #endif

    #if MEASURE_LATENCIES && TEST_HASHTABLE
        bzero(latencies_hashtable_lookup, 2 * sizeof(uint64_t));
    #endif
        

    #if RUN_TPCH
        #if MEASURE_LATENCIES
        bzero(latencies_sec_ord_scan, tpch_thrs_size * 2 * sizeof(uint64_t));
        bzero(latencies_orderline_scan, tpch_thrs_size * 2 * sizeof(uint64_t));
        #endif
        // start tpch threads
        int num_tpch_runners = run_tpch_cpus.size();
        for (int i = 0; i < num_tpch_runners; ++i) {
            tpch_runner_thrs.emplace_back(tpch_runner_thread, std::ref(db), std::ref(prof_tpch), (numa_aware? run_tpch_cpus[i] : i), time_limit, mix, std::ref(q_cnts[i]));
        }
    #endif


    uint64_t total_txn_cnt = 0;
    #if RUN_TPCC
        for (auto &t : runner_thrs)
            t.join();
        for (auto& cnt : txn_cnts)
            total_txn_cnt += cnt;
    #endif
    #if RUN_TPCH
        #if RUN_TPCC
            // measure the TPC-C throughput separately from the TPC-H
            prof.finish(total_txn_cnt);
        #endif
        // join tpch threads
        for (auto &t: tpch_runner_thrs)
            t.join();

        #if MEASURE_LATENCIES
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

        if(tpch_runner_thrs.size()>0)
            std::cout<<"TPCH done!\n";
        uint64_t q_total_count = 0;
        for (auto& cnt : q_cnts){
            q_total_count+= cnt;
        }
        prof_tpch.finish(q_total_count);
        return total_txn_cnt;
    #else 
        #if RUN_TPCC
            return total_txn_cnt;
        #endif

    #endif
    }

    static int parse_numa_nodes(char* opt, int num_threads, vector<int> & run_cpus, vector<int> & run_tpch_cpus, bool sibling){
        int numa_nodes_cnt=0;
        char* numa_node_str;
        int cpus_per_node =0;
        unsigned i=0;
        int cpus_total=0;
        #if RUN_TPCH
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
        #if RUN_TPCH
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
                    #if RUN_TPCH
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
                    if((ret = parse_numa_nodes((char*)clp->val.s, num_threads, run_cpus, run_tpch_cpus, sibling)) != 0){
                        clp_stop = true;
                        break;
                    }
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
        if(db_numa_node>0){ // NUMA-aware
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

        #if RUN_TPCH
        /*if (DBParams::Id != db_params_id::MVCC){
            std::cout<<"Error: Should use MVCC when running TPC-H\n";
            return -1;
        }*/
        #endif

        db_profiler prof(spawn_perf);
        tpcc_db<DBParams> db(num_warehouses);

        std::cout << "Prepopulating database..." << std::endl;
        prepopulate_db(db, db_numa_node);
        std::cout << "Prepopulation complete." << std::endl;

        #if RUN_TPCH
        db_profiler prof_tpch(spawn_perf, DB_size);
        #endif

        #if RUN_TPCH
            std::cout<<"DB size: "<< DB_size / 1024 / 1024 <<"MB\n";
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


        // Dimos - enable logging
        if(logging == 1) {
            logs = logset::make(nlogger);
            // TODO: initiali_timestamp should be related to the txn commit TID, since qtimes.ts is the commit TID of the current txn.
            initial_timestamp = timestamp();
        }
        else if(logging == 2){
            // 9 tables here
            std::vector<int> tbl_sizes = {
                1024 * 1024, // warehouses
                1024 * 1024 * 50, // districts
                1024 * 1024 * 50, // customers
                2* 1024 * 1024 * 100, // orders
                14 * 1024 * 1024 * 100, // orderlines
                1024 * 1024, // stocks
                1024 * 1024 * 100, // new orders
                1024 * 1024 * 100, // items
                1024 * 1024 * 100// histories
            };
           logs_tbl = logset_tbl<9>::make(nlogger, tbl_sizes);

           initial_timestamp = timestamp();
        }

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

        #if RUN_TPCH
            #if RUN_TPCC
            prof.start(profiler_mode);
            #endif
        prof_tpch.start(profiler_mode);
        run_benchmark(db, prof, prof_tpch, num_threads, run_cpus, run_tpch_cpus, time_limit, mix, verbose);
        // measure throughput inside run_benchmark to only take into account the tpcc transactions and not the tpch
        #else
            #if RUN_TPCC
            prof.start(profiler_mode);
            auto txns_total = run_benchmark(db, prof, prof, num_threads, run_cpus, time_limit, mix, verbose);
            prof.finish(txns_total);
            #endif
        #endif

        float total_log_sz=0;
        int total_log_records = 0;
        if(logging==1){
            // inspect log:
            for(unsigned i=0; i<nlogger; i++){
                auto & log = logs->log(i);
                std::cout<<"Log "<<i<<" size: " << log.cur_log_records()<<", " << (float)log.current_size() / 1024 <<"KB\n";
                total_log_sz+= (float)log.current_size() / 1024 / 1024;
                total_log_records+= log.cur_log_records();
            }
        }
        else if(logging == 2){
            for(unsigned i=0; i<nlogger; i++){
                auto & log = logs_tbl->log(i);
                std::cout<<"Log "<<i<<":\n";
                for (int tbl=0; tbl<9; tbl++){
                    std::cout<<"\tTable "<<tbl <<" size: "<< (float)log.current_size(tbl) / 1024 <<"KB\n";
                    total_log_sz+= (float)log.current_size(tbl) / 1024 / 1024;
                }
                total_log_records+= log.cur_log_records();
            }
        }
        std::cout<<"Total log records: "<< total_log_records <<std::endl;
        std::cout<<"Total log size (MB): "<< total_log_sz <<std::endl;

        //std::cout<<"Size of order_value: "<<sizeof(struct order_value)<<std::endl;
        //std::cout<<"Size of orderline_value: "<<sizeof(struct orderline_value)<<std::endl;

        //std::cout<<"Size of order_key: "<<sizeof(struct order_key)<<std::endl;
        //std::cout<<"Size of orderline_key: "<<sizeof(struct orderline_key)<<std::endl;
        
        

        size_t remaining_deliveries = 0;
        for (int wh = 1; wh <= db.num_warehouses(); wh++) {
            remaining_deliveries += db.delivery_queue().read(wh);
        }
        std::cout << "Remaining unresolved deliveries: " << remaining_deliveries << std::endl;

        if (enable_gc) {
            Transaction::global_epochs.run = false;
            advancer.join();
        }

        //std::cout<<"Cleaning up RCU set items\n";

        // Clean up all remnant RCU set items.
        for (int i = 0; i < num_threads; ++i) {
            Transaction::tinfo[i].rcu_set.release_all();
        }

        if(logging==1) 
            logset::free(logs);
        else if(logging==2)
            logset_tbl<9>::free(logs_tbl);

        //std::cout<<"Done!\n";
        return 0;
    }
}; // class tpcc_access

};

#include "TPCH_queries.hh"
