#pragma once


namespace tpcc {
    template <typename DBParams>
    class tpcc_db;
    class tpcc_input_generator;
    template <typename DBParams>
    class tpcc_access;
}


namespace tpch {

class tpch_db {

    uint64_t num_whs_;

    template <typename K, typename V>
    using OIndex = bench::ordered_index<K, V, db_params::db_default_params>;
    template <typename K, typename V>
    using UIndex = bench::unordered_index<K, V, db_params::db_default_params>;

    typedef OIndex<tpcc::order_key, tpcc::order_value>               od_table_type;
    typedef OIndex<tpcc::orderline_key, tpcc::orderline_value>       ol_table_type;
    typedef UIndex<tpcc::customer_key, tpcc::customer_value>         cu_table_type;
    typedef OIndex<tpcc::order_key, bench::dummy_row>                no_table_type;



    // secondary index for <o_entry_d, order_value*>
    typedef OIndex<tpcc::order_sec_key, tpcc::order_sec_value>       od_sec_entry_d_type;
    typedef OIndex<tpcc::orderline_sec_key, tpcc::orderline_sec_value> ol_sec_deliv_d_type;


    std::vector<od_table_type> tbl_ods_;
    std::vector<ol_table_type> tbl_ols_;
    std::vector<cu_table_type> tbl_cus_;
    std::vector<no_table_type> tbl_nos_;
    // secondary indexes
    od_sec_entry_d_type tbl_sec_ods_;
    ol_sec_deliv_d_type tbl_sec_ols_;


    std::unordered_map<uint32_t, int> q4_date_ods; // the orders that fall within a specified date.
    std::mutex date_ods_lock;

    public:
    od_table_type& tbl_orders(uint64_t w_id){
        return tbl_ods_[w_id - 1];
    }
    ol_table_type& tbl_orderlines(uint64_t w_id){
        return tbl_ols_[w_id - 1];
    }
    od_sec_entry_d_type& tbl_sec_orders(){
        return tbl_sec_ods_;
    }
    ol_sec_deliv_d_type& tbl_sec_orderlines(){
        return tbl_sec_ols_;
    }

    cu_table_type& tbl_customers(uint64_t w_id){
        return tbl_cus_[w_id-1];
    }

    no_table_type& tbl_neworders(uint64_t w_id){
        return tbl_nos_[w_id-1];
    }


    void thread_init_all(){
        for(auto& t : tbl_ods_){
            t.thread_init();
        }
        for(auto& t : tbl_ols_){
            t.thread_init();
        }
        for(auto& t : tbl_cus_){
            t.thread_init();
        }
        for(auto& t : tbl_nos_){
            t.thread_init();
        }
        tbl_sec_ods_.thread_init();
        tbl_sec_ols_.thread_init();
    }

    tpch_db(int num_whs): num_whs_(num_whs),
    tbl_sec_ods_(num_whs * 999983/*num_customers * 10 * 2*/),
    tbl_sec_ols_(num_whs * 999983/*num_customers * 100 * 2*/)
     {
        // initialize the OLAP DB
        for(int i=0; i<num_whs; i++){
            tbl_ods_.emplace_back(999983/*num_customers * 10 * 2*/);
            tbl_ols_.emplace_back(999983/*num_customers * 100 * 2*/);
            tbl_cus_.emplace_back(999983/*num_customers * 100 * 2*/);
            tbl_nos_.emplace_back(999983/*num_customers * 100 * 2*/);
        }


        #if DICTIONARY == 3
        for(int i=0; i<MAX_TPCH_THREADS; i++)
            dict[i] = std::unordered_map<std::string, int> (INIT_DICT_SZ);
        #elif DICTIONARY == 4
        for(int i=0; i<MAX_TPCH_THREADS; i++)
            dict[i] = dict_t (INIT_DICT_SZ);
        #endif
    }
    inline ~tpch_db(){}
    uint64_t num_warehouses(){
        return num_whs_;
    }

    #if DICTIONARY == 3
    std::unordered_map<std::string, int> dict [MAX_TPCH_THREADS];
    #elif DICTIONARY == 4
    struct StrId {
        enum class NamedColumn : int { 
            id = 0
        };
        StrId(int id) : id_(id) {}
        int id_;
    };
    using dict_t = bench::unordered_index<std::string, StrId, db_params::db_default_params>;
    dict_t dict[MAX_TPCH_THREADS];
    #endif

    uint64_t db_size(){
        uint64_t db_size_total = 0;
        for (uint64_t i=1; i<=num_whs_; i++){
            db_size_total += tbl_orders(i).table_size();
            db_size_total += tbl_orderlines(i).table_size();
        }
        db_size_total += tbl_sec_orders().table_size();
        return db_size_total;
    }

    uint64_t db_num_elems(){
        uint64_t db_num_elems_total = 0;
        for (uint64_t i=1; i<=num_whs_; i++){
            db_num_elems_total += tbl_orders(i).table_num_elems();
            db_num_elems_total += tbl_orderlines(i).table_num_elems();
        }
        db_num_elems_total += tbl_sec_orders().table_num_elems();
        return db_num_elems_total;
    }

    void add_date(uint32_t date){
        date_ods_lock.lock();
        if(q4_date_ods.count(date) == 0)
            q4_date_ods[date] = 1;
        else
            q4_date_ods[date]++;
        date_ods_lock.unlock();
    }

    void q4_stats(){
        std::cout<<"Q.4 orders: \n";
        for(auto& od : q4_date_ods){
            std::cout<<od.second<<std::endl;
        }
    }

};

template <typename DBParams>
class tpch_runner {
public:
    // the database will be either hybrid_db (single_node == true), or tpch_db (false).
    tpch_runner(int id, tpcc::tpcc_input_generator& ig, void* database): db(database), runner_id(id), ig(ig) { }

    // finds the next query to be run for this thread (round-robin) and runs it
    void run_next_query();

    void run_query4();

private:
    void* db;
    int runner_id;
    tpcc::tpcc_input_generator &ig;
    static constexpr bool single_node = TPCH_SINGLE_NODE;
    friend class tpcc::tpcc_access<DBParams>;

    

    static constexpr bool Commute = DBParams::Commute;
    enum class query_type : int {
        Q1=1,  Q2,  Q3,  Q4,  Q5,  Q6,
        Q7,    Q8,  Q9,  Q10, Q11, Q12,
        Q13,   Q14, Q15, Q16, Q17, Q18,
        Q19,   Q20, Q21, Q22
    };
};

};