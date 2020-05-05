#pragma once

// in order to use the TPC-C related structs

#define RUN_OCC 1

// from Transaction.hh
static constexpr unsigned tset_max_capacity = 32768;

#define LATE_MATERIALIZATION 0  // whether to fetch value from internal_elem of orders directly, or later on.
                                // Adding the internal_elem* in the struct result_cols has some overhead and it is 
                                // better to fetch the values directly instead of storing the internal_elem*!!

//STARTDATE = 1992-01-01
//CURRENTDATE = 1995-06-17
//ENDDATE = 1998-12-31
// total days:
// leap years: 1992, 1996
// 365*7 + 2 = 2,557
#if RUN_TPCH 

namespace tpch {

    /**********
    * QUERY 4 *
    ***********/
    template <typename DBParams>
    void tpch_runner<DBParams>::run_query4(){
        INIT_COUNTING
        typedef tpcc::order_sec_key::oid_type oid_type;
        typedef tpcc::order_sec_key::wdid_type wdid_type;

        //typedef typename std::remove_reference_t<decltype(db.tbl_sec_orders())>::internal_elem internal_elem;
        #if RUN_OCC
        typedef bench::ordered_index<tpcc::order_key, tpcc::order_value, db_params::db_default_params>::internal_elem internal_elem;
        #else
        typedef bench::mvcc_ordered_index<tpcc::order_key, tpcc::order_value, db_params::db_mvcc_params>::internal_elem internal_elem;
        #endif
        

        // a struct holding the required columns for the final result
        struct result_cols {
            internal_elem* el; // the pointer to the primary index of orders holding the entire row
            uint64_t o_entry_d;
            uint64_t o_ol_cnt;
            wdid_type o_wid;
            wdid_type o_did;
            oid_type o_id;

            result_cols(internal_elem* e, uint64_t entry_d, uint64_t ol_cnt, wdid_type w, wdid_type d, oid_type id) : 
                                        el(e), o_entry_d(entry_d), o_ol_cnt(ol_cnt), o_wid(w), o_did(d), o_id(id) {}
            result_cols(uint64_t entry_d, uint64_t ol_cnt, wdid_type w, wdid_type d, oid_type id) : 
                                        o_entry_d(entry_d), o_ol_cnt(ol_cnt), o_wid(w), o_did(d), o_id(id) {}
        };
        


        // holding all orders with entry dates between specified range
        vector<result_cols> intermediate_res;
        // holding all orders with ol_delivery_d >= o_entry_d and the previous entry date range
        vector<result_cols> result;
        bool exists = false; // whether exists an orderline for a given order that satisfies the query condition
        bool retried = false;
        uint64_t cur_o_entry_d=0; // the entry date used to filter the orders in filter_by_delivery_d callback

        // Two options:
        // a) get the o_ol_cnt later on that we further filter the orders based on the final predicates and this will
        //  save us from a few internal_elem*  dereferences that would be random memory access!
        // b) get o_ol_cnt now since we already scan the secondary orders table!
        auto add_entry_d_callback = [&intermediate_res] (const tpcc::order_sec_key& ok, const tpcc::order_sec_value& ov) -> bool {
            // read the value from the primary index through the internal_elem that is stored as value in the secondary index
            // we can fetch the orderline_count value later on! (Late materialization)
            internal_elem* e = reinterpret_cast<internal_elem*>(ov.o_c_entry_d_p);
            #if !LATE_MATERIALIZATION
                #if RUN_OCC
                auto vp = &e->row_container.row;
                #else
                auto *h = e->row.find(Sto::read_tid<DBParams::Commute>());
                if (h->status_is(UNUSED)){
                    always_assert(false, "history status is UNUSED!");
                }
                auto vp = h->vp();
                #endif
            
            intermediate_res.emplace_back(result_cols(bswap(ok.o_entry_d), vp->o_ol_cnt, bswap(ok.o_w_id), bswap(ok.o_d_id), bswap(ok.o_id)));
            #else
            intermediate_res.emplace_back(result_cols(e, bswap(ok.o_entry_d), 0, bswap(ok.o_w_id), bswap(ok.o_d_id), bswap(ok.o_id)));
            #endif
            return true;
        };

        auto filter_by_delivery_d = [&result, &cur_o_entry_d, &exists] (const tpcc::orderline_key&, const tpcc::orderline_value&olv) -> bool {
            if(olv.ol_delivery_d >= cur_o_entry_d) // condition is satisfied! An order that statisfies the condition exists!
                exists = true;
            return true;
        };

        auto max_oid = std::numeric_limits<oid_type>::max();
        auto max_wdid = std::numeric_limits<wdid_type>::max();


        // DATE is the first day of a randomly selected month between the first month of 1993 and the 10th month of 1997
        // [1993-01-01 - 1997-10-01]
        // 1993-01-01 - 367 days from start date - 14.3% of total days
        // 1997-10-01: 2101 days from start date - 82.1% of total days
        // That's [14.3% - 82.1%]
        uint32_t start_date = ig.gen_date(0.143*ig.get_total_dates()+ig.get_min_date(), 0.821*ig.get_total_dates() + ig.get_min_date());
        uint32_t end_date = start_date + 0.035*ig.get_total_dates(); // start_date + 3 months (90 days - 3.5% of all days)
        //std::cout<<"Start date: "<<start_date<<", end date: "<<end_date<<std::endl;
        TXN{
            // clear the previous intermediate_res if it's a retry!
            if(retried){
                intermediate_res.clear();
                result.clear();
                exists = false;
            }
            tpcc::order_sec_key k0(start_date, 0, 0, 0);
            tpcc::order_sec_key k1(end_date, max_wdid, max_wdid, max_oid);
            START_COUNTING
            bool success = db.tbl_sec_orders()
                    .template range_scan<decltype(add_entry_d_callback), false/*no reverse*/>(true /*read-only access - no TItems*/, k0, k1, add_entry_d_callback, tpcc::RowAccess::None, false /* No Phantom protection*/);
            STOP_COUNTING(latencies_sec_ord_scan, ((runner_id-40) / 4))
            retried = true;
            CHK(success);

            // step 2 - for each encountered order, check whether ol_delivery_d >= o_entry_d
            for(auto order : intermediate_res){
                // get all orderlines with that order id from orderline table (any orderline number: 0-max)
                tpcc::orderline_key k0(order.o_wid, order.o_did, order.o_id, 0);
                tpcc::orderline_key k1(order.o_wid, order.o_did, order.o_id, std::numeric_limits<uint64_t>::max());
                // stop as soon as we find at least one order that satisfies the condition!
                // TODO: Make range_scan to stop when a condition is satisifed!
                cur_o_entry_d = order.o_entry_d;
                START_COUNTING
                bool success = db.tbl_orderlines(order.o_wid)
                                .template range_scan<decltype(filter_by_delivery_d), false> (true /*read-only access - no TItems*/, k0, k1, filter_by_delivery_d, tpcc::RowAccess::None, false /*No Phantom protection*/);
                STOP_COUNTING(latencies_orderline_scan, ((runner_id-40) / 4))
                retried = true;
                CHK(success);
                if(exists){
                    #if LATE_MATERIALIZATION
                        #if RUN_OCC
                        auto vp = e->row_container.row;
                        #else
                        auto *h = order.el->row.find(Sto::read_tid<DBParams::Commute>());
                        if (h->status_is(UNUSED)){
                            always_assert(false, "history status is UNUSED!");
                        }
                        auto vp = h->vp();
                        #endif
                    order.o_ol_cnt = vp->o_ol_cnt;
                    #endif
                    result.emplace_back(order);
                }
            }

        }TEND(true);
        //std::cout<< "Found "<< intermediate_res.size() <<" intermediate results and "<< result.size() <<" final\n";
    }

    template <typename DBParams>
    void tpch_runner<DBParams>::run_next_query(){
        // for now run query 4
        query_type q = query_type::Q4;

        switch(q){
            case query_type::Q4:
                run_query4();
                break;
            default:
                std::cout<<"Error: Query " << static_cast<typename std::underlying_type<query_type>::type>(q) << " not supported\n";
                break;
        }
    }

}

#endif