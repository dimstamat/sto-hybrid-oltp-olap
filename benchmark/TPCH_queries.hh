#pragma once

// from Transaction.hh
static constexpr unsigned tset_max_capacity = 32768;

#define LATE_MATERIALIZATION 0  // whether to fetch value from internal_elem of orders directly, or later on.
                                // Adding the internal_elem* in the struct result_cols has some overhead and it is 
                                // better to fetch the values directly instead of storing the internal_elem*!!

#define EXECUTE_QUERY_PER_ELEMENT 1

//STARTDATE = 1992-01-01
//CURRENTDATE = 1995-06-17
//ENDDATE = 1998-12-31
// total days:
// leap years: 1992, 1996
// 365*7 + 2 = 2,557

namespace tpch {

    /**********
    * QUERY 1 *
    ***********/
   template <typename DBParams>
    void tpch_runner<DBParams>::query1::run_query(uint64_t epoch){
        /* TPC-H predicate is different - use the one from CHBench */
       // 2007 from CHBench corresponds to 1993, if we look at Q.4
       // 1993-01-01 - 367 days from start date - 14.3% of total days
       (void)epoch;
        uint32_t start_date = this->runner->ig.gen_date(0.143*this->runner->ig.get_total_dates()+this->runner->ig.get_min_date(), this->runner->ig.get_total_dates());
        (void)start_date;
    }

    
    /**********
    * QUERY 4 *
    ***********/
    template <typename DBParams>
    void tpch_runner<DBParams>::query4::run_query(uint64_t epoch){
        (void)epoch;
        // holding all orders with ol_delivery_d >= o_entry_d and the previous entry date range
        vector<result_cols_q4> result;
        //INIT_COUNTING_PRINT
        //typedef typename std::remove_reference_t<decltype(db.tbl_sec_orders())>::internal_elem internal_elem;
        #if TPCH_SINGLE_NODE
            typedef bench::mvcc_ordered_index<tpcc::order_key, tpcc::order_value, db_params::db_mvcc_params>::internal_elem internal_elem;
        #else
            typedef bench::ordered_index<tpcc::order_key, tpcc::order_value, db_params::db_default_params>::internal_elem internal_elem;
        #endif
        //int scan_elems_n = 0; // how many elements found during scan - required for statistics

        // holding all orders with entry dates between specified range
        vector<result_cols_q4> intermediate_res;
        
        bool exists = false; // whether exists an orderline for a given order that satisfies the query condition
        bool retried = false;
        uint64_t cur_o_entry_d=0; // the entry date used to filter the orders in filter_by_delivery_d callback


        #if !EXECUTE_QUERY_PER_ELEMENT
        // Two options:
        // a) get the o_ol_cnt later on that we further filter the orders based on the final predicates and this will
        //  save us from a few internal_elem*  dereferences that would be random memory access!
        // b) get o_ol_cnt now since we already scan the secondary orders table!
        // TODO: order_sec_value was const.
        auto add_entry_d_callback = [&intermediate_res] (const tpcc::order_sec_key& ok, tpcc::order_sec_value& ov) -> bool {
            // read the value from the primary index through the internal_elem that is stored as value in the secondary index
            // we can fetch the orderline_count value later on! (Late materialization)
            internal_elem* e = reinterpret_cast<internal_elem*>(ov.o_c_entry_d_p);
            #if !LATE_MATERIALIZATION
                #if TPCH_SINGLE_NODE
                    auto *h = e->row.find(Sto::read_tid<DBParams::Commute>());
                    //auto *h = e->row.find(Sto::read_tid<DBParams::Commute>(), false); // it is not correct to not wait for PENDING! This can result to some elements from the future and some from the past! (different snapshots)
                    if (h->status_is(UNUSED)){
                        always_assert(false, "history status is UNUSED!");
                    }
                    auto vp = h->vp();
                #else
                    auto vp = &e->row_container.row;
                #endif
                intermediate_res.emplace_back(bswap(ok.o_entry_d), vp->o_ol_cnt, bswap(ok.o_w_id), bswap(ok.o_d_id), bswap(ok.o_id));
                #if EXTENDED_SEC_INDEX
                //if(ov.query_result == 0)
                //    ov.query_result = 2;
                #endif
            #else
            intermediate_res.emplace_back(e, bswap(ok.o_entry_d), 0, bswap(ok.o_w_id), bswap(ok.o_d_id), bswap(ok.o_id));
            #endif
            //scan_elems_n++;
            return true;
        };
        #endif

        auto ig = this->runner->ig;
        auto * db = this->runner->db;

        auto filter_by_delivery_d = [&cur_o_entry_d, &exists, this] (const tpcc::orderline_key&, const tpcc::orderline_value&olv) -> bool {
            if(olv.ol_delivery_d >= cur_o_entry_d){ // condition is satisfied! An order that statisfies the condition exists!
                exists = true;
                //#if DICTIONARY == 1
                //const char* str = tpcc::tpcc_db<DBParams>::decode(olv.ol_dist_info);
                //#endif
            }
            //scan_elems_n++;
            return true;
        };

        auto execute_query_per_elem = [db, &result, &filter_by_delivery_d, &cur_o_entry_d, &exists, &epoch, this] (const tpcc::order_sec_key& ok, tpcc::order_sec_value& ov) -> bool {
            //if(epoch != ov.epoch)
            //    std::cout<<"Current epoch: "<< epoch <<", cached epoch: "<< ov.epoch<<"!\n";
            #if EXTENDED_SEC_INDEX
            // we invalidated the affected elements, so no need to check for epoch!
            #if INVALIDATE
            //if(ov.query_result == 1) // does not satisfy
            if(ov.query_result.load(std::memory_order_acquire) == 1) // does not satisfy
                return true;
            #else
            //if(epoch == ov.epoch && ov.query_result == 1) // does not satisfy
            if(epoch == ov.epoch && ov.query_result.load(std::memory_order_acquire) == 1) // does not satisfy
                return true;
            #endif
            #endif

            internal_elem* e = reinterpret_cast<internal_elem*>(ov.o_c_entry_d_p);
            #if TPCH_SINGLE_NODE
                auto *h = e->row.find(Sto::read_tid<DBParams::Commute>());
                //auto *h = e->row.find(Sto::read_tid<DBParams::Commute>(), false); // it is not correct to not wait for PENDING! This can result to some elements from the future and some from the past! (different snapshots)
                if (h->status_is(UNUSED)){
                    always_assert(false, "history status is UNUSED!");
                }
                auto vp = h->vp();
            #else
                auto vp = &e->row_container.row;
            #endif
            result_cols_q4 order = result_cols_q4(bswap(ok.o_entry_d), vp->o_ol_cnt, bswap(ok.o_w_id), bswap(ok.o_d_id), bswap(ok.o_id));
            #if EXTENDED_SEC_INDEX
            // we invalidated the affected elements, so no need to check for epoch!
            #if INVALIDATE
            //if (ov.query_result == 2){ // satisfies!
            if(ov.query_result.load(std::memory_order_acquire) == 2){ // does not satisfy
            #else
            //if (epoch == ov.epoch && ov.query_result == 2){ // satisfies!
            if (epoch == ov.epoch && ov.query_result.load(std::memory_order_acquire) == 2){ // satisfies!
            #endif
                result.emplace_back(std::move(order));
                return true;
            }
            #endif
            

            // get all orderlines with that order id from orderline table (any orderline number: 0-max)
            tpcc::orderline_key k0(order.o_wid, order.o_did, order.o_id, 0);
            tpcc::orderline_key k1(order.o_wid, order.o_did, order.o_id, std::numeric_limits<uint64_t>::max());
            // stop as soon as we find at least one order that satisfies the condition!
            // TODO: Make range_scan to stop when a condition is satisifed!
            cur_o_entry_d = order.o_entry_d;
            //START_COUNTING_PRINT
            bool success = false;
            //bool exists = false;
            exists = false;
            #if TPCH_SINGLE_NODE
                success = (reinterpret_cast<tpcc::hybrid_db<DBParams>*>(db))->tbl_orderlines(order.o_wid)
                            .template range_scan<decltype(filter_by_delivery_d), false> (true /*read-only access - no TItems*/, k0, k1, filter_by_delivery_d, tpcc::RowAccess::None, false /*No Phantom protection*/);
            #else
            success = (reinterpret_cast<tpch_db*>(db))->tbl_orderlines(order.o_wid)
            .template nontrans_range_scan<decltype(filter_by_delivery_d), false/*no reverse*/>(k0, k1, filter_by_delivery_d);
            #endif
            #if TPCH_SINGLE_NODE
            if(!success)
                return false;
            #else
            assert(success);
            #endif
            if(exists) {
                result.emplace_back(std::move(order));
                #if EXTENDED_SEC_INDEX
                #if !INVALIDATE
                ov.epoch = epoch;
                #endif
                //ov.query_result = 2;
                // faster!
                ov.query_result.store(2, std::memory_order_release);
                #endif
            }
            else {
                #if EXTENDED_SEC_INDEX
                #if !INVALIDATE
                ov.epoch = epoch;
                #endif
                //ov.query_result = 1;
                // faster!
                ov.query_result.store(1, std::memory_order_release);
                #endif
            }
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
        const tpcc::order_sec_key k0(start_date, 0, 0, 0);
        const tpcc::order_sec_key k1(end_date, max_wdid, max_wdid, max_oid);

        #if EXECUTE_QUERY_PER_ELEMENT
        #if TPCH_SINGLE_NODE
        TXN{
        #endif
        if(retried){
            result.clear();
        }
        bool success = false;
        #if TPCH_SINGLE_NODE
        success = (reinterpret_cast<tpcc::hybrid_db<DBParams>*>(db))->tbl_sec_orders()
                .template range_scan<decltype(execute_query_per_elem), false/*no reverse*/>(true /*read-only access - no TItems*/, k0, k1, execute_query_per_elem, tpcc::RowAccess::None, false /* No Phantom protection*/);
        #else
        success = (reinterpret_cast<tpch_db*>(db))->tbl_sec_orders()
                .template nontrans_range_scan<decltype(execute_query_per_elem), false/*no reverse*/>(k0, k1, execute_query_per_elem);
        #endif
        retried = true;
        #if TPCH_SINGLE_NODE
        CHK(success);
        #else
        assert(success);
        #endif
        #if TPCH_SINGLE_NODE
        }TEND(true);
        #endif

        #else
        
        #if TPCH_SINGLE_NODE
        TXN{
        #endif
            // clear the previous intermediate_res if it's a retry!
            if(retried){
                intermediate_res.clear();
                result.clear();
                exists = false;
                //scan_elems_n = 0;
            }
            
            //START_COUNTING_PRINT
            bool success = false;
            #if TPCH_SINGLE_NODE
            success = (reinterpret_cast<tpcc::hybrid_db<DBParams>*>(db))->tbl_sec_orders()
                    .template range_scan<decltype(add_entry_d_callback), false/*no reverse*/>(true /*read-only access - no TItems*/, k0, k1, add_entry_d_callback, tpcc::RowAccess::None, false /* No Phantom protection*/);
            #else
            success = (reinterpret_cast<tpch_db*>(db))->tbl_sec_orders()
                    .template nontrans_range_scan<decltype(add_entry_d_callback), false/*no reverse*/>(k0, k1, add_entry_d_callback);
            #endif
            //STOP_COUNTING(latencies_sec_ord_scan, ((runner_id-40) / 4))
            //STOP_COUNTING_PRINT(tpcc::print_mutex, "orders", scan_elems_n)
            //scan_elems_n=0;
            retried = true;
            #if TPCH_SINGLE_NODE
            CHK(success);
            #else
            assert(success);
            #endif

            // step 2 - for each encountered order, check whether ol_delivery_d >= o_entry_d
            for(auto order : intermediate_res){
                // get all orderlines with that order id from orderline table (any orderline number: 0-max)
                tpcc::orderline_key k0(order.o_wid, order.o_did, order.o_id, 0);
                tpcc::orderline_key k1(order.o_wid, order.o_did, order.o_id, std::numeric_limits<uint64_t>::max());
                // stop as soon as we find at least one order that satisfies the condition!
                // TODO: Make range_scan to stop when a condition is satisifed!
                cur_o_entry_d = order.o_entry_d;
                //START_COUNTING_PRINT
                bool success = false;
                #if TPCH_SINGLE_NODE
                    success = (reinterpret_cast<tpcc::hybrid_db<DBParams>*>(db))->tbl_orderlines(order.o_wid)
                                .template range_scan<decltype(filter_by_delivery_d), false> (true /*read-only access - no TItems*/, k0, k1, filter_by_delivery_d, tpcc::RowAccess::None, false /*No Phantom protection*/);
                #else
                    success = (reinterpret_cast<tpch_db*>(db))->tbl_orderlines(order.o_wid)
                    .template nontrans_range_scan<decltype(filter_by_delivery_d), false/*no reverse*/>(k0, k1, filter_by_delivery_d);
                #endif

                //STOP_COUNTING_PRINT(tpcc::print_mutex, "orderlines", scan_elems_n)
                //STOP_COUNTING(latencies_orderline_scan, ((runner_id-40) / 4))
                retried = true;
                #if TPCH_SINGLE_NODE
                CHK(success);
                #else
                assert(success);
                #endif
                if(exists){
                    #if LATE_MATERIALIZATION
                        #if TPCH_SIGLE_NODE
                        auto *h = order.el->row.find(Sto::read_tid<DBParams::Commute>());
                        if (h->status_is(UNUSED)){
                            always_assert(false, "history status is UNUSED!");
                        }
                        auto vp = h->vp();
                        order.o_ol_cnt = vp->o_ol_cnt;
                        #else
                        auto vp = order.el->row_container.row;
                        order.o_ol_cnt = vp.o_ol_cnt;
                        #endif
                    #endif
                    result.emplace_back(std::move(order));
                }
            }

        #if TPCH_SINGLE_NODE
        }TEND(true);
        #endif
        //std::cout<< "Found "<< intermediate_res.size() <<" intermediate results and "<< result.size() <<" final\n";
        
        // materialized views experiment
        //for (uint32_t i=start_date; i <= end_date; i++)
        //    if(i%1000 == 0)
        //        (reinterpret_cast<tpch_db*>(db))->add_date(i);
        #endif
        //std::cout<< "Found "<< result.size() <<"\n";
    }


    template <typename DBParams>
    void tpch_runner<DBParams>::run_next_query(uint64_t epoch){
        //std::cout<<"Q index: "<< current_query_ind<<std::endl;
        queries[current_query_ind]->run_query(epoch);
        current_query_ind = (current_query_ind + 1) % queries.size();
    }

}
