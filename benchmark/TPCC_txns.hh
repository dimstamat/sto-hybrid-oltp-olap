#pragma once

#include "TPCC_bench.hh"
#include <set>

#ifndef TPCC_OBSERVE_C_BALANCE
#define TPCC_OBSERVE_C_BALANCE 1
#endif

namespace tpcc {

template <typename DBParams>
void tpcc_runner<DBParams>::run_txn_neworder() {
#if TABLE_FINE_GRAINED
    typedef warehouse_value::NamedColumn wh_nc;
    typedef district_value::NamedColumn dt_nc;
    typedef customer_value::NamedColumn cu_nc;
    typedef stock_value::NamedColumn st_nc;
#endif

    uint64_t q_w_id  = ig.random(w_id_start, w_id_end);
    uint64_t q_d_id = ig.random(1, 10);
    uint64_t q_c_id = ig.gen_customer_id();
    uint64_t num_items = ig.random(5, 15);
    //uint64_t rbk = ig.random(1, 100); //XXX no rollbacks

    uint64_t ol_i_ids[15];
    uint64_t ol_supply_w_ids[15];
    uint64_t ol_quantities[15];

    uint32_t o_entry_d = ig.gen_date();

    bool all_local = true;

    for (uint64_t i = 0; i < num_items; ++i) {
        uint64_t ol_i_id = ig.gen_item_id();
        //XXX no rollbacks
        //if ((i == (num_items - 1)) && rbk == 1)
        //    ol_i_ids[i] = 0;
        //else
        ol_i_ids[i] = ol_i_id;

        bool supply_from_remote = (ig.num_warehouses() > 1) && (ig.random(1, 100) == 1);
        uint64_t ol_s_w_id = q_w_id;
        if (supply_from_remote) {
            do {
                ol_s_w_id = ig.random(1, ig.num_warehouses());
            } while (ol_s_w_id == q_w_id);
            all_local = false;
        }
        ol_supply_w_ids[i] = ol_s_w_id;

        ol_quantities[i] = ig.random(1, 10);
    }

    // holding outputs of the transaction
    volatile var_string<16> out_cus_last;
    volatile fix_string<2> out_cus_credit;
    volatile var_string<24> out_item_names[15];
    volatile double out_total_amount = 0.0;
    volatile char out_brand_generic[15];
    (void) out_brand_generic;

    size_t starts = 0;

    // begin txn
    RWTXN {
    ++starts;

    bool abort, result;
    uintptr_t row;
    const void *value;

    int64_t wh_tax_rate, dt_tax_rate;
    uint64_t dt_next_oid;

    warehouse_key wk(q_w_id);
#if TPCC_SPLIT_TABLE
    std::tie(abort, result, row, value) = db.tbl_warehouses_const().select_row(wk, RowAccess::ObserveValue);
    CHK(abort);
    assert(result);
    wh_tax_rate = reinterpret_cast<const warehouse_const_value*>(value)->w_tax;
#else
    std::tie(abort, result, row, value) = db.tbl_warehouses().select_row(wk,
#if TABLE_FINE_GRAINED
        {{wh_nc::w_tax, access_t::read}}
#else
        RowAccess::ObserveValue
#endif
    );
    CHK(abort);
    assert(result);
    wh_tax_rate = reinterpret_cast<const warehouse_value*>(value)->w_tax;
#endif

#if TPCC_SPLIT_TABLE
    district_key dk(q_w_id, q_d_id);
    std::tie(abort, result, row, value) = db.tbl_districts_const(q_w_id).select_row(dk, RowAccess::ObserveValue);
    CHK(abort);
    assert(result);

    TXP_INCREMENT(txp_tpcc_no_stage1);

    auto dv = reinterpret_cast<const district_const_value*>(value);
    dt_tax_rate = dv->d_tax;
    dt_next_oid = db.oid_generator().next(q_w_id, q_d_id);
    //dt_next_oid = new_dv->d_next_o_id ++;
    //db.tbl_districts(q_w_id).update_row(row, new_dv);

    customer_key ck(q_w_id, q_d_id, q_c_id);
    std::tie(abort, result, std::ignore, value) = db.tbl_customers_const(q_w_id).select_row(ck, RowAccess::ObserveValue);
    CHK(abort);
    assert(result);

    TXP_INCREMENT(txp_tpcc_no_stage2);

    auto ccv = reinterpret_cast<const customer_const_value*>(value);

    auto cus_discount = ccv->c_discount;
    out_cus_last = ccv->c_last;
    out_cus_credit = ccv->c_credit;
#else
    district_key dk(q_w_id, q_d_id);
    std::tie(abort, result, row, value) = db.tbl_districts(q_w_id).select_row(dk,
#if TABLE_FINE_GRAINED
        {{dt_nc::d_tax, access_t::read}}
#else
        RowAccess::ObserveValue
#endif
    );
    CHK(abort);
    assert(result);

    TXP_INCREMENT(txp_tpcc_no_stage1);

    auto dv = reinterpret_cast<const district_value*>(value);
    dt_tax_rate = dv->d_tax;
    dt_next_oid = db.oid_generator().next(q_w_id, q_d_id);
    //dt_next_oid = new_dv->d_next_o_id ++;
    //db.tbl_districts(q_w_id).update_row(row, new_dv);

    customer_key ck(q_w_id, q_d_id, q_c_id);
    std::tie(abort, result, std::ignore, value) = db.tbl_customers(q_w_id).select_row(ck,
#if TABLE_FINE_GRAINED
        {{cu_nc::c_discount, access_t::read},
         {cu_nc::c_last, access_t::read},
         {cu_nc::c_credit, access_t::read}}
#else
        RowAccess::ObserveValue
#endif
    );
    CHK(abort);
    assert(result);

    TXP_INCREMENT(txp_tpcc_no_stage2);

    auto cv = reinterpret_cast<const customer_value*>(value);

    auto cus_discount = cv->c_discount;
    out_cus_last = cv->c_last;
    out_cus_credit = cv->c_credit;
#endif

    order_key ok(q_w_id, q_d_id, dt_next_oid);
    order_cidx_key ock(q_w_id, q_d_id, q_c_id, dt_next_oid);
#if TPCC_SPLIT_TABLE
    order_const_value* ocv = Sto::tx_alloc<order_const_value>();
    order_comm_value* omv = Sto::tx_alloc<order_comm_value>();
    ocv->o_c_id = q_c_id;
    omv->o_carrier_id = 0;
    ocv->o_all_local = all_local ? 1 : 0;
    ocv->o_entry_d = o_entry_d;
    ocv->o_ol_cnt = num_items;

    std::tie(abort, result) = db.tbl_orders_const(q_w_id).insert_row(ok, ocv, false);
    CHK(abort);
    assert(!result);

    std::tie(abort, result) = db.tbl_orders_comm(q_w_id).insert_row(ok, omv, false);
    CHK(abort);
    assert(!result);
#else
    order_value* ov = Sto::tx_alloc<order_value>();
    ov->o_c_id = q_c_id;
    ov->o_carrier_id = 0;
    ov->o_all_local = all_local ? 1 : 0;
    ov->o_entry_d = o_entry_d;
    ov->o_ol_cnt = num_items;

#if RUN_TPCH
    // get the pointer of the internal_elem that stores the given key!
    uint64_t* el;
    std::tie(abort, result) = db.tbl_orders(q_w_id).insert_row(ok, ov, false, false, true, &el);
    CHK(abort);
    assert(!result);
    // insert into the secondary index also
    order_sec_value * val = Sto::tx_alloc<order_sec_value>();
    // store the location of the internal element
    val->o_c_entry_d_p = el;
    std::tie(abort, result) = db.tbl_sec_orders().insert_row(order_sec_key(o_entry_d, q_w_id, q_d_id, dt_next_oid), val, false);
    CHK(abort);
    assert(!result);
#else
    std::tie(abort, result) = db.tbl_orders(q_w_id).insert_row(ok, ov, false);
    //std::cout<<"Should add log\n";
    CHK(abort);
    assert(!result);
#endif

#endif

    std::tie(abort, result) = db.tbl_neworders(q_w_id).insert_row(ok, &bench::dummy_row::row, false);
    CHK(abort);
    assert(!result);
    std::tie(abort, result) = db.tbl_order_customer_index(q_w_id).insert_row(ock, &bench::dummy_row::row, false);
    CHK(abort);
    assert(!result);

    TXP_INCREMENT(txp_tpcc_no_stage3);

    TXP_ACCOUNT(txp_tpcc_no_stage4, num_items);

    for (uint64_t i = 0; i < num_items; ++i) {
        uint64_t iid = ol_i_ids[i];
        uint64_t wid = ol_supply_w_ids[i];
        uint64_t qty = ol_quantities[i];

        std::tie(abort, result, std::ignore, value) = db.tbl_items().select_row(item_key(iid), RowAccess::ObserveValue);
        CHK(abort);
        assert(result);
        uint64_t oid = reinterpret_cast<const item_value *>(value)->i_im_id;
        CHK(oid != 0);
        uint32_t i_price = reinterpret_cast<const item_value *>(value)->i_price;
        out_item_names[i] = reinterpret_cast<const item_value *>(value)->i_name;
        //auto i_data = reinterpret_cast<const item_value *>(value)->i_data;

#if TPCC_SPLIT_TABLE
        std::tie(abort, result, row, value) = db.tbl_stocks_const(wid).select_row(stock_key(wid, iid), RowAccess::ObserveValue);
        CHK(abort);
        assert(result);
        auto scv = reinterpret_cast<const stock_const_value*>(value);
        auto s_dist = scv->s_dists[q_d_id - 1];
        //auto s_data = scv->s_data;
        //if (i_data.contains("ORIGINAL") && s_data.contains("ORIGINAL"))
        //    out_brand_generic[i] = 'B';
        //else
        //    out_brand_generic[i] = 'G';

        std::tie(abort, result, row, value) = db.tbl_stocks_comm(wid).select_row(stock_key(wid, iid),
            Commute ? RowAccess::None : RowAccess::ObserveValue);
        CHK(abort);
        assert(result);
        if (Commute) {
            commutators::Commutator<stock_comm_value> comm(qty, wid != q_w_id);
            db.tbl_stocks_comm(wid).update_row(row, comm);
        } else {
            auto new_smv = Sto::tx_alloc(reinterpret_cast<const stock_comm_value*>(value));
            if ((new_smv->s_quantity - 10) >= (int32_t)qty)
                new_smv->s_quantity -= qty;
            else
                new_smv->s_quantity += (91 - (int32_t)qty);
            new_smv->s_ytd += qty;
            new_smv->s_order_cnt += 1;
            if (wid != q_w_id)
                new_smv->s_remote_cnt += 1;
            db.tbl_stocks_comm(wid).update_row(row, new_smv);
        }
#else
        std::tie(abort, result, row, value) = db.tbl_stocks(wid).select_row(stock_key(wid, iid),
#if TABLE_FINE_GRAINED
            {{st_nc::s_quantity, Commute ? access_t::write : access_t::update},
             {st_nc::s_ytd, Commute ? access_t::write : access_t::update},
             {st_nc::s_order_cnt, Commute ? access_t::write : access_t::update},
             {st_nc::s_remote_cnt, Commute ? access_t::write : access_t::update},
             {st_nc::s_dists, access_t::read },
             {st_nc::s_data, access_t::read }}
#else
            RowAccess::ObserveValue
#endif
        );
        CHK(abort);
        assert(result);
        auto sv = reinterpret_cast<const stock_value*>(value);
        int32_t s_quantity = sv->s_quantity;
        auto s_dist = sv->s_dists[q_d_id - 1];
        //auto s_data = sv->s_data;
        //if (i_data.contains("ORIGINAL") && s_data.contains("ORIGINAL"))
        //    out_brand_generic[i] = 'B';
        //else
        //    out_brand_generic[i] = 'G';

        if (Commute) {
            commutators::Commutator<stock_value> comm(qty, wid != q_w_id);
            db.tbl_stocks(wid).update_row(row, comm);
        } else {
            stock_value *new_sv = Sto::tx_alloc(sv);
            if ((s_quantity - 10) >= (int32_t) qty)
                new_sv->s_quantity -= qty;
            else
                new_sv->s_quantity += (91 - (int32_t) qty);
            new_sv->s_ytd += qty;
            new_sv->s_order_cnt += 1;
            if (wid != q_w_id)
                new_sv->s_remote_cnt += 1;
            db.tbl_stocks(wid).update_row(row, new_sv);
        }
#endif

        double ol_amount = qty * i_price/100.0;

        orderline_key olk(q_w_id, q_d_id, dt_next_oid, i + 1);
#if TPCC_SPLIT_TABLE
        orderline_const_value *lcv = Sto::tx_alloc<orderline_const_value>();
        orderline_comm_value *lmv = Sto::tx_alloc<orderline_comm_value>();
        lcv->ol_i_id = iid;
        lcv->ol_supply_w_id = wid;
        lmv->ol_delivery_d = 0;
        lcv->ol_quantity = qty;
        lcv->ol_amount = ol_amount;
        lcv->ol_dist_info = s_dist;

        std::tie(abort, result) = db.tbl_orderlines_const(q_w_id).insert_row(olk, lcv, false);
        CHK(abort);
        assert(!result);

        std::tie(abort, result) = db.tbl_orderlines_comm(q_w_id).insert_row(olk, lmv, false);
        CHK(abort);
        assert(!result);
#else
        orderline_value *olv = Sto::tx_alloc<orderline_value>();
        olv->ol_i_id = iid;
        olv->ol_supply_w_id = wid;
        olv->ol_delivery_d = 0;
        olv->ol_quantity = qty;
        olv->ol_amount = ol_amount;
        olv->ol_dist_info = s_dist;

    #if 0
        // get the pointer of the internal_elem that stores the given key!
        uint64_t* el;
        std::tie(abort, result) = db.tbl_orderlines(q_w_id).insert_row(olk, olv, false, false, true, &el);
        CHK(abort);
        assert(!result);
        // insert into the secondary index also
        orderline_sec_value * val = Sto::tx_alloc<orderline_sec_value>();
        // store the location of the internal element
        val->ol_c_delivery_d_p = el;
        std::tie(abort, result) = db.tbl_sec_orderlines().insert_row(orderline_sec_key(olv->ol_delivery_d, q_w_id, q_d_id, dt_next_oid, i+1), val, false);
    #endif
        std::tie(abort, result) = db.tbl_orderlines(q_w_id).insert_row(olk, olv, false);
        //std::cout<<"Should add log\n";
        CHK(abort);
        assert(!result);
#endif

        out_total_amount += ol_amount * (1.0 - cus_discount/100.0) * (1.0 + (wh_tax_rate + dt_tax_rate)/100.0);

        TXP_INCREMENT(txp_tpcc_no_stage5);
    }

    // commit txn
    // retry until commits
    } TEND(true);

    TXP_INCREMENT(txp_tpcc_no_commits);
    TXP_ACCOUNT(txp_tpcc_no_aborts, starts - 1);
}

template <typename DBParams>
void tpcc_runner<DBParams>::run_txn_payment() {
#if TABLE_FINE_GRAINED
    typedef warehouse_value::NamedColumn wh_nc;
    typedef district_value::NamedColumn dt_nc;
    typedef customer_value::NamedColumn cu_nc;
#endif

    uint64_t q_w_id = ig.random(w_id_start, w_id_end);
    uint64_t q_d_id = ig.random(1, 10);

    uint64_t q_c_w_id, q_c_d_id, q_c_id;
    std::string last_name;
    auto x = ig.random(1, 100);
    auto y = ig.random(1, 100);

    bool is_home = (ig.num_warehouses() == 1) || (x <= 85);
    bool by_name = (y <= 60);

    if (is_home) {
        q_c_w_id = q_w_id;
        q_c_d_id = q_d_id;
    } else {
        do {
            q_c_w_id = ig.random(1, ig.num_warehouses());
        } while (q_c_w_id == q_w_id);
        q_c_d_id = ig.random(1, 10);
    }

    if (by_name) {
        last_name = ig.gen_customer_last_name_run();
        q_c_id = 0;
    } else {
        q_c_id = ig.gen_customer_id();
    }

    int64_t h_amount = ig.random(100, 500000);
    uint32_t h_date = ig.gen_date();

    // holding outputs of the transaction
    volatile var_string<10> out_w_name, out_d_name;
    volatile var_string<20> out_w_street_1, out_w_street_2, out_w_city;
    volatile var_string<20> out_d_street_1, out_d_street_2, out_d_city;
    volatile fix_string<2> out_w_state, out_d_state;
    volatile fix_string<9> out_w_zip, out_d_zip;
    volatile var_string<16> out_c_first, out_c_last;
    volatile fix_string<2> out_c_middle;
    volatile var_string<20> out_c_street_1, out_c_street_2, out_c_city;
    volatile fix_string<2> out_c_state;
    volatile fix_string<9> out_c_zip;
    volatile fix_string<16> out_c_phone;
    volatile fix_string<2> out_c_credit;
    volatile uint32_t out_c_since;
    volatile int64_t out_c_credit_lim;
    volatile int64_t out_c_discount;
    volatile int64_t out_c_balance;
    (void)out_c_since;
    (void)out_c_credit_lim;
    (void)out_c_discount;
    (void)out_c_balance;

    size_t starts = 0;

    // begin txn
    RWTXN {
    Sto::transaction()->special_txp = true;
    ++starts;

    bool success, result;
    uintptr_t row;
    const void *value;

    // select warehouse row for update and retrieve warehouse info
#if TPCC_SPLIT_TABLE
    warehouse_key wk(q_w_id);
    std::tie(success, result, row, value) = db.tbl_warehouses_const().select_row(wk, RowAccess::ObserveValue);
    CHK(success);
    assert(result);
    auto wv = reinterpret_cast<const warehouse_const_value*>(value);

    out_w_name = wv->w_name;
    out_w_street_1 = wv->w_street_1;
    out_w_street_2 = wv->w_street_2;
    out_w_city = wv->w_city;
    out_w_state = wv->w_state;
    out_w_zip = wv->w_zip;

    std::tie(success, result, row, value) = db.tbl_warehouses_comm().select_row(wk,
            Commute ? RowAccess::None : RowAccess::UpdateValue);
    CHK(success);
    assert(result);

    if (Commute) {
        commutators::Commutator<warehouse_comm_value> commutator(h_amount);
        db.tbl_warehouses_comm().update_row(row, commutator);
    } else {
        auto wmv = reinterpret_cast<const warehouse_comm_value*>(value);
        auto new_wmv = Sto::tx_alloc(wmv);
        new_wmv->w_ytd += h_amount;
        db.tbl_warehouses_comm().update_row(row, new_wmv);
    }
#else
    warehouse_key wk(q_w_id);
    std::tie(success, result, row, value) = db.tbl_warehouses().select_row(wk,
#if TABLE_FINE_GRAINED
        {{wh_nc::w_name, access_t::read},
         {wh_nc::w_street_1, access_t::read},
         {wh_nc::w_street_2, access_t::read},
         {wh_nc::w_city, access_t::read},
         {wh_nc::w_state, access_t::read},
         {wh_nc::w_zip, access_t::read},
         {wh_nc::w_ytd, Commute ? access_t::write : access_t::update}}
#else
        RowAccess::ObserveValue
#endif
    );
    CHK(success);
    assert(result);
    auto wv = reinterpret_cast<const warehouse_value*>(value);
    out_w_name = wv->w_name;
    out_w_street_1 = wv->w_street_1;
    out_w_street_2 = wv->w_street_2;
    out_w_city = wv->w_city;
    out_w_state = wv->w_state;
    out_w_zip = wv->w_zip;

    // update warehouse ytd
    if (Commute) {
        commutators::Commutator<warehouse_value> commutator(h_amount);
        db.tbl_warehouses().update_row(row, commutator);
    } else {
        auto new_wv = Sto::tx_alloc(wv);
        new_wv->w_ytd += h_amount;
        db.tbl_warehouses().update_row(row, new_wv);
    }
#endif

    // select district row and retrieve district info
    district_key dk(q_w_id, q_d_id);
#if TPCC_SPLIT_TABLE
    std::tie(success, result, row, value) = db.tbl_districts_const(q_w_id).select_row(dk, RowAccess::ObserveValue);
    CHK(success);
    assert(result);
    auto dv = reinterpret_cast<const district_const_value *>(value);
    out_d_name = dv->d_name;
    out_d_street_1 = dv->d_street_1;
    out_d_street_2 = dv->d_street_2;
    out_d_city = dv->d_city;
    out_d_state = dv->d_state;
    out_d_zip = dv->d_zip;

    std::tie(success, result, row, value) = db.tbl_districts_comm(q_w_id).select_row(dk,
            Commute ? RowAccess::None : RowAccess::UpdateValue);
    CHK(success);
    assert(result);

    TXP_INCREMENT(txp_tpcc_pm_stage1);

    if (Commute) {
        // update district ytd commutatively
        commutators::Commutator<district_comm_value> commutator(h_amount);
        db.tbl_districts_comm(q_w_id).update_row(row, commutator);
    } else {
        auto dmv = reinterpret_cast<const district_comm_value*>(value);
        auto new_dmv = Sto::tx_alloc(dmv);
        // update district ytd in-place
        new_dmv->d_ytd += h_amount;
        db.tbl_districts_comm(q_w_id).update_row(row, new_dmv);
    }
#else
    std::tie(success, result, row, value) = db.tbl_districts(q_w_id).select_row(dk,
#if TABLE_FINE_GRAINED
        {{dt_nc::d_name, access_t::read},
         {dt_nc::d_street_1, access_t::read},
         {dt_nc::d_street_2, access_t::read},
         {dt_nc::d_city, access_t::read},
         {dt_nc::d_state, access_t::read},
         {dt_nc::d_zip, access_t::read},
         {dt_nc::d_ytd, Commute ? access_t::write : access_t::update}}
#else
        RowAccess::ObserveValue
#endif
    );
    CHK(success);
    assert(result);
    auto dv = reinterpret_cast<const district_value *>(value);
    out_d_name = dv->d_name;
    out_d_street_1 = dv->d_street_1;
    out_d_street_2 = dv->d_street_2;
    out_d_city = dv->d_city;
    out_d_state = dv->d_state;
    out_d_zip = dv->d_zip;

    TXP_INCREMENT(txp_tpcc_pm_stage1);

    if (Commute) {
        // update district ytd commutatively
        commutators::Commutator<district_value> commutator(h_amount);
        db.tbl_districts(q_w_id).update_row(row, commutator);
    } else {
        auto new_dv = Sto::tx_alloc(dv);
        // update district ytd in-place
        new_dv->d_ytd += h_amount;
        db.tbl_districts(q_w_id).update_row(row, new_dv);
    }
#endif

    TXP_INCREMENT(txp_tpcc_pm_stage2);

    // select and update customer
    if (by_name) {
        customer_idx_key ck(q_c_w_id, q_c_d_id, last_name);
        std::tie(success, result, row, value) = db.tbl_customer_index(q_c_w_id).select_row(ck, RowAccess::ObserveValue);
        CHK(success);
        assert(result);
        auto& c_id_list = reinterpret_cast<const customer_idx_value*>(value)->c_ids;
        uint64_t rows[100];
        int cnt = 0;
        for (auto it = c_id_list.begin(); cnt < 100 && it != c_id_list.end(); ++it, ++cnt) {
            rows[cnt] = *it;
        }
        q_c_id = rows[cnt / 2];
    } else {
        always_assert(q_c_id != 0, "q_c_id invalid when selecting customer by c_id");
    }

    TXP_INCREMENT(txp_tpcc_pm_stage3);

#if TPCC_SPLIT_TABLE
    customer_key ck(q_c_w_id, q_c_d_id, q_c_id);
    std::tie(success, result, row, value) = db.tbl_customers_const(q_c_w_id).select_row(ck, RowAccess::ObserveValue);
    CHK(success);
    assert(result);

    TXP_INCREMENT(txp_tpcc_pm_stage4);

    auto ccv = reinterpret_cast<const customer_const_value*>(value);
    out_c_since = ccv->c_since;
    out_c_credit_lim = ccv->c_credit_lim;
    out_c_discount = ccv->c_discount;

#if TPCC_OBSERVE_C_BALANCE
    std::tie(success, result, row, value) = db.tbl_customers_comm(q_c_w_id).select_row(ck, RowAccess::UpdateValue);
    CHK(success);
    assert(result);
    auto cmv = reinterpret_cast<const customer_comm_value*>(value);
    out_c_balance = cmv->c_balance;
#else
    std::tie(success, result, row, value) = db.tbl_customers_comm(q_c_w_id).select_row(ck,
            Commute ? RowAccess::None : RowAccess::UpdateValue);
    CHK(success);
    assert(result);
#endif

    if (Commute) {
        if (ccv->c_credit == "BC") {
            commutators::Commutator<customer_comm_value> commutator(-h_amount, h_amount, q_c_id, q_c_d_id, q_c_w_id,
                                                                      q_d_id, q_w_id, h_amount);
            db.tbl_customers_comm(q_c_w_id).update_row(row, commutator);
        } else {
            commutators::Commutator<customer_comm_value> commutator(-h_amount, h_amount);
            db.tbl_customers_comm(q_c_w_id).update_row(row, commutator);
        }
    } else {
        auto cmmv = reinterpret_cast<const customer_comm_value*>(value);
        auto new_cmmv = Sto::tx_alloc(cmmv);
        new_cmmv->c_balance -= h_amount;
        new_cmmv->c_payment_cnt += 1;
        new_cmmv->c_ytd_payment += h_amount;
        if (ccv->c_credit == "BC") {
            c_data_info info(q_c_id, q_c_d_id, q_c_w_id, q_d_id, q_w_id, h_amount);
            new_cmmv->c_data.insert_left(info.buf(), c_data_info::len);
        }
        db.tbl_customers_comm(q_c_w_id).update_row(row, new_cmmv);
    }
#else
    customer_key ck(q_c_w_id, q_c_d_id, q_c_id);
    std::tie(success, result, row, value) = db.tbl_customers(q_c_w_id).select_row(ck,
#if TABLE_FINE_GRAINED
        {{cu_nc::c_since,    access_t::read},
         {cu_nc::c_credit,   access_t::read},
         {cu_nc::c_discount, access_t::read},
         {cu_nc::c_balance, access_t::update},
         {cu_nc::c_payment_cnt, Commute ? access_t::write : access_t::update},
         {cu_nc::c_ytd_payment, Commute ? access_t::write : access_t::update},
         {cu_nc::c_credit, Commute ? access_t::write : access_t::update}}
#else
        RowAccess::ObserveValue
#endif
    );
    CHK(success);
    assert(result);

    TXP_INCREMENT(txp_tpcc_pm_stage4);

    auto cv = reinterpret_cast<const customer_value*>(value);
    out_c_since = cv->c_since;
    out_c_credit_lim = cv->c_credit_lim;
    out_c_discount = cv->c_discount;
    out_c_balance = cv->c_balance;

    if (Commute) {
        if (cv->c_credit == "BC") {
            commutators::Commutator<customer_value> commutator(-h_amount, h_amount, q_c_id, q_c_d_id, q_c_w_id,
                                                                      q_d_id, q_w_id, h_amount);
            db.tbl_customers(q_c_w_id).update_row(row, commutator);
        } else {
            commutators::Commutator<customer_value> commutator(-h_amount, h_amount);
            db.tbl_customers(q_c_w_id).update_row(row, commutator);
        }
    } else {
        auto new_cv = Sto::tx_alloc(cv);
        new_cv->c_balance -= h_amount;
        new_cv->c_payment_cnt += 1;
        new_cv->c_ytd_payment += h_amount;
        if (cv->c_credit == "BC") {
            c_data_info info(q_c_id, q_c_d_id, q_c_w_id, q_d_id, q_w_id, h_amount);
            new_cv->c_data.insert_left(info.buf(), c_data_info::len);
        }
        db.tbl_customers(q_c_w_id).update_row(row, new_cv);
    }
#endif

    TXP_INCREMENT(txp_tpcc_pm_stage5);

    // insert to history table
    history_value *hv = Sto::tx_alloc<history_value>();
    hv->h_c_id = q_c_id;
    hv->h_c_d_id = q_c_d_id;
    hv->h_c_w_id = q_c_w_id;
    hv->h_d_id = q_d_id;
    hv->h_w_id = q_w_id;
    hv->h_date = h_date;
    hv->h_amount = h_amount;
    hv->h_data = std::string(out_w_name.c_str()) + "    " + std::string(out_d_name.c_str());

#if HISTORY_SEQ_INSERT
    history_key hk(db.tbl_histories(q_c_w_id).gen_key());
#else
    history_key hk(q_w_id, q_d_id, q_c_id, db.tbl_histories(q_c_w_id).gen_key());
#endif
    std::tie(success, result) = db.tbl_histories(q_c_w_id).insert_row(hk, hv);
    assert(success);
    assert(!result);

    TXP_INCREMENT(txp_tpcc_pm_stage6);

    // commit txn
    // retry until commits
    } TEND(true);

    TXP_INCREMENT(txp_tpcc_pm_commits);
    TXP_ACCOUNT(txp_tpcc_pm_aborts, starts - 1);
}

template <typename DBParams>
void tpcc_runner<DBParams>::run_txn_orderstatus() {
#if TABLE_FINE_GRAINED
    typedef customer_value::NamedColumn cu_nc;
    typedef orderline_value::NamedColumn ol_nc;
#endif
    uint64_t q_w_id = ig.random(w_id_start, w_id_end);
    uint64_t q_d_id = ig.random(1, 10);

    std::string last_name;
    uint64_t q_c_id;

    auto x = ig.random(1, 100);
    bool by_name = (x <= 60);
    if (by_name) {
        last_name = ig.gen_customer_last_name_run();
        q_c_id = 0;
    } else {
        q_c_id = ig.gen_customer_id();
    }

    // holding outputs of the transaction
    volatile var_string<16> out_c_first, out_c_last;
    volatile fix_string<2> out_c_middle;
    volatile int64_t out_c_balance;
    volatile uint64_t out_o_carrier_id;
    volatile uint32_t out_o_entry_date;
    volatile uint64_t out_ol_i_id;
    volatile uint64_t out_ol_supply_w_id;
    volatile uint32_t out_ol_quantity;
    volatile uint32_t out_ol_delivery_d;
    volatile int32_t out_ol_amount;
    (void)out_c_balance;
    (void)out_o_carrier_id;
    (void)out_o_entry_date;

    size_t starts = 0;

    TXN {
    ++starts;

    bool success, result;
    uintptr_t row;
    const void *value;

    if (by_name) {
        customer_idx_key ck(q_w_id, q_d_id, last_name);
        std::tie(success, result, row, value) = db.tbl_customer_index(q_w_id).select_row(ck, RowAccess::ObserveValue);
        CHK(success);
        assert(result);
        auto& c_id_list = reinterpret_cast<const customer_idx_value*>(value)->c_ids;
        uint64_t rows[100];
        int cnt = 0;
        for (auto it = c_id_list.begin(); cnt < 100 && it != c_id_list.end(); ++it, ++cnt) {
            rows[cnt] = *it;
        }
        q_c_id = rows[cnt / 2];
    } else {
        always_assert(q_c_id != 0, "q_c_id invalid when selecting customer by c_id");
    }

#if TPCC_SPLIT_TABLE
    customer_key ck(q_w_id, q_d_id, q_c_id);
    std::tie(success, result, row, value) = db.tbl_customers_const(q_w_id).select_row(ck, RowAccess::ObserveValue);
    CHK(success);
    assert(result);

    auto ccv = reinterpret_cast<const customer_const_value*>(value);

    // simulate retrieving customer info
    out_c_first = ccv->c_first;
    out_c_last = ccv->c_last;
    out_c_middle = ccv->c_middle;

#if TPCC_OBSERVE_C_BALANCE
    std::tie(success, result, row, value) = db.tbl_customers_comm(q_w_id).select_row(ck, RowAccess::ObserveValue);
    CHK(success);
    assert(result);
    auto cmv = reinterpret_cast<const customer_comm_value*>(value);
    out_c_balance = cmv->c_balance;
#endif
#else
    customer_key ck(q_w_id, q_d_id, q_c_id);
    std::tie(success, result, row, value) = db.tbl_customers(q_w_id).select_row(ck,
#if TABLE_FINE_GRAINED
        {{cu_nc::c_first, access_t::read},
         {cu_nc::c_last, access_t::read},
         {cu_nc::c_middle, access_t::read},
         {cu_nc::c_balance, access_t::read}}
#else
        RowAccess::ObserveValue
#endif
    );
    CHK(success);
    assert(result);

    auto cv = reinterpret_cast<const customer_value*>(value);

    // simulate retrieving customer info
    out_c_first = cv->c_first;
    out_c_last = cv->c_last;
    out_c_middle = cv->c_middle;
    out_c_balance = cv->c_balance;
#endif

    // find the highest order placed by customer q_c_id
    uint64_t cus_o_id = 0;
    auto scan_callback = [&] (const order_cidx_key& key, const bench::dummy_row&) -> bool {
        cus_o_id = bswap(key.o_id);
        return true;
    };

    order_cidx_key k0(q_w_id, q_d_id, q_c_id, 0);
    order_cidx_key k1(q_w_id, q_d_id, q_c_id, std::numeric_limits<uint64_t>::max());

    success = db.tbl_order_customer_index(q_w_id)
            .template range_scan<decltype(scan_callback), true/*reverse*/>(k1, k0, scan_callback, RowAccess::ObserveExists, true, 1/*reverse scan for only 1 item*/);
    CHK(success);

    if (cus_o_id > 0) {
        order_key ok(q_w_id, q_d_id, cus_o_id);
#if TPCC_SPLIT_TABLE
        std::tie(success, result, std::ignore, value) = db.tbl_orders_const(q_w_id).select_row(ok, RowAccess::ObserveValue);
        CHK(success);
        assert(result);
        auto ocv = reinterpret_cast<const order_const_value*>(value);
        out_o_entry_date = ocv->o_entry_d;

        std::tie(success, result, std::ignore, value) = db.tbl_orders_comm(q_w_id).select_row(ok, RowAccess::ObserveValue);
        CHK(success);
        assert(result);
        auto omv = reinterpret_cast<const order_comm_value*>(value);
        out_o_carrier_id = omv->o_carrier_id;

        auto lc_scan_callback = [&] (const orderline_key&, const orderline_const_value& lcv) -> bool {
            out_ol_i_id = lcv.ol_i_id;
            out_ol_supply_w_id = lcv.ol_supply_w_id;
            out_ol_quantity = lcv.ol_quantity;
            out_ol_amount = lcv.ol_amount;
            return true;
        };
        auto lm_scan_callback = [&] (const orderline_key&, const orderline_comm_value& lmv) -> bool {
            out_ol_delivery_d = lmv.ol_delivery_d;
            return true;
        };

        orderline_key olk0(q_w_id, q_d_id, cus_o_id, 0);
        orderline_key olk1(q_w_id, q_d_id, cus_o_id, std::numeric_limits<uint64_t>::max());

        success = db.tbl_orderlines_const(q_w_id)
                .template range_scan<decltype(lc_scan_callback), false/*reverse*/>(olk0, olk1, lc_scan_callback, RowAccess::ObserveValue);
        CHK(success);
        success = db.tbl_orderlines_comm(q_w_id)
                .template range_scan<decltype(lm_scan_callback), false/*reverse*/>(olk0, olk1, lm_scan_callback, RowAccess::ObserveValue);
        CHK(success);
#else
        std::tie(success, result, row, value) = db.tbl_orders(q_w_id).select_row(ok, RowAccess::ObserveValue);
        CHK(success);
        assert(result);

        auto ov = reinterpret_cast<const order_value *>(value);
        out_o_entry_date = ov->o_entry_d;
        out_o_carrier_id = ov->o_carrier_id;

        auto ol_scan_callback = [&] (const orderline_key& olk, const orderline_value& olv) -> bool {
            (void)olk;
            out_ol_i_id = olv.ol_i_id;
            out_ol_supply_w_id = olv.ol_supply_w_id;
            out_ol_quantity = olv.ol_quantity;
            out_ol_amount = olv.ol_amount;
            out_ol_delivery_d = olv.ol_delivery_d;
            return true;
        };

        orderline_key olk0(q_w_id, q_d_id, cus_o_id, 0);
        orderline_key olk1(q_w_id, q_d_id, cus_o_id, std::numeric_limits<uint64_t>::max());

        success = db.tbl_orderlines(q_w_id)
                .template range_scan<decltype(ol_scan_callback), false/*reverse*/>(olk0, olk1, ol_scan_callback,
#if TABLE_FINE_GRAINED
                        {{ol_nc::ol_i_id, access_t::read},
                         {ol_nc::ol_supply_w_id, access_t::read},
                         {ol_nc::ol_quantity, access_t::read},
                         {ol_nc::ol_amount, access_t::read},
                         {ol_nc::ol_delivery_d, access_t::read}}
#else
                        RowAccess::ObserveValue
#endif
                );
        CHK(success);
#endif
    } else {
        // order doesn't exist, simply commit the transaction
    }

    // commit txn
    // retry until commits
    } TEND(true);

    TXP_INCREMENT(txp_tpcc_os_commits);
    TXP_ACCOUNT(txp_tpcc_os_aborts, starts - 1);
}

template <typename DBParams>
void tpcc_runner<DBParams>::run_txn_delivery(uint64_t q_w_id,
                                             std::array<uint64_t, NUM_DISTRICTS_PER_WAREHOUSE>& last_delivered) {
#if TABLE_FINE_GRAINED
    typedef order_value::NamedColumn od_nc;
    typedef orderline_value::NamedColumn ol_nc;
    typedef customer_value::NamedColumn cu_nc;
#endif

    uint64_t carrier_id = ig.random(1, 10);
    uint32_t delivery_date = ig.gen_date();

    bool success, result;
    uintptr_t row;
    const void *value;

    uint64_t order_id;
    std::array<uint64_t, NUM_DISTRICTS_PER_WAREHOUSE> delivered_order_ids;
    std::vector<uint64_t> ol_nums;
    int32_t ol_amount_sum;

    auto no_scan_callback = [&order_id] (const order_key& ok, const bench::dummy_row&) -> bool {
        order_id = bswap(ok.o_id);
        return true;
    };

    size_t starts = 0;

    TXP_INCREMENT(txp_tpcc_dl_stage1);

    RWTXN {
    ++starts;

    for (uint64_t q_d_id = 1; q_d_id <= 10; ++q_d_id) {
        order_id = 0;
        //printf("district %lu\n", q_d_id);
        //Sto::print_read_set_size("0");

        order_key k0(q_w_id, q_d_id, last_delivered[q_d_id - 1]);
        order_key k1(q_w_id, q_d_id, std::numeric_limits<order_key::oid_type>::max());
        success = db.tbl_neworders(q_w_id)
                .template range_scan<decltype(no_scan_callback), false/*reverse*/>(k0, k1, no_scan_callback, RowAccess::ObserveValue, true, 1);
        CHK(success);
        //Sto::print_read_set_size("1");

        TXP_INCREMENT(txp_tpcc_dl_stage2);

        delivered_order_ids[q_d_id - 1] = order_id;
        if (order_id == 0)
            continue;

        order_key ok(q_w_id, q_d_id, order_id);
        std::tie(success, result) = db.tbl_neworders(q_w_id).delete_row(ok);
        CHK(success);
        CHK(result);
        assert(result);

#if TPCC_SPLIT_TABLE
        std::tie(success, result, std::ignore, value) = db.tbl_orders_const(q_w_id).select_row(ok, RowAccess::ObserveValue);
        CHK(success);
        assert(result);

        auto ocv = reinterpret_cast<const order_const_value *>(value);
        uint64_t q_c_id = ocv->o_c_id;
        auto ol_cnt = ocv->o_ol_cnt;

        std::tie(success, result, row, value) = db.tbl_orders_comm(q_w_id).select_row(ok,
                Commute ? RowAccess::None : RowAccess::UpdateValue);
        CHK(success);
        assert(result);

        TXP_INCREMENT(txp_tpcc_dl_stage4);

        if (Commute) {
            commutators::Commutator<order_comm_value> commutator(carrier_id);
            db.tbl_orders_comm(q_w_id).update_row(row, commutator);
        } else {
            auto omv = reinterpret_cast<const order_comm_value*>(value);
            order_comm_value *new_omv = Sto::tx_alloc(omv);
            new_omv->o_carrier_id = carrier_id;
            db.tbl_orders_comm(q_w_id).update_row(row, new_omv);
        }
#else
        std::tie(success, result, row, value) = db.tbl_orders(q_w_id).select_row(ok,
#if TABLE_FINE_GRAINED
            {{od_nc::o_c_id, access_t::read},
             {od_nc::o_ol_cnt, access_t::read},
             {od_nc::o_carrier_id, Commute ? access_t::write : access_t::update}}
#else
            RowAccess::ObserveValue
#endif
        );
        CHK(success);
        assert(result);

        TXP_INCREMENT(txp_tpcc_dl_stage4);

        auto ov = reinterpret_cast<const order_value *>(value);
        uint64_t q_c_id = ov->o_c_id;
        assert(q_c_id != 0);
        auto ol_cnt = ov->o_ol_cnt;

        if (Commute) {
            commutators::Commutator<order_value> commutator(carrier_id);
            db.tbl_orders(q_w_id).update_row(row, commutator);
        } else {
            auto ov = reinterpret_cast<const order_value*>(value);
            order_value *new_ov = Sto::tx_alloc(ov);
            new_ov->o_carrier_id = carrier_id;
            db.tbl_orders(q_w_id).update_row(row, new_ov);
        }
#endif

        ol_amount_sum = 0;

        for (uint32_t ol_num = 1; ol_num <= ol_cnt; ++ol_num) {
            orderline_key olk(q_w_id, q_d_id, order_id, ol_num);
#if TPCC_SPLIT_TABLE
            std::tie(success, result, row, value) = db.tbl_orderlines_const(q_w_id).select_row(olk, RowAccess::ObserveValue);
            CHK(success);
            assert(result);

            auto lcv = reinterpret_cast<const orderline_const_value *>(value);
            ol_amount_sum += lcv->ol_amount;

            std::tie(success, result, row, value) = db.tbl_orderlines_comm(q_w_id).select_row(olk,
                    Commute ? RowAccess::None : RowAccess::UpdateValue);
            CHK(success);
            assert(result);

            TXP_INCREMENT(txp_tpcc_dl_stage3);

            if (Commute) {
                commutators::Commutator<orderline_comm_value> commutator(delivery_date);
                db.tbl_orderlines_comm(q_w_id).update_row(row, commutator);
            } else {
                auto lmv = reinterpret_cast<const orderline_comm_value*>(value);
                orderline_comm_value *new_lmv = Sto::tx_alloc(lmv);
                new_lmv->ol_delivery_d = delivery_date;
                db.tbl_orderlines_comm(q_w_id).update_row(row, new_lmv);
            }
#else
            std::tie(success, result, row, value) = db.tbl_orderlines(q_w_id).select_row(olk,
#if TABLE_FINE_GRAINED
                {{ol_nc::ol_amount, access_t::read},
                 {ol_nc::ol_delivery_d, Commute ? access_t::write : access_t::update}}
#else
                RowAccess::UpdateValue
#endif
            );
            CHK(success);

            assert(result);

            TXP_INCREMENT(txp_tpcc_dl_stage3);

            auto olv = reinterpret_cast<const orderline_value *>(value);
            ol_amount_sum += olv->ol_amount;

            if (Commute) {
                commutators::Commutator<orderline_value> commutator(delivery_date);
                db.tbl_orderlines(q_w_id).update_row(row, commutator);
            } else {
                auto olv = reinterpret_cast<const orderline_value*>(value);
                orderline_value *new_olv = Sto::tx_alloc(olv);
                new_olv->ol_delivery_d = delivery_date;
                db.tbl_orderlines(q_w_id).update_row(row, new_olv);
            }
#endif
        }

#if TPCC_SPLIT_TABLE
        customer_key ck(q_w_id, q_d_id, q_c_id);
        std::tie(success, result, row, value) = db.tbl_customers_comm(q_w_id).select_row(ck,
                Commute ? RowAccess::None : RowAccess::UpdateValue);
        CHK(success);
        assert(result);

        TXP_INCREMENT(txp_tpcc_dl_stage5);

        if (Commute) {
            commutators::Commutator<customer_comm_value> commutator((int64_t)ol_amount_sum);
            db.tbl_customers_comm(q_w_id).update_row(row, commutator);
        } else {
            auto cmv = reinterpret_cast<const customer_comm_value*>(value);
            auto new_cmv = Sto::tx_alloc(cmv);
            new_cmv->c_balance += (int64_t)ol_amount_sum;
            new_cmv->c_delivery_cnt += 1;
            db.tbl_customers_comm(q_w_id).update_row(row, new_cmv);
        }
#else
        customer_key ck(q_w_id, q_d_id, q_c_id);
        std::tie(success, result, row, value) = db.tbl_customers(q_w_id).select_row(ck,
#if TABLE_FINE_GRAINED
            {{cu_nc::c_balance, Commute ? access_t::write : access_t::update},
             {cu_nc::c_delivery_cnt, Commute ? access_t::write : access_t::update}}
#else
            RowAccess::ObserveValue
#endif
        );
        CHK(success);
        assert(result);

        TXP_INCREMENT(txp_tpcc_dl_stage5);

        if (Commute) {
            commutators::Commutator<customer_value> commutator((int64_t)ol_amount_sum);
            db.tbl_customers(q_w_id).update_row(row, commutator);
        } else {
            auto cv = reinterpret_cast<const customer_value*>(value);
            auto new_cv = Sto::tx_alloc(cv);
            new_cv->c_balance += (int64_t)ol_amount_sum;
            new_cv->c_delivery_cnt += 1;
            db.tbl_customers(q_w_id).update_row(row, new_cv);
        }
#endif
    }

    } TEND(true);

    last_delivered = delivered_order_ids;

    TXP_INCREMENT(txp_tpcc_dl_commits);
    TXP_ACCOUNT(txp_tpcc_dl_aborts, starts - 1);
}

template <typename DBParams>
void tpcc_runner<DBParams>::run_txn_stocklevel(){
#if TABLE_FINE_GRAINED
    typedef orderline_value::NamedColumn ol_nc;
    typedef stock_value::NamedColumn st_nc;
#endif

    uint64_t q_w_id = ig.random(w_id_start, w_id_end);
    uint64_t q_d_id = ig.random(1, 10);
    auto threshold = (int32_t)ig.random(10, 20);

    std::set<uint64_t> ol_iids;

    volatile int out_count = 0;
    (void)out_count;

    bool success, result;
    const void *value;

#if TPCC_SPLIT_TABLE
    auto lc_scan_callback = [ &ol_iids] (const orderline_key&, const orderline_const_value& value) -> bool {
        ol_iids.insert(value.ol_i_id);
        return true;
    };
#else
    auto ol_scan_callback = [ &ol_iids] (const orderline_key&, const orderline_value& value) -> bool {
        ol_iids.insert(value.ol_i_id);
        return true;
    };
#endif

    size_t starts = 0;

    TXN {
    ++starts;

    ol_iids.clear();
    auto d_next_oid = db.oid_generator().get(q_w_id, q_d_id);

    uint64_t oid_lower = (d_next_oid > 20) ? d_next_oid - 20 : 0;
    orderline_key olk0(q_w_id, q_d_id, oid_lower, 0);
    orderline_key olk1(q_w_id, q_d_id, d_next_oid, 0);

#if TPCC_SPLIT_TABLE
    success = db.tbl_orderlines_const(q_w_id)
            .template range_scan<decltype(lc_scan_callback), false/*reverse*/>(olk1, olk0, lc_scan_callback, RowAccess::ObserveValue);
    CHK(success);
#else
    success = db.tbl_orderlines(q_w_id)
            .template range_scan<decltype(ol_scan_callback), false/*reverse*/>(olk1, olk0, ol_scan_callback,
#if TABLE_FINE_GRAINED
                    {{ol_nc::ol_i_id, access_t::read}}
#else
                    RowAccess::ObserveValue
#endif
            );
    CHK(success);
#endif

    for (auto iid : ol_iids) {
        stock_key sk(q_w_id, iid);
#if TPCC_SPLIT_TABLE
        std::tie(success, result, std::ignore, value) = db.tbl_stocks_comm(q_w_id).select_row(sk, RowAccess::ObserveValue);
        CHK(success);
        assert(result);
        auto sv = reinterpret_cast<const stock_comm_value*>(value);
#else
        std::tie(success, result, std::ignore, value) = db.tbl_stocks(q_w_id).select_row(sk,
#if TABLE_FINE_GRAINED
            {{st_nc::s_quantity, access_t::read}}
#else
            RowAccess::ObserveValue
#endif
        );
        CHK(success);
        assert(result);
        auto sv = reinterpret_cast<const stock_value*>(value);
#endif
        if(sv->s_quantity < threshold) {
            out_count += 1;
        }
    }

    } TEND(true);

    TXP_INCREMENT(txp_tpcc_st_commits);
    TXP_ACCOUNT(txp_tpcc_st_aborts, starts - 1);
}

}; // namespace tpcc
