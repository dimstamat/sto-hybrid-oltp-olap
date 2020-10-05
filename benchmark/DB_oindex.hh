#pragma once


#include <sstream>

#ifndef THREADS_MAX
#define THREADS_MAX 80
#endif


#ifndef LOG_NTABLES // should be defined in TPCC_bench.hh
#define LOG_NTABLES 0
#endif

extern logset_base* logs;


namespace bench {
template <typename K, typename V, typename DBParams, short LOG=0>
class ordered_index : public TObject {

    // Dimos - emable logging
    // template argument LOG
    // 0 : no logging
    // 1 : default logging - one log per thread
    // 2 : one log per thread per table
    // 3 : one std::unordered_map per thread per table
    // the index of this table in the log buffer when we choose one log per thread per table
    int tbl_index_ = -1;
    uint32_t table_sz [THREADS_MAX];
    uint32_t table_n_elems [THREADS_MAX];
public:
    typedef K key_type;
    typedef V value_type;
    typedef commutators::Commutator<value_type> comm_type;

    //typedef typename get_occ_version<DBParams>::type occ_version_type;
    typedef typename get_version<DBParams>::type version_type;

    static constexpr typename version_type::type invalid_bit = TransactionTid::user_bit;
    static constexpr TransItem::flags_type insert_bit = TransItem::user0_bit;
    static constexpr TransItem::flags_type delete_bit = TransItem::user0_bit << 1u;
    static constexpr TransItem::flags_type row_update_bit = TransItem::user0_bit << 2u;
    static constexpr TransItem::flags_type row_cell_bit = TransItem::user0_bit << 3u;
    static constexpr TransItem::flags_type log_add_bit = TransItem::user0_bit << 4u;
    static constexpr uintptr_t internode_bit = 1;
    // TicToc node version bit
    static constexpr uintptr_t ttnv_bit = 1 << 1u;

    typedef typename value_type::NamedColumn NamedColumn;
    typedef IndexValueContainer<V, version_type> value_container_type;

    static constexpr bool value_is_small = is_small<V>::value;

    static constexpr bool index_read_my_write = DBParams::RdMyWr;

    struct internal_elem {
        key_type key;
        value_container_type row_container;
        bool deleted;
        
        internal_elem(const key_type& k, const value_type& v, bool valid)
            : key(k),
              row_container((valid ? Sto::initialized_tid() : (Sto::initialized_tid() | invalid_bit)),
                            !valid, v),
              deleted(false)
               {}

        // Dimos - initialize the value type with the timestamp found when replaying the log. This is the transaction commit TID
        internal_elem(const key_type& k, const value_type& v, uint64_t ts)
            : key(k), row_container( ts, false, v), deleted(false) {}

        version_type& version() {
            return row_container.row_version();
        }

        bool valid() {
            return !(version().value() & invalid_bit);
        }
    };

    struct internal_elem_log : internal_elem {
        // log_pos will store the creation_epoch, which is the epoch that this internal_elem_log was created.
        // If there is an overwrite from a newer epoch, we cannot simply overwrite it because the new value will be lost! (written in an older epoch that will never be read again)
        uint64_t log_pos[NLOGGERS]; // we will add the epoch number in the 16 most significant bits - up to 16k different epochs. This leaves 48 bits for log pos: (2.8 * 10^14)
        
        internal_elem_log(const key_type& k, const value_type& v, bool valid)
            : internal_elem::internal_elem(k, v, valid)
            //log_pos{0,0,0,0,0,0,0,0,0,0}
        {
            static_assert(NLOGGERS == 10); // we need to make sure the number of loggers is the same as the one specified in the initialization list above
            memset(log_pos, 0, NLOGGERS * sizeof(uint64_t));
            /*for(int i=0; i<NLOGGERS; i++)
                    log_pos[i] = -1;
                    //memset(log_pos, 0, NLOGGERS * sizeof(int));
            */
        }

        uint64_t get_log_pos(int i){
            constexpr uint64_t pos_mask = 0xFFFFFFFFFFFF; // this is the mask to get the 48 least significant bits (each hex digit is 4 bits)
            //uint64_t mask = (((uint64_t)1 << 48) - 1); // another way of getting 000...{111..} with 48 1's
            return (log_pos[i] & pos_mask);
        }

        void set_log_pos(int i, uint64_t pos, uint64_t creation_epoch){
            log_pos[i] = (creation_epoch << 48) | pos;
        }

        void update_log_pos(int i, uint64_t pos){
            constexpr uint64_t pos_mask = 0xFFFFFFFFFFFF; // this is the mask to get the 48 least significant bits (each hex digit is 4 bits)
            log_pos[i] = (log_pos[i] & ~pos_mask) | (pos & pos_mask);
        }

        uint64_t get_creation_epoch(int i){
            return (log_pos[i] >> 48);
        }

    };

    struct table_params : public Masstree::nodeparams<15,15> {
        typedef internal_elem* value_type;
        typedef Masstree::value_print<value_type> value_print_type;
        typedef threadinfo threadinfo_type;

        static constexpr bool track_nodes = (DBParams::NodeTrack && DBParams::TicToc);
        typedef std::conditional_t<track_nodes, version_type, int> aux_tracker_type;
    };

    typedef Masstree::Str Str;
    typedef Masstree::basic_table<table_params> table_type;
    typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
    typedef Masstree::tcursor<table_params> cursor_type;
    typedef Masstree::leaf<table_params> leaf_type;

    typedef typename table_type::node_type node_type;
    typedef typename unlocked_cursor_type::nodeversion_value_type nodeversion_value_type;

    using column_access_t = typename split_version_helpers<ordered_index<K, V, DBParams, LOG>>::column_access_t;
    using item_key_t = typename split_version_helpers<ordered_index<K, V, DBParams, LOG>>::item_key_t;
    template <typename T>
    static constexpr auto column_to_cell_accesses
        = split_version_helpers<ordered_index<K, V, DBParams, LOG>>::template column_to_cell_accesses<T>;
    template <typename T>
    static constexpr auto extract_item_list
        = split_version_helpers<ordered_index<K, V, DBParams, LOG>>::template extract_item_list<T>;

    typedef std::tuple<bool, bool, uintptr_t, const value_type*> sel_return_type;
    typedef std::tuple<bool, bool>                               ins_return_type;
    #if TPCH_SINGLE_NODE
    typedef std::tuple<bool, bool, internal_elem*>                               ins_elem_return_type;
    #endif
    typedef std::tuple<bool, bool>                               del_return_type;

    static __thread typename table_params::threadinfo_type *ti;

    ordered_index(size_t init_size) : tbl_index_(-1) {
        this->table_init();
        (void)init_size;
    }
    ordered_index() : tbl_index_(-1) {
        this->table_init();
    }

    // tbl_index: the index of this table in the log buffer
    ordered_index(size_t init_size, int tbl_index) : tbl_index_(tbl_index) {
        this->table_init();
        (void)init_size;
        assert(tbl_index>=0);  //&& tbl_index <= logs_tbl->log(runner_num). getNumtbl ?? )
    }



    void table_init() {
        if (ti == nullptr)
            ti = threadinfo::make(threadinfo::TI_MAIN, -1);
        table_.initialize(*ti);
        key_gen_ = 0;
        memset(table_sz, 0, THREADS_MAX * sizeof(uint32_t));
        memset(table_n_elems, 0, THREADS_MAX * sizeof(uint32_t));
    }

    static void thread_init(){
        thread_init(0);
    }

    uint32_t table_size(){
        uint32_t sz=0;
        for(int i=0; i<THREADS_MAX; i++)
            sz+= table_sz[i];
        return sz;
    }

    uint32_t table_num_elems(){
        uint32_t num_elems=0;
        for(int i=0; i<THREADS_MAX; i++)
            num_elems+= table_n_elems[i];
        return num_elems;
    }

    // runner_num: the number of this thread [0-num_runners), since it can be different than TThread::id() - TThread::id() is the actual CPU id pinned on that thread. runner_num is from 0 to num_runners!
    static void thread_init(int runner_num) {
        if (ti == nullptr)
            ti = threadinfo::make(threadinfo::TI_PROCESS, TThread::id());
        Transaction::tinfo[TThread::id()].trans_start_callback = []() {
            ti->rcu_start();
        };
        Transaction::tinfo[TThread::id()].trans_end_callback = []() {
            ti->rcu_stop();
        };
        // Dimos - logging
        if(LOG == 1){
            if(!ti->logger() && logs){ // make sure logs are initialized (they are only initialized once the TPC-C benchmark starts running ; they are not initialized during the prepopulation phase)
                //std::cout<<"Thread "<< TThread::id() <<": "<<runner_num<<std::endl;
                ti->set_logger(& (reinterpret_cast<logset*>(logs))->log(runner_num));
            }
        }
        else if(LOG == 2){
            if(!ti->logger_tbl() && logs)
                ti->set_logger(& (reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs))->log(runner_num));
        }
        else if (LOG == 3){
            if(!ti->logger_tbl() && logs)
                ti->set_logger(& (reinterpret_cast<logset_map<LOG_NTABLES>*>(logs))->log(runner_num));
        }
    }

    uint64_t gen_key() {
        return fetch_and_add(&key_gen_, 1);
    }

    sel_return_type
    select_row(const key_type& key, RowAccess acc) {
        unlocked_cursor_type lp(table_, key);
        bool found = lp.find_unlocked(*ti);
        internal_elem *e = lp.value();
        if (found) {
            return select_row(reinterpret_cast<uintptr_t>(e), acc);
        } else {
            if (!register_internode_version(lp.node(), lp))
                goto abort;
            return sel_return_type(true, false, 0, nullptr);
        }

    abort:
        return sel_return_type(false, false, 0, nullptr);
    }

    sel_return_type
    select_row(const key_type& key, std::initializer_list<column_access_t> accesses) {
        unlocked_cursor_type lp(table_, key);
        bool found = lp.find_unlocked(*ti);
        internal_elem *e = lp.value();
        if (found) {
            return select_row(reinterpret_cast<uintptr_t>(e), accesses);
        } else {
            if (!register_internode_version(lp.node(), lp))
                return sel_return_type(false, false, 0, nullptr);
            return sel_return_type(true, false, 0, nullptr);
        }

        return sel_return_type(false, false, 0, nullptr);
    }

    sel_return_type
    select_row(uintptr_t rid, RowAccess access) {
        auto e = reinterpret_cast<internal_elem *>(rid);
        bool ok = true;
        TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

        if (is_phantom(e, row_item))
            goto abort;

        if (index_read_my_write) {
            if (has_delete(row_item)) {
                return sel_return_type(true, false, 0, nullptr);
            }
            if (has_row_update(row_item)) {
                value_type *vptr;
                if (has_insert(row_item))
                    vptr = &e->row_container.row;
                else
                    vptr = row_item.template raw_write_value<value_type*>();
                return sel_return_type(true, true, rid, vptr);
            }
        }

        switch (access) {
            case RowAccess::UpdateValue:
                ok = version_adapter::select_for_update(row_item, e->version());
                row_item.add_flags(row_update_bit);
                break;
            case RowAccess::ObserveExists:
            case RowAccess::ObserveValue:
                ok = row_item.observe(e->version());
                break;
            default:
                break;
        }

        if (!ok)
            goto abort;

        return sel_return_type(true, true, rid, &(e->row_container.row));

    abort:
        return sel_return_type(false, false, 0, nullptr);
    }

    sel_return_type
    select_row(uintptr_t rid, std::initializer_list<column_access_t> accesses) {
        auto e = reinterpret_cast<internal_elem*>(rid);
        TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

        // Translate from column accesses to cell accesses
        // all buffered writes are only stored in the wdata_ of the row item (to avoid redundant copies)
        auto cell_accesses = column_to_cell_accesses<value_container_type>(accesses);

        std::array<TransItem*, value_container_type::num_versions> cell_items {};
        bool any_has_write;
        bool ok;
        std::tie(any_has_write, cell_items) = extract_item_list<value_container_type>(cell_accesses, this, e);

        if (is_phantom(e, row_item))
            goto abort;

        if (index_read_my_write) {
            if (has_delete(row_item)) {
                return sel_return_type(true, false, 0, nullptr);
            }
            if (any_has_write || has_row_update(row_item)) {
                value_type *vptr;
                if (has_insert(row_item))
                    vptr = &e->row_container.row;
                else
                    vptr = row_item.template raw_write_value<value_type *>();
                return sel_return_type(true, true, rid, vptr);
            }
        }

        ok = access_all(cell_accesses, cell_items, e->row_container);
        if (!ok)
            goto abort;

        return sel_return_type(true, true, rid, &(e->row_container.row));

    abort:
        return sel_return_type(false, false, 0, nullptr);
    }

    // Dimos - enable logging - add to log - called from TPCC_txns - not used for now!
    /*void log_add_external(enum logcommand cmd, Str& key, Str& val, loginfo::query_times& qtimes){
        // this will update the thread_info->ts_
        //qtimes.ts = ti->update_timestamp(vers);
        //qtimes.prev_ts = vers;
        if (loginfo* log = ti->logger()) {
            log->acquire();
            qtimes.epoch = global_log_epoch;
            ti->logger()->record(cmd, qtimes, key, val);
        }
    }*/

    void update_row(uintptr_t rid, value_type *new_row) {
        auto e = reinterpret_cast<internal_elem*>(rid);
        auto row_item = Sto::item(this, item_key_t::row_item_key(e));
        if (value_is_small) {
            row_item.acquire_write(e->version(), *new_row);
        } else {
            row_item.acquire_write(e->version(), new_row);
        }
        if(LOG>0)
            row_item.add_flags(log_add_bit);
    }

    void update_row(uintptr_t rid, const comm_type &comm) {
        assert(&comm);
        auto row_item = Sto::item(this, item_key_t::row_item_key(reinterpret_cast<internal_elem *>(rid)));
        row_item.add_commute(comm);
    }

    // insert assumes common case where the row doesn't exist in the table
    // if a row already exists, then use select (FOR UPDATE) instead
    // Dimos - we need to keep track of the pointer to the internal_elem, required for the secondary indexes!
    // We also need to know whether we should apply to the log or not, due to txn retries! We don't want to add to the log on every txn retry!
    ins_return_type
    insert_row(const key_type& key, value_type *vptr, bool overwrite = false, bool append=false, bool store_elem=false, uint64_t**el=nullptr) {
        (void)append;
        cursor_type lp(table_, key);
        bool found = lp.find_insert(*ti);
        if (found) {
            // NB: the insert method only manipulates the row_item. It is possible
            // this insert is overwriting some previous updates on selected columns
            // The expected behavior is that this row-level operation should overwrite
            // all changes made by previous updates (in the same transaction) on this
            // row. We achieve this by granting this row_item a higher priority.
            // During the install phase, if we notice that the row item has already
            // been locked then we simply ignore installing any changes made by cell items.
            // It should be trivial for a cell item to find the corresponding row item
            // and figure out if the row-level version is locked.
            internal_elem *e = lp.value();
            if(store_elem)
                *el = reinterpret_cast<uint64_t*>(e);
            lp.finish(0, *ti);

            TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

            if (is_phantom(e, row_item))
                goto abort;

            if (index_read_my_write) {
                if (has_delete(row_item)) {
                    auto proxy = row_item.clear_flags(delete_bit).clear_write();

                    if (value_is_small)
                        proxy.add_write(*vptr);
                    else
                        proxy.add_write(vptr);
                    return ins_return_type(true, false);
                }
            }

            if (overwrite) {
                bool ok;
                if (value_is_small)
                    ok = version_adapter::select_for_overwrite(row_item, e->version(), *vptr);
                else
                    ok = version_adapter::select_for_overwrite(row_item, e->version(), vptr);
                if (!ok)
                    goto abort;
                if (index_read_my_write) {
                    if (has_insert(row_item)) {
                        copy_row(e, vptr);
                    }
                }
            } else {
                // observes that the row exists, but nothing more
                if (!row_item.observe(e->version()))
                    goto abort;
            }
            // cannot add to log here - what if we abort!?
            // add to log on cleanup! Just mark it now!
            #if ADD_TO_LOG > 0
            if(LOG > 0 && overwrite){
                row_item.add_flags(log_add_bit);
            }
            #endif

        } else {
            internal_elem* e = nullptr;
            #if LOGREC_OVERWRITE
                if(LOG == 2) 
                    e = new internal_elem_log(key, vptr ? *vptr : value_type(),
                                       false /*!valid*/);
                else
                    e = new internal_elem(key, vptr ? *vptr : value_type(),
                                       false /*!valid*/);
            #else
                e = new internal_elem(key, vptr ? *vptr : value_type(),
                                       false /*!valid*/);
            #endif

            lp.value() = e;

            node_type *node;
            nodeversion_value_type orig_nv;
            nodeversion_value_type new_nv;

            bool split_right = (lp.node() != lp.original_node());
            if (split_right) {
                node = lp.original_node();
                orig_nv = lp.original_version_value();
                new_nv = lp.updated_version_value();
            } else {
                node = lp.node();
                orig_nv = lp.previous_full_version_value();
                new_nv = lp.next_full_version_value(1);
            }

            fence();
            if(store_elem)
                *el = reinterpret_cast<uint64_t*>(e);
            lp.finish(1, *ti);
            //fence();

            TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));
                
            row_item.acquire_write(e->version());
            row_item.add_flags(insert_bit);

            // update the node version already in the read set and modified by split
            if (!update_internode_version(node, orig_nv, new_nv))
                goto abort;
            #if ADD_TO_LOG > 0
            if(LOG > 0){
                row_item.add_flags(log_add_bit);
            }
            #endif
        }
        return ins_return_type(true, found);

    abort:
        return ins_return_type(false, false);
    }

    del_return_type
    delete_row(const key_type& key) {
        unlocked_cursor_type lp(table_, key);
        bool found = lp.find_unlocked(*ti);
        if (found) {
            internal_elem *e = lp.value();
            TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

            if (is_phantom(e, row_item)) {
                goto abort;
            }

            if (index_read_my_write) {
                if (has_delete(row_item))
                    return del_return_type(true, false);
                if (!e->valid() && has_insert(row_item)) {
                    row_item.add_flags(delete_bit);
                    return del_return_type(true, true);
                }
            }

            // Register a TicToc write to the leaf node when necessary.
            ttnv_register_node_write(lp.node());

            // select_for_update will register an observation and set the write bit of
            // the TItem
            if (!version_adapter::select_for_update(row_item, e->version())) {
                goto abort;
            }
            fence();
            if (e->deleted) {
                goto abort;
            }
            row_item.add_flags(delete_bit);
        } else {
            if (!register_internode_version(lp.node(), lp)) {
                goto abort;
            }
        }
        //TODO: add deletes to log!
        return del_return_type(true, found);

    abort:
        return del_return_type(false, false);
    }

    template <typename Callback, bool Reverse>
    bool range_scan(const key_type& begin, const key_type& end, Callback callback,
                    std::initializer_list<column_access_t> accesses, bool phantom_protection = true, int limit = -1) {
        assert((limit == -1) || (limit > 0));
        auto node_callback = [&] (leaf_type* node,
            typename unlocked_cursor_type::nodeversion_value_type version) {
            return ((!phantom_protection) || scan_track_node_version(node, version));
        };

        auto cell_accesses = column_to_cell_accesses<value_container_type>(accesses);

        auto value_callback = [&] (const lcdf::Str& key, internal_elem *e, bool& ret, bool& count) {
            TransProxy row_item = index_read_my_write ? Sto::item(this, item_key_t::row_item_key(e))
                                                      : Sto::fresh_item(this, item_key_t::row_item_key(e));

            bool any_has_write;
            std::array<TransItem*, value_container_type::num_versions> cell_items {};
            std::tie(any_has_write, cell_items) = extract_item_list<value_container_type>(cell_accesses, this, e);

            if (index_read_my_write) {
                if (has_delete(row_item)) {
                    ret = true;
                    count = false;
                    return true;
                }
                if (any_has_write) {
                    if (has_insert(row_item))
                        ret = callback(key_type(key), e->row_container.row);
                    else
                        ret = callback(key_type(key), *(row_item.template raw_write_value<value_type *>()));
                    return true;
                }
            }

            bool ok = access_all(cell_accesses, cell_items, e->row_container);
            if (!ok)
                return false;
            //bool ok = item.observe(e->version);
            //if (Adaptive) {
            //    ok = item.observe(e->version, true/*force occ*/);
            //} else {
            //    ok = item.observe(e->version);
            //}

            // skip invalid (inserted but yet committed) values, but do not abort
            if (!e->valid()) {
                ret = true;
                count = false;
                return true;
            }

            ret = callback(key_type(key), e->row_container.row);
            return true;
        };

        range_scanner<decltype(node_callback), decltype(value_callback), Reverse>
            scanner(end, node_callback, value_callback, limit);
        if (Reverse)
            table_.rscan(begin, true, scanner, *ti);
        else
            table_.scan(begin, true, scanner, *ti);
        return scanner.scan_succeeded_;
    }

    template <typename Callback, bool Reverse>
    bool range_scan(const key_type& begin, const key_type& end, Callback callback,
                    RowAccess access, bool phantom_protection = true, int limit = -1) {
                        return this-> template range_scan<Callback, Reverse>(false, begin, end, callback, access, phantom_protection, limit);
    }



    template <typename Callback, bool Reverse>
    bool nontrans_range_scan(const key_type& begin, const key_type& end, Callback callback, int limit = -1) {
        assert((limit == -1) || (limit > 0));
        auto node_callback = [&] (leaf_type* node,
                                  typename unlocked_cursor_type::nodeversion_value_type version) {
            (void)node;
            (void)version;
            return true;
        };

        auto value_callback = [&] (const lcdf::Str& key, internal_elem *e, bool& ret, bool& count) {
            if (!e->valid()) {
                ret = true;
                count = false;
                return true;
            }

            ret = callback(key_type(key), e->row_container.row);
            return true;
        };
        range_scanner<decltype(node_callback), decltype(value_callback), Reverse>
                scanner(end, node_callback, value_callback, limit);
        if (Reverse)
            table_.rscan(begin, true, scanner, *ti);
        else
            table_.scan(begin, true, scanner, *ti);
        return scanner.scan_succeeded_;
    }


    template <typename Callback, bool Reverse>
    bool range_scan(bool readOnly, const key_type& begin, const key_type& end, Callback callback,
                    RowAccess access, bool phantom_protection = true, int limit = -1) {
        assert((limit == -1) || (limit > 0));
        auto node_callback = [&] (leaf_type* node,
                                  typename unlocked_cursor_type::nodeversion_value_type version) {
            return ((!phantom_protection) || scan_track_node_version(node, version));
        };

        auto value_callback = [&] (const lcdf::Str& key, internal_elem *e, bool& ret, bool& count) {
            // Dimos - allow read-only transactions that do not create TransItems!
            if(!readOnly){
                TransProxy row_item = index_read_my_write ? Sto::item(this, item_key_t::row_item_key(e))
                                                        : Sto::fresh_item(this, item_key_t::row_item_key(e));

                if (index_read_my_write) {
                    if (has_delete(row_item)) {
                        ret = true;
                        count = false;
                        return true;
                    }
                    if (has_row_update(row_item)) {
                        if (has_insert(row_item))
                            ret = callback(key_type(key), e->row_container.row);
                        else
                            ret = callback(key_type(key), *(row_item.template raw_write_value<value_type *>()));
                        return true;
                    }
                }

                bool ok = true;
                switch (access) {
                    case RowAccess::ObserveValue:
                    case RowAccess::ObserveExists:
                        ok = row_item.observe(e->version());
                        break;
                    case RowAccess::None:
                        break;
                    default:
                        always_assert(false, "unsupported access type in range_scan");
                        break;
                }

                if (!ok)
                    return false;
            }

            // skip invalid (inserted but yet committed) values, but do not abort
            if (!e->valid()) {
                ret = true;
                count = false;
                return true;
            }

            ret = callback(key_type(key), e->row_container.row);
            return true;
        };

        range_scanner<decltype(node_callback), decltype(value_callback), Reverse>
                scanner(end, node_callback, value_callback, limit);
        if (Reverse)
            table_.rscan(begin, true, scanner, *ti);
        else
            table_.scan(begin, true, scanner, *ti);
        return scanner.scan_succeeded_;
    }

    value_type *nontrans_get(const key_type& k) {
        unlocked_cursor_type lp(table_, k);
        bool found = lp.find_unlocked(*ti);
        if (found) {
            internal_elem *e = lp.value();
            return &(e->row_container.row);
        } else
            return nullptr;
    }

    
    std::tuple<value_type*, uint64_t*> nontrans_get_el(const key_type & k){
        unlocked_cursor_type lp(table_, k);
        bool found = lp.find_unlocked(*ti);
        if (found) {
            internal_elem *e = lp.value();
            return std::make_tuple(&(e->row_container.row), (uint64_t*)e);
        } else
            return std::make_tuple(nullptr, nullptr);
    }

    // Dimos: We need to know the internal_elem where this <k,v> was inserted so that to 
    // store it in the secondary index for TPC-H
    uint64_t* nontrans_put_el(const key_type& k, const value_type& v) {
        cursor_type lp(table_, k);
        bool found = lp.find_insert(*ti);
        if (found) {
            internal_elem *e = lp.value();
            if (value_is_small)
                e->row_container.row = v;
            else
               copy_row(e, &v);
            lp.finish(0, *ti);
            return reinterpret_cast<uint64_t*>(e);
        } else {
            #if LOGREC_OVERWRITE
            if (LOG == 2){
                internal_elem_log *e = new internal_elem_log(k, v, true);
                lp.value() = e;
                lp.finish(1, *ti);
                return reinterpret_cast<uint64_t*>(e);
            }
            #endif
            internal_elem *e = new internal_elem(k, v, true);
            lp.value() = e;
            lp.finish(1, *ti);
            return reinterpret_cast<uint64_t*>(e);
        }
    }
    void nontrans_put(const key_type& k, const value_type& v, bool measure_db=false) {
        cursor_type lp(table_, k);
        bool found = lp.find_insert(*ti);
        
        if (found) {
            internal_elem *e = lp.value();
            if (value_is_small)
                e->row_container.row = v;
            else
               copy_row(e, &v);
            lp.finish(0, *ti);
        } else {
            #if LOGREC_OVERWRITE
            if (LOG == 2){
                internal_elem_log *e = new internal_elem_log(k, v, true);
                lp.value() = e;
                lp.finish(1, *ti);
            }
            else {
                internal_elem *e = new internal_elem(k, v, true);
                lp.value() = e;
                lp.finish(1, *ti);
            }
            #else
                internal_elem *e = new internal_elem(k, v, true);
                lp.value() = e;
                lp.finish(1, *ti);
            #endif
            #if MEASURE_DB_SZ
            if(measure_db){
                table_sz[ti->index()]+= (sizeof(key_type) + sizeof(value_type));
                table_n_elems[ti->index()]++;
            }
            #else
            (void)measure_db;
            #endif
        }
        
        #if TPCH_REPLICA
            // This called during the DB prepopulation phase and we must add to log so that to reproduce the same DB for OLAP! After that, we can run the benchmark like normal.
            if(LOG==2){
                assert(!found); // no overwrites of existing key while prepopulating the DB!
                logcommand cmd = logcmd_put;
                loginfo::query_times qtimes;
                qtimes.ts = 0;
                qtimes.prev_ts = 0;
                qtimes.epoch = global_log_epoch;
                void* logger = ti->logger_tbl();
                // no need to store the log pos in the internal_elem, since this log is temporary and will be cleared after DB prepopulation!
                reinterpret_cast<loginfo_tbl<LOG_NTABLES>*>(logger)->record(cmd, qtimes, Str(k), Str(v), tbl_index_);
            }
        #endif
    }

    // If key found, perform the put only if the stored version of the internal_elem is less than the given timestamp
    bool nontrans_put_if_ts(const key_type& k, const value_type& v, uint64_t ts){
        cursor_type lp(table_, k);
        bool found = lp.find_insert(*ti);
        if (found) {
            internal_elem *e = lp.value();
            uint64_t existing_ts = e->version().value();
            if(ts > existing_ts){ // insert it only if the given timestamp is greater than (after the) existing timestamp (version)
                if (value_is_small)
                    e->row_container.row = v;
                else
                    copy_row(e, &v);
                lp.finish(0, *ti);
                return true;
            }
            else {// the element found has version >= than the given timestamp, so don't insert!
                // TODO: optimization: Maintain the location of the element so that to avoid full tree traversal and also do not hold locks while going down the tree! Only grab the lock if we will actually insert!
                lp.finish(0, *ti);
                return false;
            }
        }
        else {
            internal_elem* e = new internal_elem(k, v, ts);
            lp.value() = e;
            lp.finish(1, *ti);
            #if MEASURE_DB_SZ
            table_sz[ti->index()]+= (sizeof(key_type) + sizeof(value_type));
            table_n_elems[ti->index()]++;
            #endif
            return true;
        }
    }

    // If key found, perform the put only if the stored version of the internal_elem is less than the given timestamp and return the pointer to the internal_elem
    uint64_t* nontrans_put_el_if_ts(const key_type& k, const value_type& v, uint64_t ts){
        cursor_type lp(table_, k);
        bool found = lp.find_insert(*ti);
        if (found) {
            internal_elem *e = lp.value();
            uint64_t existing_ts = e->version().value();
            if(ts > existing_ts){ // insert it only if the given timestamp is greater than (after the) existing timestamp (version)
                if (value_is_small)
                    e->row_container.row = v;
                else
                    copy_row(e, &v);
                lp.finish(0, *ti);
                return reinterpret_cast<uint64_t*>(e);
            }
            else {// the element found has version >= than the given timestamp, so don't insert!
                lp.finish(0, *ti);
                return nullptr;
            }
        }
        else {
            internal_elem* e = new internal_elem(k, v, ts);
            lp.value() = e;
            lp.finish(1, *ti);
            #if MEASURE_DB_SZ
            table_sz[ti->index()]+= (sizeof(key_type) + sizeof(value_type));
            table_n_elems[ti->index()]++;
            #endif
            return reinterpret_cast<uint64_t*>(e);
        }
    }

    // TObject interface methods
    bool lock(TransItem& item, Transaction &txn) override {
        assert(!is_internode(item));
        if constexpr (table_params::track_nodes) {
            if (is_ttnv(item)) {
                auto n = get_internode_address(item);
                return txn.try_lock(item, *static_cast<leaf_type*>(n)->get_aux_tracker());
            }
        }
        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();
        if (key.is_row_item())
            return txn.try_lock(item, e->version());
        else
            return txn.try_lock(item, e->row_container.version_at(key.cell_num()));
    }

    bool check(TransItem& item, Transaction& txn) override {
        if (is_internode(item)) {
            node_type *n = get_internode_address(item);
            auto curr_nv = static_cast<leaf_type *>(n)->full_version_value();
            auto read_nv = item.template read_value<decltype(curr_nv)>();
            return (curr_nv == read_nv);
        } else {
            if constexpr (table_params::track_nodes) {
                if (is_ttnv(item)) {
                    auto n = get_internode_address(item);
                    return static_cast<leaf_type*>(n)->get_aux_tracker()->cp_check_version(txn, item);
                }
            }
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            if (key.is_row_item())
                return e->version().cp_check_version(txn, item);
            else
                return e->row_container.version_at(key.cell_num()).cp_check_version(txn, item);
        }
    }

    // Use given v only if has_insert is true. That's because we cannot access the value from internal_elem after we release the lock!
    // If has_insert is false, then we can access the value directly from TransItem
    inline void log_add(TransItem& item, internal_elem* e, value_type* v, kvtimestamp_t ts){
        logcommand cmd;
        if(has_insert(item)){
            cmd = logcmd_put;
        } else if(has_delete(item)){
            cmd = logcmd_remove;
        } else {
            cmd = logcmd_replace;
        }
        #if LOG_DRAIN_REQUEST
        kvepoch_t global_epoch_cp = global_log_epoch; // get a copy of the global log epoch. If it changes later on, we will see it during the next check
        #endif

        //TODO: Only for the first TItem in the write set:
        //      - check whether request_log_drain is true and if it is, set local_epoch = global_epoch. This will ensure that we decide in which epoch this transaction belongs to at commit time, and before we start logging its TItems.
        // assign timestamp
        loginfo::query_times qtimes;
        //qtimes.ts = Sto::commit_tid(); // this won't work because there are no commit_tids set in Non-opaque/OCC so this will use global counter!
        //qtimes.prev_ts =  ( !has_insert(item) && !(has_delete(item)) ? TThread::txn->prev_commit_tid() : 0 );
        //qtimes.ts = e->version().value();
        qtimes.ts = ts;
        //qtimes.prev_ts = 0;
        void* logger = ti->logger_tbl();
        // check if there is a pending request for log drain and advance local epoch if there is!!
        #if LOGGING == 2 && LOG_DRAIN_REQUEST
        bool signal_logdrainer = false;
        // We need to know whether this is the first TItem from the write set to be added to the log
        // This is in case we have concurrent log drainers so that to advance local epoch right before committing the current txn!
        // A log drain is requested. Simply advance local log epoch (will happen later on) and signal the condition variable of waiting log drainers
        if(((reinterpret_cast<loginfo_tbl<LOG_NTABLES>*>(logger))->get_local_epoch() < global_epoch_cp) && item.has_flag(TransItem::first_titem_bit) ){  // change epoch now!
            signal_logdrainer = true; // signal the cond variable that the log drainer is waiting on. It should be signaled right after we record a log record with the new log epoch (which is not visible to the log drainer)
            qtimes.epoch = global_epoch_cp;
        }
        else { // do not change epoch yet! Do it in the next transaction!
            qtimes.epoch = (reinterpret_cast<loginfo_tbl<LOG_NTABLES>*>(logger))->get_local_epoch();
        }
        #else
            qtimes.epoch = global_log_epoch;
        #endif
        if(logger){
            //TODO: no need for locks since we use one log per thread!
            //if(LOG == 1)
            //    reinterpret_cast<loginfo*>(logger)->acquire();
            //if(LOG == 2)
            //    reinterpret_cast<loginfo_tbl<LOG_NTABLES>*>(logger)->acquire();
            // Must use a local log_epoch so that to increase only when a log drain is requested AND the current transaction is committed and fully applied in the log. We will do local_epoch = global_epoch only when transaction committed successfully (in TPCC_bench.hh)
            value_type* valp=nullptr;
            if(!has_insert(item)) // it is an update
                valp = item.template raw_write_value<value_type*>();
            /*if(has_insert(item)){
                #if LOGREC_OVERWRITE
                if(LOG == 2)
                    valp = & ((reinterpret_cast<internal_elem_log*>(e))->row_container.row);
                else
                    valp = &e->row_container.row;
                #else
                    // we shouldn't access internal_elem after unlocking!!
                    //valp = &e->row_container.row;
                    //valp = v;
                #endif
            }*/
            if(LOG == 1)
                reinterpret_cast<loginfo*>(logger)->record(cmd, qtimes, Str(e->key), Str(*(valp==nullptr?v : valp)), tbl_index_);
            else if(LOG == 2){
                auto logger_tbl = reinterpret_cast<loginfo_tbl<LOG_NTABLES>*>(logger);
                // experiment with not converting to string but storing the actual key: 
                // Not converting to string is a tiny bit better in high contention and a bit worse in low contention.
                // Even Str() converts the struct to Str but keeps the entire size of the struct and not the actual size of the constructed string!
                // In both cases it  writes more bytes because the size of key in orders table is 3 64-bit uints and thus occupies 24 bytes even for small numbered kes.
                // Experiment with converting to Str (call to_str()), and then we will only write as many digits as the key. A small key would only be 3 bytes (1 digit per key part: warehouse, district, o_id).
                //auto callback = [] (logrec_kv* lr) -> void {
                //};
                #if LOG_RECS == 1 || LOG_RECS == 3
                #if LOGREC_OVERWRITE
                int log_indx = logger_tbl->get_log_index();
                //std::cout<<"indx: "<< log_indx<<std::endl;
                internal_elem_log* el = reinterpret_cast<internal_elem_log*>(e);
                //if(cmd == logcmd_replace && el->log_pos[log_indx] >= 0){ // specify the location of the log record! This will overwrite existing log record for this key
                // TODO: either >=0, or >0, depending on how we initialize the log_pos!
                if(cmd == logcmd_replace &&  el->get_creation_epoch(log_indx) == qtimes.epoch.value()){ // specify the location of the log record! This will overwrite existing log record for this key, if they are for the same epoch
                    // make sure to check whether e->log_pos[log_indx] exists. It could be that the log command is replace (update_row), but it does not exist in the log! In case we apply the log entries after DB prepopulation and clear the log!
                    //logger_tbl->record(cmd, qtimes, Str(el->key), Str(*(valp==nullptr?v : valp)), tbl_index_,  el->log_pos[log_indx] );
                    // TODO: for now do not overwrite, just to test the overhead of storing the log pos and epoch!
                    el->update_log_pos(log_indx, logger_tbl->record(cmd, qtimes, Str(el->key), Str(*(valp==nullptr?v : valp)), tbl_index_, el->get_log_pos(log_indx)));
                    //logger_tbl->record(cmd, qtimes, Str(el->key), Str(*(valp==nullptr?v : valp)), tbl_index_);
                }
                else if (el->get_creation_epoch(log_indx) == 0) // there is no log record location for this key (creation epoch =0), store it!
                    el->set_log_pos(log_indx, logger_tbl->record(cmd, qtimes, Str(el->key), Str(*(valp==nullptr?v : valp)), tbl_index_), qtimes.epoch.value());
                else // different epoch, do not update!
                    logger_tbl->record(cmd, qtimes, Str(el->key), Str(*(valp==nullptr?v : valp)), tbl_index_);
                #else
                logger_tbl->record(cmd, qtimes, Str(e->key), Str(*(valp==nullptr?v : valp)), tbl_index_);
                #endif
                #if LOGGING == 2 && LOG_DRAIN_REQUEST
                    if(signal_logdrainer){
                        // add new epoch log record to all tables!
                        for (int i=0; i<LOG_NTABLES; i++){
                            if(logger_tbl->current_size(i) > 0 && i != tbl_index_)
                                logger_tbl->record_new_epoch(i, qtimes.epoch);
                        }
                        //if(logger_tbl->get_log_index() == 0) // do not signal the first log drainer right away otherwise it will miss the signal!
                            usleep(10);
                        //std::cout<<"Signaling log drainer!\n";
                        logger_tbl->signal_to_logdrain(); // signal the cond variable that the log drainer is waiting on. It should be signaled right after we record a log record with the new log epoch (which is not visible to the log drainer)
                        signal_logdrainer = false;
                    }
                #endif
                #elif LOG_RECS == 2
                const char* k = e->key.to_str();
                const char* val = (valp==nullptr?v : valp)->to_str();
                reinterpret_cast<loginfo_tbl<LOG_NTABLES>*>(logger)->record(cmd, qtimes, k, strlen(k), val, strlen(val), tbl_index_);
                delete[] k;
                delete[] val;
                #endif
                //reinterpret_cast<loginfo_tbl<LOG_NTABLES>*>(logger)->record(cmd, qtimes, &e->key, sizeof(e->key), (valp==nullptr?v : valp), sizeof(value_type), tbl_index_);
            }
            else if(LOG == 3)
                reinterpret_cast<loginfo_map<LOG_NTABLES>*>(logger)->record(cmd, qtimes, Str(e->key), Str(*(valp==nullptr?v : valp)), tbl_index_);
        }
    }

    void install(TransItem& item, Transaction& txn) override {
        assert(!is_internode(item));
        
        if constexpr (table_params::track_nodes) {
            if (is_ttnv(item)) {
                auto n = get_internode_address(item);
                txn.set_version_unlock(*static_cast<leaf_type*>(n)->get_aux_tracker(), item);
                return;
            }
        }

        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();
        #if ADD_TO_LOG == 2
        kvtimestamp_t ts;
        value_type v;
        #endif

        if (key.is_row_item()) {
            //assert(e->version.is_locked());
            if (has_delete(item)) {
                assert(e->valid() && !e->deleted);
                e->deleted = true;
                txn.set_version(e->version());
                return;
            }

            if (!has_insert(item)) {
                if (item.has_commute()) {
                    comm_type &comm = item.write_value<comm_type>();
                    if (has_row_update(item)) {
                        copy_row(e, comm);
                    } else if (has_row_cell(item)) {
                        e->row_container.install_cell(comm);
                    }
                } else {
                    value_type *vptr;
                    if (value_is_small) {
                        vptr = &(item.write_value<value_type>());
                    } else {
                        vptr = item.write_value<value_type *>();
                    }

                    if (has_row_update(item)) {
                        if (value_is_small) {
                            e->row_container.row = *vptr;
                        } else {
                            copy_row(e, vptr);
                        }
                    } else if (has_row_cell(item)) {
                        // install only the difference part
                        // not sure if works when there are more than 1 minor version fields
                        // should still work
                        e->row_container.install_cell(0, vptr);
                    }
                }
            }
            else{
                #if ADD_TO_LOG == 2
                v = e->row_container.row; // copy it now because we can't access e after unlocking!
                #endif
            }

            // We need to use the version after we unlock. However, someone else might lock that element and change the version.
            // That's why we get the value that was set without the need to go back to the internal element after we unlock it.
            //txn.set_version_unlock(e->version(), item);
            auto vers = txn.set_version_unlock_return(e->version(), item);
            // we assume that the element won't be locked until the next instruction.
            #if ADD_TO_LOG == 2
            // Do not get the version now! It might have changed by a concurrent thread!
            //ts = e->version().value();
            ts = vers;
            #endif
        
            // add to log here, so that we already have the updated version.
            #if ADD_TO_LOG == 2
                if(LOG > 0 && has_log_add(item))
                    log_add(item, e, &v, ts);
            #endif
        } else {
            // skip installation if row-level update is present
            auto row_item = Sto::item(this, item_key_t::row_item_key(e));
            if (!has_row_update(row_item)) {
                if (row_item.has_commute()) {
                    comm_type &comm = row_item.template write_value<comm_type>();
                    assert(&comm);
                    e->row_container.install_cell(comm);
                } else {
                    value_type *vptr;
                    if (value_is_small)
                        vptr = &(row_item.template raw_write_value<value_type>());
                    else
                        vptr = row_item.template raw_write_value<value_type *>();

                    e->row_container.install_cell(key.cell_num(), vptr);
                }
            }
            // Adding to Log not supported for non row-item
            txn.set_version_unlock(e->row_container.version_at(key.cell_num()), item);
        }
    }

    void unlock(TransItem& item) override {
        assert(!is_internode(item));
        if constexpr (table_params::track_nodes) {
            if (is_ttnv(item)) {
                auto n = get_internode_address(item);
                static_cast<leaf_type*>(n)->get_aux_tracker()->cp_unlock(item);
                return;
            }
        }
        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();
        if (key.is_row_item())
            e->version().cp_unlock(item);
        else
            e->row_container.version_at(key.cell_num()).cp_unlock(item);
        //TODO (LOG): Add to log here, after unlocking. We might want to use the version before the unlock though (for log_rec->ts).
    }

    void cleanup(TransItem& item, bool committed) override {
        #if ADD_TO_LOG == 1
        if(LOG > 0 && committed && has_log_add(item)){
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            log_add(item, e, e->version().value());
        }
        #endif

        if (committed ? has_delete(item) : has_insert(item)) {
            auto key = item.key<item_key_t>();
            assert(key.is_row_item());
            internal_elem *e = key.internal_elem_ptr();
            bool ok = _remove(e->key);
            if (!ok) {
                std::cout << committed << "," << has_delete(item) << "," << has_insert(item) << std::endl;
                always_assert(false, "insert-bit exclusive ownership violated");
            }
            item.clear_needs_unlock();
        }
    }

protected:
    template <typename NodeCallback, typename ValueCallback, bool Reverse>
    class range_scanner {
    public:
        range_scanner(const Str upper, NodeCallback ncb, ValueCallback vcb, int limit) :
            boundary_(upper), boundary_compar_(false), scan_succeeded_(true), limit_(limit), scancount_(0),
            node_callback_(ncb), value_callback_(vcb) {}

        template <typename ITER, typename KEY>
        void check(const ITER& iter, const KEY& key) {
            int min = std::min(boundary_.length(), key.prefix_length());
            int cmp = memcmp(boundary_.data(), key.full_string().data(), min);
            if (!Reverse) {
                if (cmp < 0 || (cmp == 0 && boundary_.length() <= key.prefix_length()))
                    boundary_compar_ = true;
                else if (cmp == 0) {
                    uint64_t last_ikey = iter.node()->ikey0_[iter.permutation()[iter.permutation().size() - 1]];
                    uint64_t slice = string_slice<uint64_t>::make_comparable(boundary_.data() + key.prefix_length(),
                        std::min(boundary_.length() - key.prefix_length(), 8));
                    boundary_compar_ = (slice <= last_ikey);
                }
            } else {
                if (cmp >= 0)
                    boundary_compar_ = true;
            }
        }

        template <typename ITER>
        void visit_leaf(const ITER& iter, const Masstree::key<uint64_t>& key, threadinfo&) {
            if (!node_callback_(iter.node(), iter.full_version_value())) {
                scan_succeeded_ = false;
            }
            if (this->boundary_) {
                check(iter, key);
            }
        }

        bool visit_value(const Masstree::key<uint64_t>& key, internal_elem *e, threadinfo&) {
            if (this->boundary_compar_) {
                if ((Reverse && (boundary_ >= key.full_string())) ||
                    (!Reverse && (boundary_ <= key.full_string())))
                    return false;
            }
            bool visited = false;
            bool count = true;
            if (!value_callback_(key.full_string(), e, visited, count)) {
                scan_succeeded_ = false;
                if (count) {++scancount_;}
                return false;
            } else {
                if (!visited)
                    scan_succeeded_ = false;
                if (count) {++scancount_;}
                if (limit_ > 0 && scancount_ >= limit_) {
                    return false;
                }
                return visited;
            }
        }

        Str boundary_;
        bool boundary_compar_;
        bool scan_succeeded_;
        int limit_;
        int scancount_;

        NodeCallback node_callback_;
        ValueCallback value_callback_;
    };

private:
    table_type table_;
    uint64_t key_gen_;

    static bool
    access_all(std::array<access_t, value_container_type::num_versions>& cell_accesses, std::array<TransItem*, value_container_type::num_versions>& cell_items, value_container_type& row_container) {
        for (size_t idx = 0; idx < cell_accesses.size(); ++idx) {
            auto& access = cell_accesses[idx];
            auto proxy = TransProxy(*Sto::transaction(), *cell_items[idx]);
            if (static_cast<uint8_t>(access) & static_cast<uint8_t>(access_t::read)) {
                if (!proxy.observe(row_container.version_at(idx)))
                    return false;
            }
            if (static_cast<uint8_t>(access) & static_cast<uint8_t>(access_t::write)) {
                if (!proxy.acquire_write(row_container.version_at(idx)))
                    return false;
                if (proxy.item().key<item_key_t>().is_row_item()) {
                    proxy.item().add_flags(row_cell_bit);
                }
            }
        }
        return true;
    }

    static bool has_insert(const TransItem& item) {
        return (item.flags() & insert_bit) != 0;
    }
    static bool has_delete(const TransItem& item) {
        return (item.flags() & delete_bit) != 0;
    }
    static bool has_row_update(const TransItem& item) {
        return (item.flags() & row_update_bit) != 0;
    }
    static bool has_row_cell(const TransItem& item) {
        return (item.flags() & row_cell_bit) != 0;
    }
    static bool is_phantom(internal_elem *e, const TransItem& item) {
        return (!e->valid() && !has_insert(item));
    }
    static bool has_log_add(const TransItem& item){
        return (item.flags() & log_add_bit) != 0;
    }

    bool register_internode_version(node_type *node, unlocked_cursor_type& cursor) {
        if constexpr (table_params::track_nodes) {
            return ttnv_register_node_read_with_snapshot(node, *cursor.get_aux_tracker());
        } else {
            TransProxy item = Sto::item(this, get_internode_key(node));
            if constexpr (DBParams::Opaque) {
                return item.add_read_opaque(cursor.full_version_value());
            } else {
                return item.add_read(cursor.full_version_value());
            }
        }
    }

    // Used in scan helpers to track leaf node timestamps for phantom protection.
    bool scan_track_node_version(node_type *node, nodeversion_value_type nodeversion) {
        if constexpr (table_params::track_nodes) {
            (void)nodeversion;
            return ttnv_register_node_read(node);
        } else {
            TransProxy item = Sto::item(this, get_internode_key(node));
            if constexpr (DBParams::Opaque) {
                return item.add_read_opaque(nodeversion);
            } else {
                return item.add_read(nodeversion);
            }
        }
    }

    bool update_internode_version(node_type *node,
            nodeversion_value_type prev_nv, nodeversion_value_type new_nv) {
        ttnv_register_node_write(node);
        TransProxy item = Sto::item(this, get_internode_key(node));
        if (!item.has_read()) {
            return true;
        }
        if (prev_nv == item.template read_value<nodeversion_value_type>()) {
            item.update_read(prev_nv, new_nv);
            return true;
        }
        return false;
    }

    void ttnv_register_node_write(node_type* node) {
        (void)node;
        if constexpr (table_params::track_nodes) {
            static_assert(DBParams::TicToc, "Node tracking requires TicToc.");
            always_assert(node->isleaf(), "Tracking non-leaf node!!");
            auto tt_item = Sto::item(this, get_ttnv_key(node));
            tt_item.acquire_write(*static_cast<leaf_type*>(node)->get_aux_tracker());
        }
    }

    bool ttnv_register_node_read_with_snapshot(node_type* node, typename table_params::aux_tracker_type& snapshot) {
        (void)node; (void)snapshot;
        if constexpr (table_params::track_nodes) {
            static_assert(DBParams::TicToc, "Node tracking requires TicToc.");
            always_assert(node->isleaf(), "Tracking non-leaf node!!");
            auto tt_item = Sto::item(this, get_ttnv_key(node));
            return tt_item.observe(*static_cast<leaf_type*>(node)->get_aux_tracker(), snapshot);
        } else {
            return true;
        }
    }

    bool ttnv_register_node_read(node_type* node) {
        (void)node;
        if constexpr (table_params::track_nodes) {
            static_assert(DBParams::TicToc, "Node tracking requires TicToc.");
            always_assert(node->isleaf(), "Tracking non-leaf node!!");
            auto tt_item = Sto::item(this, get_ttnv_key(node));
            return tt_item.observe(*static_cast<leaf_type*>(node)->get_aux_tracker());
        } else {
            return true;
        }
    }

    bool _remove(const key_type& key) {
        cursor_type lp(table_, key);
        bool found = lp.find_locked(*ti);
        if (found) {
            internal_elem *el = lp.value();
            lp.finish(-1, *ti);
            Transaction::rcu_delete(el);
        } else {
            // XXX is this correct?
            lp.finish(0, *ti);
        }
        return found;
    }

    static uintptr_t get_internode_key(node_type* node) {
        return reinterpret_cast<uintptr_t>(node) | internode_bit;
    }
    static bool is_internode(TransItem& item) {
        return (item.key<uintptr_t>() & internode_bit) != 0;
    }
    static node_type *get_internode_address(TransItem& item) {
        if (is_internode(item)) {
            return reinterpret_cast<node_type *>(item.key<uintptr_t>() & ~internode_bit);
        } else if (is_ttnv(item)) {
            return reinterpret_cast<node_type *>(item.key<uintptr_t>() & ~ttnv_bit);
        }
        assert(false);
        return nullptr;
    }

    static uintptr_t get_ttnv_key(node_type* node) {
        return reinterpret_cast<uintptr_t>(node) | ttnv_bit;
    }
    static bool is_ttnv(TransItem& item) {
        return (item.key<uintptr_t>() & ttnv_bit);
    }

    static void copy_row(internal_elem *e, comm_type &comm) {
        e->row_container.row = comm.operate(e->row_container.row);
    }
    static void copy_row(internal_elem *e, const value_type *new_row) {
        if (new_row == nullptr)
            return;
        e->row_container.row = *new_row;
    }
};

template <typename K, typename V, typename DBParams, short LOG>
__thread typename ordered_index<K, V, DBParams, LOG>::table_params::threadinfo_type
*ordered_index<K, V, DBParams, LOG>::ti;

template <typename K, typename V, typename DBParams>
class mvcc_ordered_index : public TObject {

    // the index of this table in the log buffer
    int tbl_index_=0;

public:
    typedef K key_type;
    typedef V value_type;
    typedef MvObject<value_type> object_type;
    typedef typename object_type::history_type history_type;
    typedef commutators::Commutator<value_type> comm_type;

    static constexpr TransItem::flags_type insert_bit = TransItem::user0_bit;
    static constexpr TransItem::flags_type delete_bit = TransItem::user0_bit << 1u;
    static constexpr TransItem::flags_type row_update_bit = TransItem::user0_bit << 2u;
    static constexpr TransItem::flags_type row_cell_bit = TransItem::user0_bit << 3u;
    static constexpr uintptr_t internode_bit = 1;

    typedef typename value_type::NamedColumn NamedColumn;

    static constexpr bool index_read_my_write = DBParams::RdMyWr;

    struct internal_elem {
        static constexpr size_t num_versions = 1;
        key_type key;
        object_type row;

        internal_elem(const key_type& k)
            : key(k), row() {}
        internal_elem(const key_type& k, const value_type& val)
            : key(k), row(val) {}

        constexpr static int map(int col_n) {
            (void)col_n;  // TODO(column-splitting)
            return 0;
        }
    };

    struct table_params : public Masstree::nodeparams<15,15> {
        typedef internal_elem* value_type;
        typedef Masstree::value_print<value_type> value_print_type;
        typedef threadinfo threadinfo_type;
    };

    typedef Masstree::Str Str;
    typedef Masstree::basic_table<table_params> table_type;
    typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
    typedef Masstree::tcursor<table_params> cursor_type;
    typedef Masstree::leaf<table_params> leaf_type;

    typedef typename table_type::node_type node_type;
    typedef typename unlocked_cursor_type::nodeversion_value_type nodeversion_value_type;

    typedef std::tuple<bool, bool, uintptr_t, const value_type*> sel_return_type;
    typedef std::tuple<bool, bool>                               ins_return_type;
    #if TPCH_SINGLE_NODE
    typedef std::tuple<bool, bool, internal_elem*>               ins_elem_return_type;
    #endif
    typedef std::tuple<bool, bool>                               del_return_type;

    using index_t = mvcc_ordered_index<K, V, DBParams>;
    using column_access_t = typename split_version_helpers<index_t>::column_access_t;
    using item_key_t = typename split_version_helpers<index_t>::item_key_t;
    template <typename T> static constexpr auto column_to_cell_accesses =
        split_version_helpers<index_t>::template column_to_cell_accesses<T>;
    template <typename T> static constexpr auto extract_item_list =
        split_version_helpers<index_t>::template extract_item_list<T>;

    static __thread typename table_params::threadinfo_type *ti;

    mvcc_ordered_index(size_t init_size) : tbl_index_(-1) {
        this->table_init();
        (void)init_size;
    }
    mvcc_ordered_index() : tbl_index_(-1){
        this->table_init();
    }

    // tbl_index: the index of this table in the log buffer
    mvcc_ordered_index(size_t init_size, int tbl_index) : tbl_index_(tbl_index) {
        this->table_init();
        (void)init_size;
        assert(tbl_index>=0);  //&& tbl_index <= logs_tbl->log(runner_num). getNumtbl ?? )
    }

    void table_init() {
        static_assert(DBParams::Opaque, "MVCC must operate in opaque mode.");
        if (ti == nullptr)
            ti = threadinfo::make(threadinfo::TI_MAIN, -1);
        table_.initialize(*ti);
        key_gen_ = 0;
    }

    static void thread_init(){
        thread_init(0);
    }

    // runner_num: the number of this thread [0-num_runners), since it can be different than TThread::id()!
    static void thread_init(int runner_num) {
        (void)runner_num;
        if (ti == nullptr)
            ti = threadinfo::make(threadinfo::TI_PROCESS, TThread::id());
        Transaction::tinfo[TThread::id()].trans_start_callback = []() {
            ti->rcu_start();
        };
        Transaction::tinfo[TThread::id()].trans_end_callback = []() {
            ti->rcu_stop();
        };
        // Dimos - logging - no logging in MVCC
        /*if(logging == 1){
            if(!ti->logger() && logs){ // make sure logs are initialized (they are only initialized once the TPC-C benchmark starts running ; they are not initialized during the prepopulation phase)
                ti->set_logger(& (reinterpret_cast<logset*>(logs))->log(runner_num));
            }
        }
        else if(logging == 2){
            if(!ti->logger_tbl() && logs)
                ti->set_logger(&(reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs))->log(runner_num));
        }
        else if(logging == 3){
            if(!ti->logger_tbl() && logs)
                ti->set_logger(&(reinterpret_cast<logset_map<LOG_NTABLES>*>(logs))->log(runner_num));
        }*/
    }

    uint64_t gen_key() {
        return fetch_and_add(&key_gen_, 1);
    }

    sel_return_type
    select_row(const key_type& key, RowAccess acc) {
        unlocked_cursor_type lp(table_, key);
        bool found = lp.find_unlocked(*ti);
        internal_elem *e = lp.value();
        if (found) {
            return select_row(reinterpret_cast<uintptr_t>(e), acc);
        } else {
            if (!register_internode_version(lp.node(), lp.full_version_value()))
                goto abort;
            return sel_return_type(true, false, 0, nullptr);
        }

    abort:
        return sel_return_type(false, false, 0, nullptr);
    }

    sel_return_type
    select_row(const key_type& key, std::initializer_list<column_access_t> accesses) {
        unlocked_cursor_type lp(table_, key);
        bool found = lp.find_unlocked(*ti);
        internal_elem *e = lp.value();
        if (found) {
            return select_row(reinterpret_cast<uintptr_t>(e), accesses);
        } else {
            if (!register_internode_version(lp.node(), lp.full_version_value()))
                return sel_return_type(false, false, 0, nullptr);
            return sel_return_type(true, false, 0, nullptr);
        }

        return sel_return_type(false, false, 0, nullptr);
    }

    sel_return_type
    select_row(uintptr_t rid, RowAccess access) {
        auto e = reinterpret_cast<internal_elem *>(rid);
        TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

        history_type *h = e->row.find(txn_read_tid());
        //history_type *h = e->row.find(txn_read_tid(), false);

        if (h->status_is(UNUSED)) {
            return sel_return_type(true, false, 0, nullptr);
        }

        if (is_phantom(h, row_item))
            return sel_return_type(true, false, 0, nullptr);

        if (index_read_my_write) {
            if (has_delete(row_item)) {
                return sel_return_type(true, false, 0, nullptr);
            }
            if (has_row_update(row_item)) {
                value_type *vptr;
                if (has_insert(row_item)) {
#if SAFE_FLATTEN
                    vptr = h->vp_safe_flatten();
                    if (vptr == nullptr)
                        return { false, false, 0, nullptr };
#else
                    vptr = h->vp();
#endif
                } else {
                    vptr = row_item.template raw_write_value<value_type *>();
                }
                assert(vptr);
                return sel_return_type(true, true, rid, vptr);
            }
        }

        if (access != RowAccess::None) {
            MvAccess::template read<value_type>(row_item, h);
#if SAFE_FLATTEN
            auto vp = h->vp_safe_flatten();
            if (vp == nullptr)
                return { false, false, 0, nullptr };
#else
            auto vp = h->vp();
            assert(vp);
#endif
            return sel_return_type(true, true, rid, vp);
        } else {
            return sel_return_type(true, true, rid, nullptr);
        }
    }

    sel_return_type
    select_row(uintptr_t, std::initializer_list<column_access_t>) {
        always_assert(false, "Not implemented in MVCC, use split table instead.");
        return { false, false, 0, nullptr };
    }

    // Log is not supported (and not required) on MVCC.
    void log_add(enum logcommand cmd, Str& key, Str& val, loginfo::query_times& qtimes){
        (void)cmd;
        (void)key;
        (void)val;
        (void)qtimes;
    }

    void update_row(uintptr_t rid, value_type* new_row) {
        auto row_item = Sto::item(this, item_key_t::row_item_key(reinterpret_cast<internal_elem *>(rid)));
        // TODO: address this extra copying issue
        row_item.add_write(new_row);
        // Just update the pointer, don't set the actual write flag
        // we don't want to confuse installs at commit time
        //row_item.clear_write();
    }

    void update_row(uintptr_t rid, const comm_type &comm) {
        auto row_item = Sto::item(this, item_key_t::row_item_key(reinterpret_cast<internal_elem *>(rid)));
        // TODO: address this extra copying issue
        row_item.add_commute(comm);
    }

    // insert assumes common case where the row doesn't exist in the table
    // if a row already exists, then use select (FOR UPDATE) instead
    // we need to store the pointer to the internal_elem, required for the secondary indexes!
    ins_return_type
    insert_row(const key_type& key, value_type *vptr, bool overwrite = false, bool append=false, bool store_elem=false, uint64_t**el=nullptr) {
        cursor_type lp(table_, key);
        bool found = lp.find_insert(*ti);
        if (found) {
            // NB: the insert method only manipulates the row_item. It is possible
            // this insert is overwriting some previous updates on selected columns
            // The expected behavior is that this row-level operation should overwrite
            // all changes made by previous updates (in the same transaction) on this
            // row. We achieve this by granting this row_item a higher priority.
            // During the install phase, if we notice that the row item has already
            // been locked then we simply ignore installing any changes made by cell items.
            // It should be trivial for a cell item to find the corresponding row item
            // and figure out if the row-level version is locked.
            internal_elem *e = lp.value();
            if(store_elem)
                *el = reinterpret_cast<uint64_t*>(e);
            lp.finish(0, *ti);

            TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

            auto h = e->row.find(txn_read_tid());
            //auto h = e->row.find(txn_read_tid(), false);
            if (is_phantom(h, row_item))
                return ins_return_type(true, false);

            if (index_read_my_write) {
                if (has_delete(row_item)) {
                    auto proxy = row_item.clear_flags(delete_bit).clear_write();
                    proxy.add_write(*vptr);
                    return ins_return_type(true, false);
                }
            }

            if (overwrite) {
                row_item.add_write(*vptr);
            } else {
                // TODO: This now acts like a full read of the value
                // at rtid. Once we add predicates we can change it to
                // something else.
                MvAccess::template read<value_type>(row_item, h);
            }

            if(append){
                if (h->status_is(UNUSED)) {
                    return ins_return_type(true, false);
                }
                #if SAFE_FLATTEN
                    auto vp = h->vp_safe_flatten();
                    if (vp == nullptr)
                        return { false, false};
                #else
                    auto vp = h->vp();
                    assert(vp);
                    // append vp into vptr
                    // 1. traverse list in vptr until next == nullptr and insert vp there
                    // 2. done
                #endif
            }
        } else {
            auto e = new internal_elem(key);
            lp.value() = e;

            node_type *node;
            nodeversion_value_type orig_nv;
            nodeversion_value_type new_nv;

            bool split_right = (lp.node() != lp.original_node());
            if (split_right) {
                node = lp.original_node();
                orig_nv = lp.original_version_value();
                new_nv = lp.updated_version_value();
            } else {
                node = lp.node();
                orig_nv = lp.previous_full_version_value();
                new_nv = lp.next_full_version_value(1);
            }

            fence();
            if(store_elem)
                *el = reinterpret_cast<uint64_t*>(e);
            lp.finish(1, *ti);
            //fence();

            TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));
            row_item.add_write(vptr);
            row_item.add_flags(insert_bit);

            // update the node version already in the read set and modified by split
            if (!update_internode_version(node, orig_nv, new_nv))
                goto abort;
        }

        return ins_return_type(true, found);

    abort:
        return ins_return_type(false, false);
    }

    del_return_type
    delete_row(const key_type& key) {
        unlocked_cursor_type lp(table_, key);
        bool found = lp.find_unlocked(*ti);
        if (found) {
            internal_elem *e = lp.value();
            TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

            auto h = e->row.find(txn_read_tid());
            //auto h = e->row.find(txn_read_tid(), false);

            if (is_phantom(h, row_item))
                return del_return_type(true, false);

            if (index_read_my_write) {
                if (has_delete(row_item))
                    return del_return_type(true, false);
                if (h->status_is(DELETED) && has_insert(row_item)) {
                    row_item.add_flags(delete_bit);
                    return del_return_type(true, true);
                }
            }

            MvAccess::template read<value_type>(row_item, h);
            if (h->status_is(DELETED))
                return del_return_type(true, false);
            row_item.add_write(0);
            row_item.add_flags(delete_bit);
        } else {
            if (!register_internode_version(lp.node(), lp.full_version_value()))
                goto abort;
        }

        return del_return_type(true, found);

    abort:
        return del_return_type(false, false);
    }

    template <typename Callback, bool Reverse>
    bool range_scan(const key_type&, const key_type&, Callback,
                    std::initializer_list<column_access_t>, bool phantom_protection = true, int limit = -1) {
        (void)phantom_protection; (void)limit;
        always_assert(false, "Not implemented in MVCC, use split table instead.");
    }


    template <typename Callback, bool Reverse>
    bool range_scan(const key_type& begin, const key_type& end, Callback callback,
                    RowAccess access, bool phantom_protection = true, int limit = -1) {
                        return this->template range_scan<Callback, Reverse>(false, begin, end, callback, access, phantom_protection, limit);
    }

    template <typename Callback, bool Reverse>
    bool nontrans_range_scan(const key_type& begin, const key_type& end, Callback callback, int limit = -1) {
        (void)begin;
        (void)end;
        (void)callback;
        (void)limit;
        always_assert(false, "Non-transactional range scan not supported in MVCC");
        return true;
    }

    template <typename Callback, bool Reverse>
    bool range_scan(bool readOnly, const key_type& begin, const key_type& end, Callback callback,
                    RowAccess access, bool phantom_protection = true, int limit = -1) {
        (void)access;  // TODO: Scan ignores writes right now
        assert((limit == -1) || (limit > 0));
        auto node_callback = [&] (leaf_type* node,
                                  typename unlocked_cursor_type::nodeversion_value_type version) {
            return ((!phantom_protection) || register_internode_version(node, version));
        };

        auto value_callback = [&] (const lcdf::Str& key, internal_elem *e, bool& ret, bool& count) {
            auto h = e->row.find(txn_read_tid());
            //auto h = e->row.find(txn_read_tid(), false); // it is not correct to not wait for PENDING! This can result to some elements from the future and some from the past! (different snapshots)
            // skip invalid (inserted but yet committed) and/or deleted values, but do not abort
            if (h->status_is(DELETED)) {
                ret = true;
                count = false;
                return true;
            }

            // If it is a read-only transaction, no need for TransItems!
            if (!readOnly){
                TransProxy row_item = index_read_my_write ? Sto::item(this, item_key_t::row_item_key(e))
                                                        : Sto::fresh_item(this, item_key_t::row_item_key(e));

                if (index_read_my_write) {
                    if (has_delete(row_item)) {
                        ret = true;
                        count = false;
                        return true;
                    }
                    if (has_row_update(row_item)) {
                        ret = callback(key_type(key), *(row_item.template raw_write_value<value_type *>()));
                        return true;
                    }
                }

                MvAccess::template read<value_type>(row_item, h);
            }

#if SAFE_FLATTEN
            auto vptr = h->vp_safe_flatten();
            if (vptr == nullptr) {
                ret = false;
                return false;
            }
#else
            auto vptr = h->vp();
#endif
            ret = callback(key_type(key), *vptr);
            return true;
        };

        range_scanner<decltype(node_callback), decltype(value_callback), Reverse>
                scanner(end, node_callback, value_callback, limit);
        if (Reverse)
            table_.rscan(begin, true, scanner, *ti);
        else
            table_.scan(begin, true, scanner, *ti);
        return scanner.scan_succeeded_;
    }

    value_type *nontrans_get(const key_type& k) {
        unlocked_cursor_type lp(table_, k);
        bool found = lp.find_unlocked(*ti);
        if (found) {
            internal_elem *e = lp.value();
            return &(e->row.nontrans_access());
        } else
            return nullptr;
    }
    
    std::tuple<value_type*, uint64_t*> nontrans_get_el(const key_type & k){
        unlocked_cursor_type lp(table_, k);
        bool found = lp.find_unlocked(*ti);
        if (found) {
            internal_elem *e = lp.value();
            return std::make_tuple(&(e->row.nontrans_access()), (uint64_t*)e);
        } else
            return std::make_tuple(nullptr, nullptr);
    }

    // Dimos: We need to know the location where this <k,v> was inserted (internal_elem*) so that to 
    // store it in the secondary index for TPC-H
    uint64_t * nontrans_put_el(const key_type& k, const value_type& v) {
        cursor_type lp(table_, k);
        bool found = lp.find_insert(*ti);
        if (found) {
            internal_elem *e = lp.value();
            e->row.nontrans_access() = v;
            lp.finish(0, *ti);
            return reinterpret_cast<uint64_t*>(e);
        } else {
            internal_elem *e = new internal_elem(k, v);
            e->row.nontrans_access() = v;
            lp.value() = e;
            lp.finish(1, *ti);
            return reinterpret_cast<uint64_t*>(e);
        }
    }
    void nontrans_put(const key_type& k, const value_type& v) {
        cursor_type lp(table_, k);
        bool found = lp.find_insert(*ti);
        if (found) {
            internal_elem *e = lp.value();
            e->row.nontrans_access() = v;
            lp.finish(0, *ti);
        } else {
            internal_elem *e = new internal_elem(k, v);
            e->row.nontrans_access() = v;
            lp.value() = e;
            lp.finish(1, *ti);
        }
    }

    // TObject interface methods
    bool lock(TransItem& item, Transaction& txn) override {
        assert(!is_internode(item));
        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();

        history_type *hprev = nullptr;
        if (item.has_read()) {
            hprev = item.read_value<history_type*>();
            if (Sto::commit_tid() < hprev->rtid()) {
                TransProxy(txn, item).add_write(nullptr);
                TXP_ACCOUNT(txp_tpcc_lock_abort1, txn.special_txp);
                return false;
            }
        }
        history_type *h = nullptr;
        if (item.has_commute()) {
            auto wval = item.template write_value<comm_type>();
            h = e->row.new_history(
                Sto::commit_tid(), &e->row, std::move(wval), hprev);
        } else {
            auto wval = item.template raw_write_value<value_type*>();
            if (has_delete(item)) {
                h = e->row.new_history(
                    Sto::commit_tid(), &e->row, nullptr, hprev);
                h->status_delete();
                h->set_delete_cb(this, _delete_cb, e);
            } else {
                h = e->row.new_history(
                    Sto::commit_tid(), &e->row, wval, hprev);
            }
        }
        assert(h);
        bool result = e->row.cp_lock(Sto::commit_tid(), h);
        if (!result && !h->status_is(MvStatus::ABORTED)) {
            e->row.delete_history(h);
            TransProxy(txn, item).add_mvhistory(nullptr);
            TXP_ACCOUNT(txp_tpcc_lock_abort2, txn.special_txp);
        } else {
            TransProxy(txn, item).add_mvhistory(h);
            TXP_ACCOUNT(txp_tpcc_lock_abort3, txn.special_txp && !result);
        }
        return result;
    }

    bool check(TransItem& item, Transaction& txn) override {
        if (is_internode(item)) {
            node_type *n = get_internode_address(item);
            auto curr_nv = static_cast<leaf_type *>(n)->full_version_value();
            auto read_nv = item.template read_value<decltype(curr_nv)>();
            auto result = (curr_nv == read_nv);
            TXP_ACCOUNT(txp_tpcc_check_abort1, txn.special_txp && !result);
            return result;
        } else {
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            auto h = item.template read_value<history_type*>();
            auto result = e->row.cp_check(txn_read_tid(), h);
            TXP_ACCOUNT(txp_tpcc_check_abort2, txn.special_txp && !result);
            return result;
        }
    }

    void install(TransItem& item, Transaction&) override {
        assert(!is_internode(item));
        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();
        auto h = item.template write_value<history_type*>();

        e->row.cp_install(h);
    }

    void unlock(TransItem& item) override {
        (void)item;
        assert(!is_internode(item));
    }

    void cleanup(TransItem& item, bool committed) override {
        if (!committed) {
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            if (item.has_mvhistory()) {
                auto h = item.template write_value<history_type*>();
                if (h) {
                    e->row.abort(h);
                }
            }
        }
    }

protected:
    template <typename NodeCallback, typename ValueCallback, bool Reverse>
    class range_scanner {
    public:
        range_scanner(const Str upper, NodeCallback ncb, ValueCallback vcb, int limit) :
            boundary_(upper), boundary_compar_(false), scan_succeeded_(true), limit_(limit), scancount_(0),
            node_callback_(ncb), value_callback_(vcb) {}

        template <typename ITER, typename KEY>
        void check(const ITER& iter, const KEY& key) {
            int min = std::min(boundary_.length(), key.prefix_length());
            int cmp = memcmp(boundary_.data(), key.full_string().data(), min);
            if (!Reverse) {
                if (cmp < 0 || (cmp == 0 && boundary_.length() <= key.prefix_length()))
                    boundary_compar_ = true;
                else if (cmp == 0) {
                    uint64_t last_ikey = iter.node()->ikey0_[iter.permutation()[iter.permutation().size() - 1]];
                    uint64_t slice = string_slice<uint64_t>::make_comparable(boundary_.data() + key.prefix_length(),
                        std::min(boundary_.length() - key.prefix_length(), 8));
                    boundary_compar_ = (slice <= last_ikey);
                }
            } else {
                if (cmp >= 0)
                    boundary_compar_ = true;
            }
        }

        template <typename ITER>
        void visit_leaf(const ITER& iter, const Masstree::key<uint64_t>& key, threadinfo&) {
            if (!node_callback_(iter.node(), iter.full_version_value())) {
                scan_succeeded_ = false;
            }
            if (this->boundary_) {
                check(iter, key);
            }
        }

        bool visit_value(const Masstree::key<uint64_t>& key, internal_elem *e, threadinfo&) {
            if (this->boundary_compar_) {
                if ((Reverse && (boundary_ >= key.full_string())) ||
                    (!Reverse && (boundary_ <= key.full_string())))
                    return false;
            }
            bool visited = false;
            bool count = true;
            if (!value_callback_(key.full_string(), e, visited, count)) {
                scan_succeeded_ = false;
                if (count) {++scancount_;}
                return false;
            } else {
                if (!visited)
                    scan_succeeded_ = false;
                if (count) {++scancount_;}
                if (limit_ > 0 && scancount_ >= limit_) {
                    return false;
                }
                return visited;
            }
        }

        Str boundary_;
        bool boundary_compar_;
        bool scan_succeeded_;
        int limit_;
        int scancount_;

        NodeCallback node_callback_;
        ValueCallback value_callback_;
    };

private:
    table_type table_;
    uint64_t key_gen_;

    static bool
    access_all(std::array<access_t, internal_elem::num_versions>&, std::array<TransItem*, internal_elem::num_versions>&, internal_elem*) {
        always_assert(false, "Not implemented.");
        return true;
    }

    static TransactionTid::type txn_read_tid() {
        return Sto::read_tid<DBParams::Commute>();
    }

    static bool has_insert(const TransItem& item) {
        return (item.flags() & insert_bit) != 0;
    }
    static bool has_delete(const TransItem& item) {
        return (item.flags() & delete_bit) != 0;
    }
    static bool has_row_update(const TransItem& item) {
        return (item.flags() & row_update_bit) != 0;
    }
    static bool has_row_cell(const TransItem& item) {
        return (item.flags() & row_cell_bit) != 0;
    }
    static bool is_phantom(const history_type *h, const TransItem& item) {
        return (h->status_is(DELETED) && !has_insert(item));
    }

    bool register_internode_version(node_type *node, nodeversion_value_type nodeversion) {
        TransProxy item = Sto::item(this, get_internode_key(node));
            return item.add_read(nodeversion);
    }
    bool update_internode_version(node_type *node,
            nodeversion_value_type prev_nv, nodeversion_value_type new_nv) {
        TransProxy item = Sto::item(this, get_internode_key(node));
        if (!item.has_read()) {
            return true;
        }
        if (prev_nv == item.template read_value<nodeversion_value_type>()) {
            item.update_read(prev_nv, new_nv);
            return true;
        }
        return false;
    }

    bool _remove(const key_type& key) {
        cursor_type lp(table_, key);
        bool found = lp.find_locked(*ti);
        if (found) {
            internal_elem *el = lp.value();
            lp.finish(-1, *ti);
            Transaction::rcu_delete(el);
        } else {
            // XXX is this correct?
            lp.finish(0, *ti);
        }
        return found;
    }

    static void _delete_cb(
            void *index_ptr, void *ele_ptr, void *history_ptr) {
        auto ip = reinterpret_cast<mvcc_ordered_index<K, V, DBParams>*>(index_ptr);
        auto el = reinterpret_cast<internal_elem*>(ele_ptr);
        auto hp = reinterpret_cast<history_type*>(history_ptr);
        cursor_type lp(ip->table_, el->key);
        bool found = lp.find_locked(*ip->ti);
        if (found) {
            if ((lp.value() == el) && el->row.is_head(hp)) {
                lp.finish(-1, *ip->ti);
                Transaction::rcu_delete(el);
            } else {
                lp.finish(0, *ip->ti);
            }
        } else {
            lp.finish(0, *ip->ti);
        }
    }

    static uintptr_t get_internode_key(node_type* node) {
        return reinterpret_cast<uintptr_t>(node) | internode_bit;
    }
    static bool is_internode(TransItem& item) {
        return (item.key<uintptr_t>() & internode_bit) != 0;
    }
    static node_type *get_internode_address(TransItem& item) {
        assert(is_internode(item));
        return reinterpret_cast<node_type *>(item.key<uintptr_t>() & ~internode_bit);
    }
};

template <typename K, typename V, typename DBParams>
__thread typename mvcc_ordered_index<K, V, DBParams>::table_params::threadinfo_type
*mvcc_ordered_index<K, V, DBParams>::ti;

} // namespace bench
