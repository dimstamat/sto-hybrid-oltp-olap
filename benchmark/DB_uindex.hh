#pragma once

class logset_base;
template <int N_TBLS>
class logset_tbl;

extern logset_base* logs;

#ifndef LOG_NTABLES // should be defined in TPCC_bench.hh
#define LOG_NTABLES 0
#endif

namespace bench {
// unordered index implemented as hashtable
template <typename K, typename V, typename DBParams, short LOG=0>
class unordered_index : public index_common<K, V, DBParams>, public TObject {

// Dimos - emable logging
// template argument LOG
// 0 : no logging
// 1 : default logging - one log per thread
// 2 : one log per thread per table
// 3 : one std::unordered_map per thread per table
public:
    // Premable
    using C = index_common<K, V, DBParams>;
    using typename C::key_type;
    using typename C::value_type;
    using typename C::sel_return_type;
    using typename C::ins_return_type;
    using typename C::del_return_type;

    using typename C::version_type;
    using typename C::value_container_type;
    using typename C::comm_type;

    using C::invalid_bit;
    using C::insert_bit;
    using C::delete_bit;
    using C::row_update_bit;
    using C::row_cell_bit;

    using C::has_insert;
    using C::has_delete;
    using C::has_row_update;
    using C::has_row_cell;

    using C::sel_abort;
    using C::ins_abort;
    using C::del_abort;

    using C::index_read_my_write;

    static constexpr TransItem::flags_type log_add_bit = TransItem::user0_bit << 4u;

    typedef typename get_occ_version<DBParams>::type bucket_version_type;

    typedef std::hash<K> Hash;
    typedef std::equal_to<K> Pred;

    // our hashtable is an array of linked lists.
    // an internal_elem is the node type for these linked lists
    struct internal_elem {
        internal_elem *next;
        key_type key;
        value_container_type row_container;
        bool deleted;

        internal_elem(const key_type& k, const value_type& v, bool valid)
            : next(nullptr), key(k),
              row_container((valid ? Sto::initialized_tid() : (Sto::initialized_tid() | invalid_bit)), !valid, v),
              deleted(false) {}

        version_type& version() {
            return row_container.row_version();
        }

        bool valid() {
            return !(version().value() & invalid_bit);
        }
    };

    void thread_init() {}

    void thread_init(int runner_num) {
        // set logger!
        if(LOG == 2){
            if(!logger_ && logs)
                set_logger_(& (reinterpret_cast<logset_tbl<LOG_NTABLES>*>(logs))->log(runner_num)); // get the corresponding log for that thread!
        }
    }

    loginfo* logger() const {
        return (loginfo*)logger_;
    }

    void* logger_tbl() const{
        return logger_;
    }
    
    


    ~unordered_index() override {}

private:

    void set_logger_(void* logger) {
        assert(!logger_ && logger);
        logger_ = logger;
    }

    struct bucket_entry {
        internal_elem *head;
        // this is the bucket version number, which is incremented on insert
        // we use it to make sure that an unsuccessful key lookup will still be
        // unsuccessful at commit time (because this will always be true if no
        // new inserts have occurred in this bucket)
        bucket_version_type version;
        bucket_entry() : head(nullptr), version(0) {}
    };

    typedef std::vector<bucket_entry> MapType;
    // this is the hashtable itself, an array of bucket_entry's
    MapType map_;
    Hash hasher_;
    Pred pred_;

    uint64_t key_gen_;

    // the index of this table in the log buffer when we choose one log per thread per table
    int tbl_index_=0;

    static __thread void* logger_; // one logger per thread! This will point to the corresponding log for that thread.

    // used to mark whether a key is a bucket (for bucket version checks)
    // or a pointer (which will always have the lower 3 bits as 0)
    static constexpr uintptr_t bucket_bit = C::item_key_tag;

public:
    // split version helper stuff
    using index_t = unordered_index<K, V, DBParams, LOG>;
    using column_access_t = typename split_version_helpers<index_t>::column_access_t;
    using item_key_t = typename split_version_helpers<index_t>::item_key_t;
    template <typename T>
    static constexpr auto column_to_cell_accesses
        = split_version_helpers<index_t>::template column_to_cell_accesses<T>;
    template <typename T>
    static constexpr auto extract_item_list
        = split_version_helpers<index_t>::template extract_item_list<T>;

    // Main constructor
    unordered_index(size_t size, Hash h = Hash(), Pred p = Pred()) :
            map_(), hasher_(h), pred_(p), key_gen_(0), tbl_index_(-1) {
        map_.resize(size);
    }

    // Dimos - we need an empty constructor for the log! -> see log.hh
    unordered_index(Hash h = Hash(), Pred p = Pred()) :
            map_(), hasher_(h), pred_(p), key_gen_(0), tbl_index_(-1) {
    }

    // tbl_index: the index of this table in the log buffer
    unordered_index(size_t size, int tbl_index, Hash h = Hash(), Pred p = Pred()) :
            map_(), hasher_(h), pred_(p), key_gen_(0), tbl_index_(tbl_index) {
        map_.resize(size);
        assert(tbl_index>=0);  //&& tbl_index <= logs_tbl->log(runner_num). getNumtbl ?? )
    }


    inline size_t hash(const key_type& k) const {
        return hasher_(k);
    }
    inline size_t nbuckets() const {
        return map_.size();
    }
    inline size_t find_bucket_idx(const key_type& k) const {
        return hash(k) % nbuckets();
    }

    uint64_t gen_key() {
        return fetch_and_add(&key_gen_, 1);
    }

    // Dimos - support traverse for using UIndex for the log!
    void traverse_all(std::function<void(const key_type& , value_type* )> callback){
        for (auto& buck : map_){
            internal_elem *curr = buck.head;
            while (curr){ // traverse all elements within bucket
                callback(curr->key, &curr->row_container.row);
                curr = curr->next;
            }
        }
    }

    sel_return_type
    select_row(const key_type& k, RowAccess access) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        bucket_version_type buck_vers = buck.version;
        fence();
        internal_elem *e = find_in_bucket(buck, k);

        if (e != nullptr) {
            return select_row(reinterpret_cast<uintptr_t>(e), access);
        } else {
            if (!Sto::item(this, make_bucket_key(buck)).observe(buck_vers)) {
                return sel_abort;
            }
            return { true, false, 0, nullptr };
        }
    }

    sel_return_type
    select_row(const key_type& k, std::initializer_list<column_access_t> accesses) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        bucket_version_type buck_vers = buck.version;
        fence();
        internal_elem *e = find_in_bucket(buck, k);

        if (e != nullptr) {
            return select_row(reinterpret_cast<uintptr_t>(e), accesses);
        } else {
            if (!Sto::item(this, make_bucket_key(buck)).observe(buck_vers)) {
                return sel_abort;
            }
            return { true, false, 0, nullptr };
        }
    }

    sel_return_type
    select_row(uintptr_t rid, RowAccess access) {
        auto e = reinterpret_cast<internal_elem*>(rid);
        bool ok = true;
        TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

        if (is_phantom(e, row_item))
            return sel_abort;

        if (index_read_my_write) {
            if (has_delete(row_item))
                return { true, false, 0, nullptr };
            if (has_row_update(row_item)) {
                value_type* vptr = nullptr;
                if (has_insert(row_item))
                    vptr = &(e->row_container.row);
                else
                    vptr = row_item.template raw_write_value<value_type*>();
                assert(vptr);
                return { true, true, rid, vptr };
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
            return sel_abort;

        return { true, true, rid, &(e->row_container.row) };
    }

    sel_return_type
    select_row(uintptr_t rid, std::initializer_list<column_access_t> accesses) {
        auto e = reinterpret_cast<internal_elem*>(rid);
        TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

        auto cell_accesses = column_to_cell_accesses<value_container_type>(accesses);

        std::array<TransItem*, value_container_type::num_versions> cell_items {};
        bool any_has_write;
        bool ok;
        std::tie(any_has_write, cell_items) = extract_item_list<value_container_type>(cell_accesses, this, e);

        if (is_phantom(e, row_item))
            return sel_abort;

        if (index_read_my_write) {
            if (has_delete(row_item)) {
                return { true, false, 0, nullptr };
            }
            if (any_has_write || has_row_update(row_item)) {
                value_type *vptr;
                if (has_insert(row_item))
                    vptr = &(e->row_container.row);
                else
                    vptr = row_item.template raw_write_value<value_type *>();
                return { true, true, rid, vptr };
            }
        }

        ok = access_all(cell_accesses, cell_items, e->row_container);
        if (!ok)
            return sel_abort;

        return sel_return_type(true, true, rid, &(e->row_container.row));
    }

    void update_row(uintptr_t rid, value_type *new_row) {
        auto e = reinterpret_cast<internal_elem*>(rid);
        auto row_item = Sto::item(this, item_key_t::row_item_key(e));
        row_item.acquire_write(e->version(), new_row);
        if(LOG > 0)
            row_item.add_flags(log_add_bit);
    }

    void update_row(uintptr_t rid, const comm_type &comm) {
        assert(&comm);
        auto row_item = Sto::item(this, item_key_t::row_item_key(reinterpret_cast<internal_elem *>(rid)));
        row_item.add_commute(comm);
    }

    ins_return_type
    insert_row(const key_type& k, value_type *vptr, bool overwrite = false) {
        bucket_entry& buck = map_[find_bucket_idx(k)];

        buck.version.lock_exclusive();
        internal_elem* e = find_in_bucket(buck, k);

        if (e) {
            buck.version.unlock_exclusive();
            auto row_item = Sto::item(this, item_key_t::row_item_key(e));
            if (is_phantom(e, row_item))
                return ins_abort;

            if (index_read_my_write) {
                if (has_delete(row_item)) {
                    row_item.clear_flags(delete_bit).clear_write().template add_write<value_type *>(vptr);
                    return { true, false };
                }
            }

            if (overwrite) {
                if (!version_adapter::select_for_overwrite(row_item, e->version(), vptr))
                    return ins_abort;
                if (index_read_my_write) {
                    if (has_insert(row_item)) {
                        copy_row(e, vptr);
                    }
                }
            } else {
                if (!row_item.observe(e->version()))
                    return ins_abort;
            }
            #if ADD_TO_LOG > 0
            if(LOG > 0 && overwrite){
                row_item.add_flags(log_add_bit);
            }
            #endif
            return { true, true };
        } else {
            // insert the new row to the table and take note of bucket version changes
            auto buck_vers_0 = bucket_version_type(buck.version.unlocked_value());
            insert_in_bucket(buck, k, vptr, false);
            internal_elem *new_head = buck.head;
            auto buck_vers_1 = bucket_version_type(buck.version.unlocked_value());

            buck.version.unlock_exclusive();

            // update bucket version in the read set (if any) since it's changed by ourselves
            auto bucket_item = Sto::item(this, make_bucket_key(buck));
            if (bucket_item.has_read())
                bucket_item.update_read(buck_vers_0, buck_vers_1);

            auto item = Sto::item(this, item_key_t::row_item_key(new_head));
            // XXX adding write is probably unnecessary, am I right?
            item.template add_write<value_type*>(vptr);
            item.add_flags(insert_bit);
            #if ADD_TO_LOG > 0
            if(LOG > 0){
                bucket_item.add_flags(log_add_bit);
            }
            #endif
            return { true, false };
        }
    }

    // returns (success : bool, found : bool)
    // for rows that are not inserted by this transaction, the actual delete doesn't take place
    // until commit time
    del_return_type
    delete_row(const key_type& k) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        bucket_version_type buck_vers = buck.version;
        fence();

        internal_elem* e = find_in_bucket(buck, k);
        if (e) {
            auto item = Sto::item(this, item_key_t::row_item_key(e));
            bool valid = e->valid();
            if (is_phantom(e, item))
                return del_abort;
            if (index_read_my_write) {
                if (!valid && has_insert(item)) {
                    // deleting something we inserted
                    _remove(e);
                    item.remove_read().remove_write().clear_flags(insert_bit | delete_bit);
                    Sto::item(this, make_bucket_key(buck)).observe(buck_vers);
                    return { true, true };
                }
                assert(valid);
                if (has_delete(item))
                    return { true, false };
            }
            // select_for_update() will automatically add an observation for OCC version types
            // so that we can catch change in "deleted" status of a table row at commit time
            if (!version_adapter::select_for_update(item, e->version()))
                return del_abort;
            fence();
            // it vital that we check the "deleted" status after registering an observation
            if (e->deleted)
                return del_abort;
            item.add_flags(delete_bit);

            return { true, true };
        } else {
            // not found -- add observation of bucket version
            bool ok = Sto::item(this, make_bucket_key(buck)).observe(buck_vers);
            if (!ok)
                return del_abort;
            return { true, false };
        }
    }

    // non-transactional methods
    value_type* nontrans_get(const key_type& k) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        internal_elem* e = find_in_bucket(buck, k);
        if (e == nullptr)
            return nullptr;
        return &(e->row_container.row);
    }

    void nontrans_put(const key_type& k, const value_type& v) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        buck.version.lock_exclusive();
        internal_elem *e = find_in_bucket(buck, k);
        if (e == nullptr) {
            internal_elem *new_head = new internal_elem(k, v, true);
            new_head->next = buck.head;
            buck.head = new_head;
        } else {
            copy_row(e, &v);
        }
        buck.version.unlock_exclusive();
    }

    // Dimos - required when using for log!
    void nontrans_put_no_lock(const key_type& k, const value_type& v){
        bucket_entry& buck = map_[find_bucket_idx(k)];
        internal_elem *e = find_in_bucket(buck, k);
        if (e == nullptr) {
            internal_elem *new_head = new internal_elem(k, v, true);
            new_head->next = buck.head;
            buck.head = new_head;
        } else {
            copy_row(e, &v);
        }
    }

    // TObject interface methods
    bool lock(TransItem& item, Transaction& txn) override {
        assert(!is_bucket(item));
        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();
        if (key.is_row_item()) {
            return txn.try_lock(item, e->version());
        } else {
            return txn.try_lock(item, e->row_container.version_at(key.cell_num()));
        }
    }

    bool check(TransItem& item, Transaction& txn) override {
        if (is_bucket(item)) {
            bucket_entry &buck = *bucket_address(item);
            return buck.version.cp_check_version(txn, item);
        } else {
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            if (key.is_row_item())
                return e->version().cp_check_version(txn, item);
            else
                return e->row_container.version_at(key.cell_num()).cp_check_version(txn, item);
        }
    }

    inline void log_add(TransItem& item, internal_elem* e){
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
        qtimes.ts = e->version().value();
        qtimes.prev_ts = 0;
        void* logger = logger_tbl();
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
            // Must use a local log_epoch so that to increase only when a log drain is requested AND the current transaction is committed and fully applied in the log. We will do local_epoch = global_epoch only when transaction committed successfully (in TPCC_bench.hh)
            value_type* valp;
            if(has_insert(item)){
                #if LOGREC_OVERWRITE
                if(LOG == 2)
                    valp = & ((reinterpret_cast<internal_elem_log*>(e))->row_container.row);
                else
                    valp = &e->row_container.row;
                #else
                    valp = &e->row_container.row;
                #endif
            }
            else{
                valp = item.template raw_write_value<value_type*>();
            }
            if(LOG == 1)
                reinterpret_cast<loginfo*>(logger)->record(cmd, qtimes, Str(e->key), Str(*valp), tbl_index_);
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
                    //logger_tbl->record(cmd, qtimes, Str(el->key), Str(*valp), tbl_index_,  el->log_pos[log_indx] );
                    // TODO: for now do not overwrite, just to test the overhead of storing the log pos and epoch!
                    el->update_log_pos(log_indx, logger_tbl->record(cmd, qtimes, Str(el->key), Str(*valp), tbl_index_, el->get_log_pos(log_indx)));
                    //logger_tbl->record(cmd, qtimes, Str(el->key), Str(*valp), tbl_index_);
                }
                else if (el->get_creation_epoch(log_indx) == 0) // there is no log record location for this key (creation epoch =0), store it!
                    el->set_log_pos(log_indx, logger_tbl->record(cmd, qtimes, Str(el->key), Str(*valp), tbl_index_), qtimes.epoch.value());
                else // different epoch, do not update!
                    logger_tbl->record(cmd, qtimes, Str(el->key), Str(*valp), tbl_index_);
                #else
                logger_tbl->record(cmd, qtimes, Str(e->key), Str(*valp), tbl_index_);
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
                const char* v = valp->to_str();
                reinterpret_cast<loginfo_tbl<LOG_NTABLES>*>(logger)->record(cmd, qtimes, k, strlen(k), v, strlen(v), tbl_index_);
                delete[] k;
                delete[] v;
                #endif
                //reinterpret_cast<loginfo_tbl<LOG_NTABLES>*>(logger)->record(cmd, qtimes, &e->key, sizeof(e->key), valp, sizeof(value_type), tbl_index_);
            }
            else if(LOG == 3)
                reinterpret_cast<loginfo_map<LOG_NTABLES>*>(logger)->record(cmd, qtimes, Str(e->key), Str(*valp), tbl_index_);
        }
    }

    void install(TransItem& item, Transaction& txn) override {
        assert(!is_bucket(item));
        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();

        if (key.is_row_item()) {
            if (has_delete(item)) {
                assert(e->valid() && !e->deleted);
                e->deleted = true;
                fence();
                txn.set_version(e->version());
                return;
            }

            if (!has_insert(item)) {
                // update
                if (item.has_commute()) {
                    comm_type &comm = item.write_value<comm_type>();
                    if (has_row_update(item)) {
                        copy_row(e, comm);
                    } else if (has_row_cell(item)) {
                        e->row_container.install_cell(comm);
                    }
                } else {
                    auto vptr = item.write_value<value_type*>();
                    if (has_row_update(item)) {
                        copy_row(e, vptr);
                    } else if (has_row_cell(item)) {
                        e->row_container.install_cell(0, vptr);
                    }
                }
            }
            txn.set_version_unlock(e->version(), item);
        } else {
            auto row_item = Sto::item(this, item_key_t::row_item_key(e));
            if (!has_row_update(row_item)) {
                if (row_item.has_commute()) {
                    comm_type &comm = row_item.template write_value<comm_type>();
                    assert(&comm);
                    e->row_container.install_cell(comm);
                } else {
                    auto vptr = row_item.template raw_write_value<value_type*>();
                    e->row_container.install_cell(key.cell_num(), vptr);
                }
            }
            txn.set_version_unlock(e->row_container.version_at(key.cell_num()), item);
        }
        // add to log here, so that we already have the updated version.
        #if ADD_TO_LOG == 2
            if(LOG > 0 && has_log_add(item))
                log_add(item, e);
        #endif
    }

    void unlock(TransItem& item) override {
        assert(!is_bucket(item));
        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();
        if (key.is_row_item())
            e->version().cp_unlock(item);
        else
            e->row_container.version_at(key.cell_num()).cp_unlock(item);
    }

    void cleanup(TransItem& item, bool committed) override {
        #if ADD_TO_LOG == 1
        if(LOG > 0 && committed && has_log_add(item)){
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            log_add(item, e);
        }
        #endif
        if (committed ? has_delete(item) : has_insert(item)) {
            assert(!is_bucket(item));
            auto key = item.key<item_key_t>();
            internal_elem* e = key.internal_elem_ptr();
            assert(!e->valid() || e->deleted);
            _remove(e);
            item.clear_needs_unlock();
        }
    }

private:
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

    // remove a k-v node during transactions (with locks)
    void _remove(internal_elem *el) {
        bucket_entry& buck = map_[find_bucket_idx(el->key)];
        buck.version.lock_exclusive();
        internal_elem *prev = nullptr;
        internal_elem *curr = buck.head;
        while (curr != nullptr && curr != el) {
            prev = curr;
            curr = curr->next;
        }
        assert(curr);
        if (prev != nullptr)
            prev->next = curr->next;
        else
            buck.head = curr->next;
        buck.version.unlock_exclusive();
        Transaction::rcu_delete(curr);
    }
    // non-transactional remove by key
    bool remove(const key_type& k) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        buck.version.lock_exclusive();
        internal_elem *prev = nullptr;
        internal_elem *curr = buck.head;
        while (curr != nullptr && !pred_(curr->key, k)) {
            prev = curr;
            curr = curr->next;
        }
        if (curr == nullptr) {
            buck.version.unlock_exclusive();
            return false;
        }
        if (prev != nullptr)
            prev->next = curr->next;
        else
            buck.head = curr->next;
        buck.version.unlock_exclusive();
        delete curr;
        return true;
    }
    // insert a k-v node to a bucket
    void insert_in_bucket(bucket_entry& buck, const key_type& k, const value_type *v, bool valid) {
        assert(buck.version.is_locked());

        internal_elem *new_head = new internal_elem(k, v ? *v : value_type(), valid);
        internal_elem *curr_head = buck.head;

        new_head->next = curr_head;
        buck.head = new_head;

        buck.version.inc_nonopaque();
    }
    // find a key's k-v node (internal_elem) within a bucket
    internal_elem *find_in_bucket(const bucket_entry& buck, const key_type& k) {
        internal_elem *curr = buck.head;
        while (curr && !pred_(curr->key, k))
            curr = curr->next;
        return curr;
    }

    static bool is_phantom(internal_elem *e, const TransItem& item) {
        return (!e->valid() && !has_insert(item));
    }

    static bool has_log_add(const TransItem& item){
        return (item.flags() & log_add_bit) != 0;
    }

    // TransItem keys
    static bool is_bucket(const TransItem& item) {
        return item.key<uintptr_t>() & bucket_bit;
    }
    static uintptr_t make_bucket_key(const bucket_entry& bucket) {
        return (reinterpret_cast<uintptr_t>(&bucket) | bucket_bit);
    }
    static bucket_entry *bucket_address(const TransItem& item) {
        uintptr_t bucket_key = item.key<uintptr_t>();
        return reinterpret_cast<bucket_entry*>(bucket_key & ~bucket_bit);
    }

    static void copy_row(internal_elem *e, comm_type &comm) {
        e->row_container.row = comm.operate(e->row_container.row);
    }
    static void copy_row(internal_elem *table_row, const value_type *value) {
        if (value == nullptr)
            return;
        table_row->row_container.row = *value;
    }
};

template <typename K, typename V, typename DBParams, short LOG>
__thread void* unordered_index<K, V, DBParams, LOG>::logger_;

// MVCC variant
template <typename K, typename V, typename DBParams>
class mvcc_unordered_index : public index_common<K, V, DBParams>, public TObject {
public:
    // Premable
    using C = index_common<K, V, DBParams>;
    using typename C::key_type;
    using typename C::value_type;
    using typename C::sel_return_type;
    using typename C::ins_return_type;
    using typename C::del_return_type;

    using typename C::version_type;
    using typename C::value_container_type;
    using typename C::comm_type;

    using C::invalid_bit;
    using C::insert_bit;
    using C::delete_bit;
    using C::row_update_bit;
    using C::row_cell_bit;

    using C::has_insert;
    using C::has_delete;
    using C::has_row_update;
    using C::has_row_cell;

    using C::sel_abort;
    using C::ins_abort;
    using C::del_abort;

    using C::index_read_my_write;

    typedef MvObject<value_type> object_type;
    typedef typename object_type::history_type history_type;
    typedef typename get_occ_version<DBParams>::type bucket_version_type;

    typedef std::hash<K> Hash;
    typedef std::equal_to<K> Pred;

    // our hashtable is an array of linked lists. 
    // an internal_elem is the node type for these linked lists
    struct internal_elem {
        internal_elem *next;
        key_type key;
        object_type row;

        internal_elem(const key_type& k)
            : next(nullptr), key(k), row() {}
    };

    static void thread_init() {}

    static void thread_init(int runner_num) {
        (void)runner_num;
    }

    ~mvcc_unordered_index() override {}

private:
    struct bucket_entry {
        internal_elem *head;
        // this is the bucket version number, which is incremented on insert
        // we use it to make sure that an unsuccessful key lookup will still be
        // unsuccessful at commit time (because this will always be true if no
        // new inserts have occurred in this bucket)
        bucket_version_type version;
        bucket_entry() : head(nullptr), version(0) {}
    };

    typedef std::vector<bucket_entry> MapType;
    // this is the hashtable itself, an array of bucket_entry's
    MapType map_;
    Hash hasher_;
    Pred pred_;

    uint64_t key_gen_;

    // the index of this table in the log buffer
    int tbl_index_=0;

    // used to mark whether a key is a bucket (for bucket version checks)
    // or a pointer (which will always have the lower 3 bits as 0)
    static constexpr uintptr_t bucket_bit = C::item_key_tag;

public:
    // split version helper stuff
    using index_t = mvcc_unordered_index<K, V, DBParams>;
    using column_access_t = typename split_version_helpers<index_t>::column_access_t;
    using item_key_t = typename split_version_helpers<index_t>::item_key_t;

    // Main constructor
    mvcc_unordered_index(size_t size, Hash h = Hash(), Pred p = Pred()) :
            map_(), hasher_(h), pred_(p), key_gen_(0), tbl_index_(-1) {
        map_.resize(size);
    }

    // tbl_index: the index of this table in the log buffer
    mvcc_unordered_index(size_t size, int tbl_index, Hash h = Hash(), Pred p = Pred()) :
            map_(), hasher_(h), pred_(p), key_gen_(0), tbl_index_(tbl_index) {
        map_.resize(size);
        assert(tbl_index>=0);  //&& tbl_index <= logs_tbl->log(runner_num). getNumtbl ?? )
    }


    inline size_t hash(const key_type& k) const {
        return hasher_(k);
    }
    inline size_t nbuckets() const {
        return map_.size();
    }
    inline size_t find_bucket_idx(const key_type& k) const {
        return hash(k) % nbuckets();
    }

    uint64_t gen_key() {
        return fetch_and_add(&key_gen_, 1);
    }

    sel_return_type
    select_row(const key_type& k, RowAccess access) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        bucket_version_type buck_vers = buck.version;
        fence();
        internal_elem *e = find_in_bucket(buck, k);

        if (e != nullptr) {
            return select_row(reinterpret_cast<uintptr_t>(e), access);
        } else {
            if (!Sto::item(this, make_bucket_key(buck)).observe(buck_vers)) {
                return sel_abort;
            }
            return { true, false, 0, nullptr };
        }
    }

    sel_return_type
    select_row(const key_type& k, std::initializer_list<column_access_t> accesses) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        bucket_version_type buck_vers = buck.version;
        fence();
        internal_elem *e = find_in_bucket(buck, k);

        if (e != nullptr) {
            return select_row(reinterpret_cast<uintptr_t>(e), accesses);
        } else {
            if (!Sto::item(this, make_bucket_key(buck)).observe(buck_vers)) {
                return sel_abort;
            }
            return { true, false, 0, nullptr };
        }
    }

    sel_return_type
    select_row(uintptr_t rid, RowAccess access) {
        auto e = reinterpret_cast<internal_elem*>(rid);
        TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

        history_type* h = e->row.find(txn_read_tid());

        if (h->status_is(UNUSED))
            return { true, false, 0, nullptr };

        if (is_phantom(h, row_item))
            return { true, false, 0, nullptr };

        if (index_read_my_write) {
            if (has_delete(row_item))
                return { true, false, 0, nullptr };
            if (has_row_update(row_item)) {
                value_type* vptr = nullptr;
                if (has_insert(row_item)) {
#if SAFE_FLATTEN
                    vptr = h->vp_safe_flatten();
                    if (vptr == nullptr)
                        return { false, false, 0, nullptr };
#else
                    vptr = h->vp();
#endif
                } else {
                    vptr = row_item.template raw_write_value<value_type*>();
                }
                assert(vptr);
                return { true, true, rid, vptr };
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
            return { true, true, rid, vp };
        } else {
            return { true, true, rid, nullptr };
        }
    }

    sel_return_type
    select_row(uintptr_t, std::initializer_list<column_access_t>) {
        always_assert(false, "Not implemented in MVCC, use split table instead.");
        return { false, false, 0, nullptr };
    }

    void update_row(uintptr_t rid, value_type *new_row) {
        auto e = reinterpret_cast<internal_elem*>(rid);
        auto row_item = Sto::item(this, item_key_t::row_item_key(e));
        row_item.add_write(new_row);
    }
    
    void update_row(uintptr_t rid, const comm_type &comm) {
        assert(&comm);
        auto row_item = Sto::item(this, item_key_t::row_item_key(reinterpret_cast<internal_elem *>(rid)));
        row_item.add_commute(comm);
    }

    ins_return_type
    insert_row(const key_type& k, value_type *vptr, bool overwrite = false) {
        bucket_entry& buck = map_[find_bucket_idx(k)];

        buck.version.lock_exclusive();
        internal_elem* e = find_in_bucket(buck, k);

        if (e) {
            buck.version.unlock_exclusive();
            auto row_item = Sto::item(this, item_key_t::row_item_key(e));
            auto h = e->row.find(txn_read_tid());
            if (is_phantom(h, row_item))
                return ins_abort;

            if (index_read_my_write) {
                if (has_delete(row_item)) {
                    row_item.clear_flags(delete_bit).clear_write().template add_write<value_type*>(vptr);
                    return { true, false };
                }
            }

            if (overwrite) {
                row_item.template add_write<value_type*>(vptr);
            } else {
                MvAccess::template read<value_type>(row_item, h);
            }

            return { true, true };
        } else {
            // insert the new row to the table and take note of bucket version changes
            auto buck_vers_0 = bucket_version_type(buck.version.unlocked_value());
            insert_in_bucket(buck, k);
            internal_elem *new_head = buck.head;
            auto buck_vers_1 = bucket_version_type(buck.version.unlocked_value());

            buck.version.unlock_exclusive();

            // update bucket version in the read set (if any) since it's changed by ourselves
            auto bucket_item = Sto::item(this, make_bucket_key(buck));
            if (bucket_item.has_read())
                bucket_item.update_read(buck_vers_0, buck_vers_1);

            auto item = Sto::item(this, item_key_t::row_item_key(new_head));
            // XXX adding write is probably unnecessary, am I right?
            item.template add_write<value_type*>(vptr);
            item.add_flags(insert_bit);

            return { true, false };
        }
    }

    // returns (success : bool, found : bool)
    // for rows that are not inserted by this transaction, the actual delete doesn't take place
    // until commit time
    del_return_type
    delete_row(const key_type& k) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        bucket_version_type buck_vers = buck.version;
        fence();

        internal_elem* e = find_in_bucket(buck, k);
        if (e) {
            auto row_item = Sto::item(this, item_key_t::row_item_key(e));

            auto h = e->row.find(txn_read_tid());

            if (is_phantom(h, row_item))
                return { true, false };

            if (index_read_my_write) {
                if (has_delete(row_item))
                    return { true, false };
                if (h->status_is(DELETED) && has_insert(row_item)) {
                    row_item.add_flags(delete_bit);
                    return { true, true };
                }
            }

            MvAccess::template read<value_type>(row_item, h);
            if (h->status_is(DELETED))
                return { true, false };
            row_item.add_write();
            row_item.add_flags(delete_bit);

            return { true, true };
        } else {
            // not found -- add observation of bucket version
            bool ok = Sto::item(this, make_bucket_key(buck)).observe(buck_vers);
            if (!ok)
                return del_abort;
            return { true, false };
        }
    }

    // non-transactional methods
    value_type* nontrans_get(const key_type& k) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        internal_elem* e = find_in_bucket(buck, k);
        if (e == nullptr)
            return nullptr;
        return &(e->row.nontrans_access());
    }

    void nontrans_put(const key_type& k, const value_type& v) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        buck.version.lock_exclusive();
        internal_elem *e = find_in_bucket(buck, k);
        if (e == nullptr) {
            internal_elem *new_head = new internal_elem(k);
            new_head->row.nontrans_access() = v;
            new_head->next = buck.head;
            buck.head = new_head;
        } else {
            e->row.nontrans_access() = v;
        }
        buck.version.unlock_exclusive();
    }

    // TObject interface methods
    bool lock(TransItem& item, Transaction& txn) override {
        assert(!is_bucket(item));
        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();

        history_type* hprev = nullptr;
        if (item.has_read()) {
            hprev = item.read_value<history_type*>();
            if (Sto::commit_tid() < hprev->rtid()) {
                TransProxy(txn, item).add_write(nullptr);
                return false;
            }
        }
        history_type* h;
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
        } else {
            TransProxy(txn, item).add_mvhistory(h);
        }
        return result;
    }

    bool check(TransItem& item, Transaction& txn) override {
        if (is_bucket(item)) {
            bucket_entry &buck = *bucket_address(item);
            return buck.version.cp_check_version(txn, item);
        } else {
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            auto h = item.template read_value<history_type*>();
            auto result = e->row.cp_check(txn_read_tid(), h);
            return result;
        }
    }

    void install(TransItem& item, Transaction&) override {
        assert(!is_bucket(item));
        auto key = item.key<item_key_t>();
        auto e = key.internal_elem_ptr();
        auto h = item.template write_value<history_type*>();

        e->row.cp_install(h);
    }

    void unlock(TransItem& item) override {
        (void)item;
        assert(!is_bucket(item));
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

private:
    // remove a k-v node during transactions (with locks)
    void _remove(internal_elem *el) {
        bucket_entry& buck = map_[find_bucket_idx(el->key)];
        buck.version.lock_exclusive();
        internal_elem *prev = nullptr;
        internal_elem *curr = buck.head;
        while (curr != nullptr && curr != el) {
            prev = curr;
            curr = curr->next;
        }
        assert(curr);
        if (prev != nullptr)
            prev->next = curr->next;
        else
            buck.head = curr->next;
        buck.version.unlock_exclusive();
        Transaction::rcu_delete(curr);
    }
    // non-transactional remove by key
    bool remove(const key_type& k) {
        bucket_entry& buck = map_[find_bucket_idx(k)];
        buck.version.lock_exclusive();
        internal_elem *prev = nullptr;
        internal_elem *curr = buck.head;
        while (curr != nullptr && !pred_(curr->key, k)) {
            prev = curr;
            curr = curr->next;
        }
        if (curr == nullptr) {
            buck.version.unlock_exclusive();
            return false;
        }
        if (prev != nullptr)
            prev->next = curr->next;
        else
            buck.head = curr->next;
        buck.version.unlock_exclusive();
        delete curr;
        return true;
    }

    static void _delete_cb(
            void *index_ptr, void *ele_ptr, void *history_ptr) {
        auto ip = reinterpret_cast<mvcc_unordered_index<K, V, DBParams>*>(index_ptr);
        auto el = reinterpret_cast<internal_elem*>(ele_ptr);
        auto hp = reinterpret_cast<history_type*>(history_ptr);
        bucket_entry& buck = ip->map_[ip->find_bucket_idx(el->key)];
        buck.version.lock_exclusive();
        internal_elem *prev = nullptr;
        internal_elem *curr = buck.head;
        while (curr != nullptr && curr != el) {
            prev = curr;
            curr = curr->next;
        }
        assert(curr);
        if (prev != nullptr)
            prev->next = curr->next;
        else
            buck.head = curr->next;
        if (el->row.is_head(hp)) {
            buck.version.unlock_exclusive();
            Transaction::rcu_delete(el);
        } else {
            buck.version.unlock_exclusive();
        }
    }

    // insert a k-v node to a bucket
    void insert_in_bucket(bucket_entry& buck, const key_type& k) {
        assert(buck.version.is_locked());

        internal_elem *new_head = new internal_elem(k);
        internal_elem *curr_head = buck.head;

        new_head->next = curr_head;
        buck.head = new_head;

        buck.version.inc_nonopaque();
    }
    // find a key's k-v node (internal_elem) within a bucket
    internal_elem *find_in_bucket(const bucket_entry& buck, const key_type& k) {
        internal_elem *curr = buck.head;
        while (curr && !pred_(curr->key, k))
            curr = curr->next;
        return curr;
    }

    static bool is_phantom(const history_type *h, const TransItem& item) {
        return (h->status_is(DELETED) && !has_insert(item));
    }

    // TransItem keys
    static bool is_bucket(const TransItem& item) {
        return item.key<uintptr_t>() & bucket_bit;
    }
    static uintptr_t make_bucket_key(const bucket_entry& bucket) {
        return (reinterpret_cast<uintptr_t>(&bucket) | bucket_bit);
    }
    static bucket_entry *bucket_address(const TransItem& item) {
        uintptr_t bucket_key = item.key<uintptr_t>();
        return reinterpret_cast<bucket_entry*>(bucket_key & ~bucket_bit);
    }

    static TransactionTid::type txn_read_tid() {
        return Sto::read_tid<DBParams::Commute>();
    }
};

}
