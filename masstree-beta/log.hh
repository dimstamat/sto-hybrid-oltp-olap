/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2014 President and Fellows of Harvard College
 * Copyright (c) 2012-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
#ifndef MASSTREE_LOG_HH
#define MASSTREE_LOG_HH
#include "kvthread.hh"
#include "string.hh"
#include "kvproto.hh"
#include "str.hh"
#include <pthread.h>
#include "misc.hh"

#define MEASURE_LOG_RECORDS 0
#define LOG_PER_TABLE 1 // support separate log per table. We will simply index the log (pointer arithmetic) to go to the log records of a specific table.  

class logset;

using lcdf::Str;
namespace lcdf { class Json; }

template <int N_TBLS> 
class logset_tables;

// in-memory log.
// more than one, to reduce contention on the lock.
class loginfo {
  public:
    void initialize(const lcdf::String& logfile);

    inline void acquire();
    inline void release();

    inline kvepoch_t flushed_epoch() const;
    inline bool quiescent() const;

    // logging
    struct query_times {
        kvepoch_t epoch;
        kvtimestamp_t ts;
        kvtimestamp_t prev_ts;
    };
    // NB may block!
    void record(int command, const query_times& qt, Str key, Str value);
    void record(int command, const query_times& qt, Str key,
                const lcdf::Json* req, const lcdf::Json* end_req);

    uint32_t current_size(){
        return pos_;
    }

    #if MEASURE_LOG_RECORDS
    uint32_t cur_log_records(){
        return records_num_;
    }

    void incr_cur_log_records(){
        records_num_++;
    }
    #else
    uint32_t cur_log_records(){
        return 0;
    }
    #endif

  private:
    struct waitlist {
        waitlist* next;
    };
    struct front {
        uint32_t lock_;
        waitlist* waiting_;
        lcdf::String::rep_type filename_;
        logset* logset_;
    };
    struct logset_info {
        int32_t size_;
        int allocation_offset_;
    };

    front f_;
    #if MEASURE_LOG_RECORDS
    uint32_t records_num_; // Dimos - number of log records currently stored
    char padding1_[CACHE_LINE_SIZE - sizeof(front) - sizeof(uint32_t)]; // subtract the size of records_num_ also!
    #else
    char padding1_[CACHE_LINE_SIZE - sizeof(front)];
    #endif

    kvepoch_t log_epoch_;       // epoch written to log (non-quiescent)
    kvepoch_t quiescent_epoch_; // epoch we went quiescent
    kvepoch_t wake_epoch_;      // epoch for which we recorded a wake command
    kvepoch_t flushed_epoch_;   // epoch fsync()ed to disk

    union { // 32 bytes
        struct { // 28 bytes
            char *buf_;
            uint32_t pos_; // position of the next log entry to store (offset of pointer to buffer)
            uint32_t len_; // total length of log

            // We have logged all writes up to, but not including,
            // flushed_epoch_.
            // Log is quiesced to disk if quiescent_epoch_ != 0
            // and quiescent_epoch_ == flushed_epoch_.
            // When a log wakes up from quiescence, it sets global_wake_epoch;
            // other threads must record a logcmd_wake in their logs.
            // Invariant: log_epoch_ != quiescent_epoch_ (unless both are 0).

            threadinfo *ti_;
            int logindex_;
        };
        struct { // 32 bytes
            char cache_line_2_[CACHE_LINE_SIZE - 4 * sizeof(kvepoch_t) - sizeof(logset_info)]; // 24 bytes
            logset_info lsi_;
        };
    };

    loginfo(logset* ls, int logindex);
    ~loginfo();
    void* run();
    static void* trampoline(void*);

    friend class logset;
};

// in-memory log, one per thread. Buffer is indexed by table and thus we need one loginfo for all tables.
template <int N_TBLS=1>
class loginfo_tables {
    public:
    inline void acquire();
    inline void release();

    // logging
    struct query_times {
        kvepoch_t epoch;
        kvtimestamp_t ts;
        kvtimestamp_t prev_ts;
    };
    // NB may block!
    // tbl: The table where the log record belongs
    void record(int command, const query_times& qt, Str key, Str value, int tbl);
    void record(int command, const query_times& qt, Str key,
                const lcdf::Json* req, const lcdf::Json* end_req);

    uint32_t current_size(uint32_t tbl){
        return pos_[tbl] - start_[tbl];
    }

    uint32_t cur_log_records(){
        return 0;
    }

    private:
    struct waitlist {
        waitlist* next;
    };
    struct front { // 20 bytes
        uint32_t lock_;
        waitlist* waiting_;
        logset_tables<N_TBLS>* logset_;
    };
    struct logset_info {
        int32_t size_;
        int allocation_offset_;
    };
    
    front f_;
    
    #if MEASURE_LOG_RECORDS
    uint32_t records_num_; // Dimos - number of log records currently stored
    char padding1_[CACHE_LINE_SIZE - sizeof(front) - 4* sizeof(kvepoch_t) - sizeof(uint32_t)]; // subtract the size of records_num_ also!
    #else
    char padding1_[CACHE_LINE_SIZE - sizeof(front) - 4* sizeof(kvepoch_t)]; // we can fit all these in one cache line.
    #endif

    kvepoch_t log_epoch_;       // epoch written to log (non-quiescent)
    // TODO: might not needed since we don't flush this log to disk!
    kvepoch_t quiescent_epoch_; // epoch we went quiescent
    kvepoch_t wake_epoch_;      // epoch for which we recorded a wake command
    kvepoch_t flushed_epoch_;   // epoch fsync()ed to disk


    
    union { // 20 + 12 * N_TBLS bytes
        struct { // 20 + 12 * N_TBLS bytes
            char *buf_;
            uint32_t pos_[N_TBLS]; // position of the next log entry to store (offset of pointer to buffer) for each table - absolute position
            uint32_t start_[N_TBLS]; // position in the buffer where each table log starts - absolute position
            uint32_t len_[N_TBLS]; // total length of log for each table - relative length (separate for each table)

            threadinfo *ti_;
            int logindex_;
        };
        struct {
            char cache_line_2_ [ ((sizeof(char*) + sizeof(threadinfo*) + sizeof(int)+ 3 * sizeof(uint32_t) * N_TBLS)/CACHE_LINE_SIZE) * CACHE_LINE_SIZE + CACHE_LINE_SIZE - sizeof(logset_info)];
            logset_info lsi_; // 8 bytes
        };
    };

    loginfo_tables(logset_tables<N_TBLS>* ls, int logindex, std::vector<int>& tbl_sizes);
    
    ~loginfo_tables();

    friend class logset_tables<N_TBLS>;

};




template <int N_TBLS=1>
class logset_tables{
  public:
    static logset_tables* make(int size, std::vector<int>& tbl_sizes);

    static void free(logset_tables* ls);

    inline int size() const;
    inline loginfo_tables<N_TBLS>& log(int i);
    inline const loginfo_tables<N_TBLS>& log(int i) const;

  private:
    loginfo_tables<N_TBLS> li_[0];

};

class logset {
  public:
    static logset* make(int size);
    static void free(logset* ls);

    inline int size() const;
    inline loginfo& log(int i);
    inline const loginfo& log(int i) const;

  private:
    loginfo li_[0];
};

extern kvepoch_t global_log_epoch;
extern kvepoch_t global_wake_epoch;
extern struct timeval log_epoch_interval;

enum logcommand {
    logcmd_none = 0,
    logcmd_put = 0x5455506B,            // "kPUT" in little endian
    logcmd_replace = 0x3155506B,        // "kPU1"
    logcmd_modify = 0x444F4D6B,         // "kMOD"
    logcmd_remove = 0x4D45526B,         // "kREM"
    logcmd_epoch = 0x4F50456B,          // "kEPO"
    logcmd_quiesce = 0x4955516B,        // "kQUI"
    logcmd_wake = 0x4B41576B            // "kWAK"
};


// replay the in-memory log.
// size is the size of the log to be played.
class logmem_replay {
    public:
    logmem_replay(char * buf, off_t size) : size_(size), buf_(buf) {}
    ~logmem_replay();

    struct info_type {
        kvepoch_t first_epoch;
        kvepoch_t last_epoch;
        kvepoch_t wake_epoch;
        kvepoch_t min_post_quiescent_wake_epoch;
        bool quiescent;
    };

    info_type info() const;
    kvepoch_t min_post_quiescent_wake_epoch(kvepoch_t quiescent_epoch) const;

    uint64_t replay();

  private:
    off_t size_;
    char *buf_;

};

class logreplay {
  public:
    logreplay(const lcdf::String &filename);
    ~logreplay();
    int unmap();

    struct info_type {
        kvepoch_t first_epoch;
        kvepoch_t last_epoch;
        kvepoch_t wake_epoch;
        kvepoch_t min_post_quiescent_wake_epoch;
        bool quiescent;
    };
    info_type info() const;
    kvepoch_t min_post_quiescent_wake_epoch(kvepoch_t quiescent_epoch) const;

    void replay(int i, threadinfo *ti);

  private:
    lcdf::String filename_;
    int errno_;
    off_t size_;
    char *buf_;

    uint64_t replayandclean1(kvepoch_t min_epoch, kvepoch_t max_epoch,
                             threadinfo *ti);
    int replay_truncate(size_t len);
    int replay_copy(const char *tmpname, const char *first, const char *last);
};

enum { REC_NONE, REC_CKP, REC_LOG_TS, REC_LOG_ANALYZE_WAKE,
       REC_LOG_REPLAY, REC_DONE };
extern void recphase(int nactive, int state);
extern void waituntilphase(int phase);
extern void inactive();
extern pthread_mutex_t rec_mu;
extern logreplay::info_type *rec_log_infos;
extern kvepoch_t rec_ckp_min_epoch;
extern kvepoch_t rec_ckp_max_epoch;
extern kvepoch_t rec_replay_min_epoch;
extern kvepoch_t rec_replay_max_epoch;
extern kvepoch_t rec_replay_min_quiescent_last_epoch;



// log records


struct logrec_base {
    uint32_t command_;
    uint32_t size_;

    static size_t size() {
        return sizeof(logrec_base);
    }
    static size_t store(char *buf, uint32_t command) {
        // XXX check alignment on some architectures
        logrec_base *lr = reinterpret_cast<logrec_base *>(buf);
        lr->command_ = command;
        lr->size_ = sizeof(*lr);
        return sizeof(*lr);
    }
    static bool check(const char *buf) {
        const logrec_base *lr = reinterpret_cast<const logrec_base *>(buf);
        return lr->size_ >= sizeof(*lr);
    }
    static uint32_t command(const char *buf) {
        const logrec_base *lr = reinterpret_cast<const logrec_base *>(buf);
        return lr->command_;
    }
};

struct logrec_epoch {
    uint32_t command_;
    uint32_t size_;
    kvepoch_t epoch_;

    static size_t size() {
        return sizeof(logrec_epoch);
    }
    static size_t store(char *buf, uint32_t command, kvepoch_t epoch) {
        // XXX check alignment on some architectures
        logrec_epoch *lr = reinterpret_cast<logrec_epoch *>(buf);
        lr->command_ = command;
        lr->size_ = sizeof(*lr);
        lr->epoch_ = epoch;
        return sizeof(*lr);
    }
    static bool check(const char *buf) {
        const logrec_epoch *lr = reinterpret_cast<const logrec_epoch *>(buf);
        return lr->size_ >= sizeof(*lr);
    }
};

struct logrec_kv {
    uint32_t command_;
    uint32_t size_;
    kvtimestamp_t ts_;
    uint32_t keylen_;
    char buf_[0];

    static size_t size(uint32_t keylen, uint32_t vallen) {
        return sizeof(logrec_kv) + keylen + vallen;
    }
    static size_t store(char *buf, uint32_t command,
                        Str key, Str val,
                        kvtimestamp_t ts) {
        // XXX check alignment on some architectures
        logrec_kv *lr = reinterpret_cast<logrec_kv *>(buf);
        lr->command_ = command;
        lr->size_ = sizeof(*lr) + key.len + val.len;
        lr->ts_ = ts;
        lr->keylen_ = key.len;
        memcpy(lr->buf_, key.s, key.len);
        memcpy(lr->buf_ + key.len, val.s, val.len);
        return sizeof(*lr) + key.len + val.len;
    }
    static bool check(const char *buf) {
        const logrec_kv *lr = reinterpret_cast<const logrec_kv *>(buf);
        return lr->size_ >= sizeof(*lr)
            && lr->size_ >= sizeof(*lr) + lr->keylen_;
    }
};

struct logrec_kvdelta {
    uint32_t command_;
    uint32_t size_;
    kvtimestamp_t ts_;
    kvtimestamp_t prev_ts_;
    uint32_t keylen_;
    char buf_[0];

    static size_t size(uint32_t keylen, uint32_t vallen) {
        return sizeof(logrec_kvdelta) + keylen + vallen;
    }
    static size_t store(char *buf, uint32_t command,
                        Str key, Str val,
                        kvtimestamp_t prev_ts, kvtimestamp_t ts) {
        // XXX check alignment on some architectures
        logrec_kvdelta *lr = reinterpret_cast<logrec_kvdelta *>(buf);
        lr->command_ = command;
        lr->size_ = sizeof(*lr) + key.len + val.len;
        lr->ts_ = ts;
        lr->prev_ts_ = prev_ts;
        lr->keylen_ = key.len;
        memcpy(lr->buf_, key.s, key.len);
        memcpy(lr->buf_ + key.len, val.s, val.len);
        return sizeof(*lr) + key.len + val.len;
    }
    static bool check(const char *buf) {
        const logrec_kvdelta *lr = reinterpret_cast<const logrec_kvdelta *>(buf);
        return lr->size_ >= sizeof(*lr)
            && lr->size_ >= sizeof(*lr) + lr->keylen_;
    }
};

// function implementations

inline void loginfo::acquire() {
    test_and_set_acquire(&f_.lock_);
}

inline void loginfo::release() {
    test_and_set_release(&f_.lock_);
}

inline kvepoch_t loginfo::flushed_epoch() const {
    return flushed_epoch_;
}

inline bool loginfo::quiescent() const {
    return quiescent_epoch_ && quiescent_epoch_ == flushed_epoch_;
}

inline int logset::size() const {
    return li_[-1].lsi_.size_;
}

inline loginfo& logset::log(int i) {
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}

inline const loginfo& logset::log(int i) const {
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}

template <int N_TBLS>
inline void loginfo_tables<N_TBLS>::acquire() {
    test_and_set_acquire(&f_.lock_);
}

template <int N_TBLS>
inline void loginfo_tables<N_TBLS>::release() {
    test_and_set_release(&f_.lock_);
}

template <int N_TBLS>
inline loginfo_tables<N_TBLS>& logset_tables<N_TBLS>::log(int i){
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}

template <int N_TBLS>
inline const loginfo_tables<N_TBLS>& logset_tables<N_TBLS>::log(int i) const{
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}
template <int N_TBLS>
inline int logset_tables<N_TBLS>::size() const {
    return li_[-1].lsi_.size_;
}


/*=========================*\
|       logset_tables       |
|===========================|
|*    Implementation of    *|
|*    logset_tables        *|
|*    member functions.    *|
\*_________________________*/
template <int N_TBLS>
logset_tables<N_TBLS>* logset_tables<N_TBLS>::make(int size, std::vector<int>& tbl_sizes){
    static_assert((sizeof(loginfo_tables<N_TBLS>) % CACHE_LINE_SIZE) == 0, "unexpected sizeof(loginfo)");
    always_assert(N_TBLS == tbl_sizes.size(), "table sizes should be the same as N_TBLS");
    std::cout<<"Size of loginfo_tables: "<< sizeof(loginfo_tables<N_TBLS>)<<std::endl;
    assert(size > 0 && size <= 64);
    char* x = new char[sizeof(loginfo_tables<N_TBLS>) * size + sizeof(typename loginfo_tables<N_TBLS>::logset_info) + CACHE_LINE_SIZE];
    std::cout<<"size of logset_info: "<< sizeof(typename loginfo_tables<N_TBLS>::logset_info)<<std::endl;
    char* ls_pos = x + sizeof(typename loginfo_tables<N_TBLS>::logset_info);
    
    uintptr_t left = reinterpret_cast<uintptr_t>(ls_pos) % CACHE_LINE_SIZE;
    if (left)
        ls_pos += CACHE_LINE_SIZE - left;
    logset_tables<N_TBLS>* ls = reinterpret_cast<logset_tables<N_TBLS>*>(ls_pos);
    ls->li_[-1].lsi_.size_ = size;
    ls->li_[-1].lsi_.allocation_offset_ = (int) (x - ls_pos);
    for (int i = 0; i != size; ++i)
        new((void*) &ls->li_[i]) loginfo_tables<N_TBLS>(ls, i, tbl_sizes);
    return ls;
}

template <int N_TBLS>
void logset_tables<N_TBLS>::free(logset_tables* ls){
    for (int i = 0; i != ls->size(); ++i)
        ls->li_[i].~loginfo_tables();
    delete[] (reinterpret_cast<char*>(ls) + ls->li_[-1].lsi_.allocation_offset_);
}



/*=========================*\
|       loginfo_tables      |
|===========================|
|*    Implementation of    *|
|*    loginfo_tables.      *|
\*_________________________*/
template <int N_TBLS>
loginfo_tables<N_TBLS>::loginfo_tables(logset_tables<N_TBLS>* ls, int logindex, std::vector<int>& tbl_sizes){
    f_.lock_ = 0;
    f_.waiting_ = 0;
    #if MEASURE_LOG_RECORDS
    records_num_=0;
    #endif

    int total_len = 0;
    int i=0;
    for(auto s : tbl_sizes){
        total_len+=s;
        len_[i++]=s;
    }
    
    buf_ = (char *) malloc(total_len);
    always_assert(buf_);

    // std::cout<<"Total log size: "<< total_len<<std::endl;
    for(int tbl=0; tbl<N_TBLS; tbl++){
        start_[tbl] = pos_[tbl] = tbl==0 ? 0 : (pos_[tbl-1] + len_[tbl-1]);
        //std::cout<<"Table "<< tbl<<": pos_: "<< pos_[tbl]<<", len_: "<< len_[tbl]<<std::endl;
    }
    //std::cout<<"sizeof loginfo_tables: "<<sizeof(*this)<<std::endl;
    log_epoch_ = 0;
    quiescent_epoch_ = 0;
    wake_epoch_ = 0;
    flushed_epoch_ = 0;

    ti_ = 0;
    f_.logset_ = ls;
    logindex_ = logindex;
    (void) padding1_;
}

template <int N_TBLS>
loginfo_tables<N_TBLS>::~loginfo_tables(){
    free(buf_);
}


// tbl: The table where the log record belongs
template <int N_TBLS>
void loginfo_tables<N_TBLS>::record(int command, const query_times& qtimes, Str key, Str value, int tbl){
    //pos_[tbl];    the position in the buffer where the next log record for this table should be stored
    //start_[tbl];  the position in the buffer where the log records for this table start
    //len_[tbl];    the capacity of the log for this table

    //assert(!recovering);
    size_t n = logrec_kvdelta::size(key.len, value.len)
        + logrec_epoch::size() + logrec_base::size();
    waitlist wait = { &wait };
    int stalls = 0;
    while (1) {
        if ((start_[tbl]+len_[tbl]) - pos_[tbl] >= n
            && (wait.next == &wait || f_.waiting_ == &wait)) {
            kvepoch_t we = global_wake_epoch;

            // Potentially record a new epoch.
            if (qtimes.epoch != log_epoch_) {
                log_epoch_ = qtimes.epoch;
                std::cout<<"log_epoch: "<< log_epoch_<<std::endl;
                pos_[tbl] += logrec_epoch::store(buf_ + pos_[tbl], logcmd_epoch, qtimes.epoch);
                #if MEASURE_LOG_RECORDS
                incr_cur_log_records();
                #endif
            }

            if (quiescent_epoch_) {
                // We're recording a new log record on a log that's been
                // quiescent for a while. If the quiescence marker has been
                // flushed, then all epochs less than the query epoch are
                // effectively on disk.
                if (flushed_epoch_ == quiescent_epoch_)
                    flushed_epoch_ = qtimes.epoch;
                quiescent_epoch_ = 0;
                while (we < qtimes.epoch)
                    we = cmpxchg(&global_wake_epoch, we, qtimes.epoch);
            }
            
            // Log epochs should be recorded in monotonically increasing
            // order, but the wake epoch may be ahead of the query epoch (if
            // the query took a while). So potentially record an EARLIER
            // wake_epoch. This will get fixed shortly by the next log
            // record.
            if (we != wake_epoch_ && qtimes.epoch < we)
                we = qtimes.epoch;
            if (we != wake_epoch_) {
                wake_epoch_ = we;
                pos_[tbl] += logrec_base::store(buf_ + pos_[tbl], logcmd_wake);
                #if MEASURE_LOG_RECORDS
                incr_cur_log_records();
                #endif
            }
            if (command == logcmd_put && qtimes.prev_ts
                && !(qtimes.prev_ts & 1))
                pos_[tbl] += logrec_kvdelta::store(buf_ + pos_[tbl],
                                              logcmd_modify, key, value,
                                              qtimes.prev_ts, qtimes.ts);
            else
                pos_[tbl] += logrec_kv::store(buf_ + pos_[tbl],
                                         command, key, value, qtimes.ts);
            #if MEASURE_LOG_RECORDS
            incr_cur_log_records();
            #endif
            if (f_.waiting_ == &wait)
                f_.waiting_ = wait.next;
            release();
            return;
        }
        //printf("wait.next == wait? %d, f_.waiting_ == wait? %d\n", (wait.next == &wait) ,  (f_.waiting_ == &wait) );
        // Otherwise must spin
        if (wait.next == &wait) {
            waitlist** p = &f_.waiting_;
            while (*p)
                p = &(*p)->next;
            *p = &wait;
            wait.next = 0;
        }
        release();
        if (stalls == 0)
            printf("stall\n");
        else if (stalls % 25 == 0) {
            printf("stall %d\n", stalls);
        }
        ++stalls;
        napms(50);
        acquire();
    }
}




template <typename R>
struct row_delta_marker : public row_marker {
    kvtimestamp_t prev_ts_;
    R *prev_;
    char s_[0];
};

template <typename R>
inline bool row_is_delta_marker(const R* row) {
    if (row_is_marker(row)) {
        const row_marker* m =
            reinterpret_cast<const row_marker *>(row->col(0).s);
        return m->marker_type_ == m->mt_delta;
    } else
        return false;
}

template <typename R>
inline row_delta_marker<R>* row_get_delta_marker(const R* row, bool force = false) {
    (void) force;
    assert(force || row_is_delta_marker(row));
    return reinterpret_cast<row_delta_marker<R>*>
        (const_cast<char*>(row->col(0).s));
}

#endif
