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

#define MEASURE_LOG_RECORDS 0
#define LOG_PER_TABLE 1 // support separate log per table. We will simply index the log (pointer arithmetic) to go to the log records of a specific table.  

class logset;

using lcdf::Str;
namespace lcdf { class Json; }

template <int N_TBLS> 
class logset_tables;

// in-memory log, one per thread. Buffer is indexed by table and thus we need one loginfo for all tables.
template <int N_TBLS=1>
class loginfo_tables {
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
            uint32_t pos_[N_TBLS]; // position of the next log entry to store (offset of pointer to buffer) for each table
            uint32_t start_[N_TBLS]; // position in the buffer where each table log starts
            uint32_t len_[N_TBLS]; // total length of log for each table

            threadinfo *ti_;
            int logindex_;
        };
        struct {
            char cache_line_2_ [ ((sizeof(char*) + sizeof(threadinfo*) + sizeof(int)+ 3 * sizeof(uint32_t) * N_TBLS)/CACHE_LINE_SIZE) * CACHE_LINE_SIZE + CACHE_LINE_SIZE - sizeof(logset_info)];
            logset_info lsi_; // 8 bytes
        };
    };

    loginfo_tables(logset_tables<N_TBLS>* ls, int logindex, std::vector<int>& tbl_sizes){
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
            pos_[tbl] = tbl==0 ? 0 : (pos_[tbl-1] + len_[tbl-1]);
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
    
    ~loginfo_tables(){
        free(buf_);
    }

    friend class logset_tables<N_TBLS>;

};


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

template <int N_TBLS=1>
class logset_tables{
  public:
    static logset_tables* make(int size, std::vector<int>& tbl_sizes){
        static_assert((sizeof(loginfo_tables<N_TBLS>) % CACHE_LINE_SIZE) == 0, "unexpected sizeof(loginfo)");
        always_assert(N_TBLS == tbl_sizes.size(), "table sizes should be the same as N_TBLS");
        std::cout<<"Size of loginfo_tables: "<< sizeof(loginfo_tables<N_TBLS>)<<std::endl;
        assert(size > 0 && size <= 64);
        char* x = new char[sizeof(loginfo_tables<N_TBLS>) * size + sizeof(typename loginfo_tables<N_TBLS>::logset_info) + CACHE_LINE_SIZE];
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

    static void free(logset_tables* ls){
        for (int i = 0; i != ls->size(); ++i)
            ls->li_[i].~loginfo();
        delete[] (reinterpret_cast<char*>(ls) + ls->li_[-1].lsi_.allocation_offset_);
    }

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
