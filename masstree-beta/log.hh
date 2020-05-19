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

#include <unordered_map>

#include "../benchmark/DB_params.hh"

#define MEASURE_LOG_RECORDS 0

#ifndef LOG_NTABLES // should be defined in TPCC_bench.hh
#define LOG_NTABLES 0
#endif

// 1 for std::unordered_map
// 2 for STO Uindex (non-transactional)
#define MAP 1
#define STD_MAP (MAP == 1 || LOG_NTABLES == 0) // if LOG_NTABLES is not set, then use default std::unordered_map since STO UIndex is not defined! (That's when we include log.hh from different files)
#define STO_MAP (MAP == 2 && LOG_NTABLES > 0)


class logset;

using lcdf::Str;
namespace lcdf { class Json; }

template <int N_TBLS> 
class logset_tbl;

template <int N_TBLS>
class logset_map;

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

    char* get_buf(){ // we need the pointer to the buffer for log replay (since this log is not intended to be flushed to disk)
        return buf_ ;
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

struct logrec {
    enum class NamedColumn : int { 
        cmd = 0, val, epoch, ts, next
    };
    uint32_t command;
    Str val;
    kvepoch_t epoch;
    kvtimestamp_t ts;
    logrec* next;

    logrec() : command(0), val(""), epoch(0), ts(0), next(0) {}
    logrec(uint32_t cmd, Str v, kvepoch_t e, kvtimestamp_t t, logrec* n): command(cmd), val(v), epoch(e), ts(t), next(n) {}
};

template <int N_TBLS=1>
class loginfo_map {
  public:
    // logging
    struct query_times {
        kvepoch_t epoch;
        kvtimestamp_t ts;
        kvtimestamp_t prev_ts;
    };

    struct logset_info {
        int32_t size_;
        int allocation_offset_;
    };
    
    loginfo_map(int logindex);
    ~loginfo_map();

    void record(int command, const query_times& qt, Str key, Str value, int tbl);

    uint32_t current_size(int tbl){
        (void)tbl;
        return 0;
    }

    uint32_t cur_log_records(int tbl){
        #if STD_MAP
        return map[tbl].size();
        #elif STO_MAP
        return map[tbl].nbuckets();
        #endif
    }

    private:

    #if STD_MAP
    typedef std::unordered_map<Str, logrec> logmap_t;
    #elif STO_MAP
    typedef bench::unordered_index<Str, logrec, db_params::db_default_params> logmap_t;
    #endif
    
    logmap_t map [N_TBLS]; // N_TBLS * 56

    char cache_line_2_ [ ((N_TBLS * sizeof(logmap_t) + 2*sizeof(threadinfo*) )  % CACHE_LINE_SIZE  > 0? (CACHE_LINE_SIZE -  (N_TBLS * sizeof(logmap_t) + 2*sizeof(threadinfo*) )  % CACHE_LINE_SIZE) : 0) ];

    union { // 32 bytes
        struct { // 12 bytes
            threadinfo *ti_;
            int logindex_;
            //logmap_t * map [N_TBLS]; //N_TBLS * 8 bytes
        };
        struct {
            //char cache_line_2_[ ((sizeof(threadinfo*) + sizeof(int) + N_TBLS * sizeof(std::unordered_map<Str, logrec>)) / CACHE_LINE_SIZE ) * CACHE_LINE_SIZE +
            // ((sizeof(threadinfo*) + sizeof(int) + N_TBLS * sizeof(std::unordered_map<Str, logrec>)) % CACHE_LINE_SIZE == 0? /* 0 */ CACHE_LINE_SIZE : CACHE_LINE_SIZE) - sizeof(logset_info) ];
            // the compiler assigns more memory sometimes in order to automatically align by cache line. This makes it occupy more memory than the calculated one! Thus, give it on more cache line always.
            logset_info lsi_;
        };
    };

    friend class logset_map<N_TBLS>;

};

/*=========================*\
|   ONE LOG PER TABLE       |
|===========================|
|* Divide log buffer into  *|
|* tables and index to get *|
|* to the desired table.   *|
\*_________________________*/
// Support separate log per table. We will simply index the log (pointer arithmetic) to go to the log records of a specific table.  

// in-memory log, one per thread. Buffer is indexed by table and thus we need one loginfo for all tables.
template <int N_TBLS=1>
class loginfo_tbl {
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

    char* get_buf(uint32_t tbl){ // we need the pointer to the buffer of the given table. This will be used for log replay (since this log is not intended to be flushed to disk)
        return buf_  + start_[tbl];
    }

    private:
    struct waitlist {
        waitlist* next;
    };
    struct front { // 20 bytes
        uint32_t lock_;
        waitlist* waiting_;
        logset_tbl<N_TBLS>* logset_;
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
    // we don't need quiescent, wake, flushed for in-memory log
    char padding1_[CACHE_LINE_SIZE - sizeof(front) - 1* sizeof(kvepoch_t)]; // we can fit all these in one cache line.
    #endif

    kvepoch_t log_epoch_;       // epoch written to log (non-quiescent)
    // TODO: might not needed since we don't flush this log to disk!
    //kvepoch_t quiescent_epoch_; // epoch we went quiescent
    //kvepoch_t wake_epoch_;      // epoch for which we recorded a wake command
    //kvepoch_t flushed_epoch_;   // epoch fsync()ed to disk


    
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
            char cache_line_2_ [ ((sizeof(char*) + sizeof(threadinfo*) + sizeof(int)+ 3 * sizeof(uint32_t) * N_TBLS)/CACHE_LINE_SIZE) * CACHE_LINE_SIZE + 
            ((sizeof(char*) + sizeof(threadinfo*) + sizeof(int)+ 3 * sizeof(uint32_t) * N_TBLS) % CACHE_LINE_SIZE == 0? /* 0 */ CACHE_LINE_SIZE: CACHE_LINE_SIZE) - sizeof(logset_info)];
            // the compiler assigns more memory sometimes in order to automatically align by cache line. This makes it occupy more memory than the calculated one! Thus, give it on more cache line always.
            logset_info lsi_; // 8 bytes
        };
    };

    loginfo_tbl(logset_tbl<N_TBLS>* ls, int logindex, std::vector<int>& tbl_sizes);
    
    ~loginfo_tbl();

    friend class logset_tbl<N_TBLS>;

};


class logset_base {

};


template <int N_TBLS=1>
class logset_tbl : public logset_base {
  public:
    static logset_tbl* make(int size, std::vector<int>& tbl_sizes);

    static void free(logset_tbl* ls);

    inline int size() const;
    inline loginfo_tbl<N_TBLS>& log(int i);
    inline const loginfo_tbl<N_TBLS>& log(int i) const;

  private:
    loginfo_tbl<N_TBLS> li_[0];

};

class logset  : public logset_base {
  public:
    static logset* make(int size);
    static void free(logset* ls);

    inline int size() const;
    inline loginfo& log(int i);
    inline const loginfo& log(int i) const;

  private:
    loginfo li_[0];
};

template<int N_TBLS=1>
class logset_map: public logset_base {
  public:
    static logset_map<N_TBLS>* make(int size);
    static void free(logset_map<N_TBLS>* ls);

    inline int size() const;
    inline loginfo_map<N_TBLS>& log(int i);
    inline const loginfo_map<N_TBLS>& log(int i) const;

  private:
    loginfo_map<N_TBLS> li_[0];
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
// buf:         the pointer in the log buffer from where to start replaying (it could be any location within the buffer)
// size:        the size of the log to be played, starting from buf.
// to_epoch:    the epoch until which it should replay. When reaches an epoch greater than to_epoch, it will stop and store the location in buf.
class logmem_replay {
    public:
    logmem_replay(char * buf, off_t size) : size_(size), buf_(buf) {}
    ~logmem_replay(){}

    struct info_type {
        kvepoch_t to_epoch;
        std::unordered_map<uint64_t, char*> epoch_ptr; // we need to know where each epoch starts in the buffer. That way, we don't have to traverse the entire buffer to learn about the epoch's location, but 
        // rather learn them as we go. We need to store where each epoch starts so that next time to be able to start from that location in the buffer, rather than the beginning.
    };

    info_type info() const;
    // to_epoch: until which epoch to play the log. As soon as we encounter a log epoch greater than to_epoch, we mark the location in the buffer in epoch_ptr and return
    template <typename Callback>
    uint64_t replay(kvepoch_t to_epoch, Callback callback);

  private:
    off_t size_;
    char *buf_;

};

// LOG RECORD

struct logmem_record {
    uint32_t command;
    Str key;
    Str val;
    kvtimestamp_t ts;
    kvtimestamp_t prev_ts;
    kvepoch_t epoch;

    const char *extract(const char *buf, const char *end);
};

// replays the in-memory log and returns the 
// number of entries found.
template <typename Callback>
uint64_t logmem_replay::replay(kvepoch_t to_epoch, Callback callback){
    uint64_t nr = 0;
    const char *pos = buf_, *end = buf_ + size_;
    logmem_record lr;
    
    std::cout<<"Replaying log of size "<< size_ <<std::endl;

    // XXX
    while (pos < end) {
        const char *nextpos = lr.extract(pos, end);
        if (lr.command == logcmd_none) {
            fprintf(stderr, "replay: %" PRIu64 " entries replayed, CORRUPT @%zu\n",
                    nr, pos - buf_);
            break;
        }
        if(lr.command == logcmd_epoch){
            if(lr.epoch > to_epoch){ // reached to_epoch: stop playing!
                std::cout<<"Replay reached epoch "<<to_epoch.value()<<". Stop!\n";
                return nr;
            }
        }
        if (lr.key.len) { // skip empty entry
            if (lr.command == logcmd_put
                || lr.command == logcmd_replace
                || lr.command == logcmd_modify
                || lr.command == logcmd_remove) {
                callback(lr.key);
            }
            ++nr;
        }
        // XXX RCU
        pos = nextpos;
    }
    return nr;
}

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


/*=============================================================================================================================*/

/*=========================*\
|       loginfo_map         |
|===========================|
|*    Implementation of    *|
|*    loginfo_map          *|
|*    member functions.    *|
\*_________________________*/
template <int N_TBLS>
loginfo_map<N_TBLS>::loginfo_map(int logindex) {
    for(int i=0; i<N_TBLS; i++)
        map[i] = loginfo_map<N_TBLS>::logmap_t(1024 * 1024);
    ti_ = 0;
    logindex_ = logindex;
}

template <int N_TBLS>
loginfo_map<N_TBLS>::~loginfo_map(){

}
template <int N_TBLS>
void loginfo_map<N_TBLS>::record(int command, const loginfo_map<N_TBLS>::query_times& qt, Str key, Str value, int tbl){
    auto & m = map[tbl];
    #if STD_MAP
    auto elem = m.find(key);
    if( elem!= m.end()){ // found - check epoch!
        logrec& rec = elem->second;
        if(rec.epoch < qt.epoch){ // new log record has a newer epoch than existing log record! Create a new one and chain it! - we expect to only have up to 2 elements in the chain since the log drainer creates new epoch and once done we can discard the older epochs.
            logrec * newrec = new logrec(command, value, qt.epoch, qt.ts, nullptr);
            rec.next = newrec;
        }
    }
    else {
        m[key] = logrec(command, value, qt.epoch, qt.ts, nullptr);
    }
    #elif STO_MAP
    auto * elem = m.nontrans_get(key);
    if (elem != nullptr){ // found - check epoch!
        if(elem->epoch < qt.epoch){
            logrec * newrec = new logrec(command, value, qt.epoch, qt.ts, nullptr);
            elem->next = newrec;
        }
    }
    else{
        m.nontrans_put(key, logrec(command, value, qt.epoch, qt.ts, nullptr));
    }

    #endif
}



template <int N_TBLS>
inline loginfo_map<N_TBLS>& logset_map<N_TBLS>::log(int i) {
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}
template <int N_TBLS>
inline const loginfo_map<N_TBLS>& logset_map<N_TBLS>::log(int i) const {
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}



/*=========================*\
|       logset_map          |
|===========================|
|*    Implementation of    *|
|*    logset_map           *|
|*    member functions.    *|
\*_________________________*/

template <int N_TBLS>
logset_map<N_TBLS>* logset_map<N_TBLS>::make(int size){
    assert(size > 0 && size <= 64);
    static_assert((sizeof(loginfo_map<N_TBLS>) % CACHE_LINE_SIZE)==0, "unexpected size of loginfo_map");
    std::cout<<"Size of loginfo_map: "<< sizeof(loginfo_map<N_TBLS>)<<std::endl;
    
    char* x = new char[sizeof(loginfo_map<N_TBLS>) * size + sizeof(typename loginfo_map<N_TBLS>::logset_info) + CACHE_LINE_SIZE];
    char* ls_pos = x + sizeof(typename loginfo_map<N_TBLS>::logset_info);
    uintptr_t left = reinterpret_cast<uintptr_t>(ls_pos) % CACHE_LINE_SIZE;
    if (left)
        ls_pos += CACHE_LINE_SIZE - left;
    logset_map<N_TBLS>* ls = reinterpret_cast<logset_map<N_TBLS>*>(ls_pos);
    ls->li_[-1].lsi_.size_ = size;
    ls->li_[-1].lsi_.allocation_offset_ = (int) (x - ls_pos);
    for (int i = 0; i != size; ++i)
        new((void*) &ls->li_[i]) loginfo_map<N_TBLS>(i);
    std::cout<<"Size of loginfo_map: "<< sizeof(loginfo_map<N_TBLS>)<<std::endl;
    return ls;
}

template <int N_TBLS>
void logset_map<N_TBLS>::free(logset_map<N_TBLS>* ls){
    for (int i = 0; i != ls->size(); ++i)
        ls->li_[i].~loginfo_map();
    delete[] (reinterpret_cast<char*>(ls) + ls->li_[-1].lsi_.allocation_offset_);
}

template <int N_TBLS>
inline int logset_map<N_TBLS>::size() const{
    return li_[-1].lsi_.size_;
}



/*=========================*\
|       logset_tbl       |
|===========================|
|*    Implementation of    *|
|*    logset_tbl        *|
|*    member functions.    *|
\*_________________________*/
template <int N_TBLS>
logset_tbl<N_TBLS>* logset_tbl<N_TBLS>::make(int size, std::vector<int>& tbl_sizes){
    static_assert((sizeof(loginfo_tbl<N_TBLS>) % CACHE_LINE_SIZE) == 0, "unexpected sizeof(loginfo)");
    always_assert(N_TBLS == tbl_sizes.size(), "table sizes should be the same as N_TBLS");
    //std::cout<<"Size of loginfo_tbl: "<< sizeof(loginfo_tbl<N_TBLS>)<<std::endl;
    assert(size > 0 && size <= 64);
    char* x = new char[sizeof(loginfo_tbl<N_TBLS>) * size + sizeof(typename loginfo_tbl<N_TBLS>::logset_info) + CACHE_LINE_SIZE];
    char* ls_pos = x + sizeof(typename loginfo_tbl<N_TBLS>::logset_info);
    
    uintptr_t left = reinterpret_cast<uintptr_t>(ls_pos) % CACHE_LINE_SIZE;
    if (left)
        ls_pos += CACHE_LINE_SIZE - left;
    logset_tbl<N_TBLS>* ls = reinterpret_cast<logset_tbl<N_TBLS>*>(ls_pos);
    ls->li_[-1].lsi_.size_ = size;
    ls->li_[-1].lsi_.allocation_offset_ = (int) (x - ls_pos);
    for (int i = 0; i != size; ++i)
        new((void*) &ls->li_[i]) loginfo_tbl<N_TBLS>(ls, i, tbl_sizes);
    return ls;
}

template <int N_TBLS>
void logset_tbl<N_TBLS>::free(logset_tbl* ls){
    for (int i = 0; i != ls->size(); ++i)
        ls->li_[i].~loginfo_tbl();
    delete[] (reinterpret_cast<char*>(ls) + ls->li_[-1].lsi_.allocation_offset_);
}

template <int N_TBLS>
inline int logset_tbl<N_TBLS>::size() const {
    return li_[-1].lsi_.size_;
}


/*=========================*\
|       loginfo_tbl      |
|===========================|
|*    Implementation of    *|
|*    loginfo_tbl.      *|
\*_________________________*/
template <int N_TBLS>
loginfo_tbl<N_TBLS>::loginfo_tbl(logset_tbl<N_TBLS>* ls, int logindex, std::vector<int>& tbl_sizes){
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
    //std::cout<<"sizeof loginfo_tbl: "<<sizeof(*this)<<std::endl;
    log_epoch_ = 0;
    //quiescent_epoch_ = 0;
    //wake_epoch_ = 0;
    //flushed_epoch_ = 0;

    ti_ = 0;
    f_.logset_ = ls;
    logindex_ = logindex;
    (void) padding1_;
}

template <int N_TBLS>
loginfo_tbl<N_TBLS>::~loginfo_tbl(){
    free(buf_);
}

template <int N_TBLS>
inline void loginfo_tbl<N_TBLS>::acquire() {
    test_and_set_acquire(&f_.lock_);
}

template <int N_TBLS>
inline void loginfo_tbl<N_TBLS>::release() {
    test_and_set_release(&f_.lock_);
}

template <int N_TBLS>
inline loginfo_tbl<N_TBLS>& logset_tbl<N_TBLS>::log(int i){
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}

template <int N_TBLS>
inline const loginfo_tbl<N_TBLS>& logset_tbl<N_TBLS>::log(int i) const{
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}


// tbl: The table where the log record belongs
template <int N_TBLS>
void loginfo_tbl<N_TBLS>::record(int command, const query_times& qtimes, Str key, Str value, int tbl){
    //pos_[tbl];    the position in the buffer where the next log record for this table should be stored
    //start_[tbl];  the position in the buffer where the log records for this table start
    //len_[tbl];    the capacity of the log for this table

    //assert(!recovering);
    //size_t n = logrec_kvdelta::size(key.len, value.len)
    //    + logrec_epoch::size() + logrec_base::size();
    // not needed for in-memory log
    size_t n = logrec_kvdelta::size(key.len, value.len)
        + logrec_epoch::size();
    waitlist wait = { &wait };
    int stalls = 0;
    while (1) {
        if ((start_[tbl]+len_[tbl]) - pos_[tbl] >= n
            && (wait.next == &wait || f_.waiting_ == &wait)) {
            //kvepoch_t we = global_wake_epoch;

            // Potentially record a new epoch.
            if (qtimes.epoch != log_epoch_) {
                log_epoch_ = qtimes.epoch;
                //std::cout<<"changing log_epoch to: "<< log_epoch_.value()<<std::endl;
                pos_[tbl] += logrec_epoch::store(buf_ + pos_[tbl], logcmd_epoch, qtimes.epoch);
                #if MEASURE_LOG_RECORDS
                incr_cur_log_records();
                #endif
            }
            /* -- Not needed for in-memory log
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
            */
            if (command == logcmd_put && qtimes.prev_ts
                && !(qtimes.prev_ts & 1))
                pos_[tbl] += logrec_kvdelta::store(buf_ + pos_[tbl],
                                              logcmd_modify, key, value,
                                              qtimes.prev_ts, qtimes.ts);
            else
                pos_[tbl] += logrec_kv::store(buf_ + pos_[tbl],
                                         command, key, value, qtimes.ts);
            assert(command != logcmd_none);
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
