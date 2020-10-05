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

#include <time.h> 



#include <unordered_map>

#include "../benchmark/DB_params.hh"

#define MEASURE_LOG_RECORDS 1
#define MEASURE_MAP_SZ 1

#ifndef LOG_NTABLES // should be defined in TPCC_bench.hh
#define LOG_NTABLES 0
#endif
#ifndef LOG_RECS // should be defined in TPCC_bench.hh
#define LOG_RECS 0
#endif
#ifndef LOGREC_OVERWRITE // should be defined in TPCC_bench.hh
#define LOGREC_OVERWRITE 0
#endif

#ifndef LOG_DRAIN_REQUEST // should be defined in TPCC_bench.hh
#define LOG_DRAIN_REQUEST 0
#endif

#ifndef NLOGGERS // should be defined in TPCC_bench.hh
#define NLOGGERS 10
#endif

// 1 for std::unordered_map
// 2 for STO Uindex (non-transactional)
#define MAP 2
//#define STD_MAP (MAP == 1 || LOG_NTABLES == 0) // if LOG_NTABLES is not set, then use default std::unordered_map since STO UIndex is not defined! (That's when we include log.hh from different files)
//#define STO_MAP (MAP == 2 && LOG_NTABLES > 0)
#define STD_MAP 1

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
    // pass the table ID as a parameter, so that to know for which table this record is
    void record(int command, const query_times& qt, Str key, Str value, char tbl);

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

// the log record stored in the map
struct logmap_rec {
    enum class NamedColumn : int { 
        cmd = 0, val, epoch, ts, next
    };
    uint32_t command;
    Str val;
    kvepoch_t epoch;
    kvtimestamp_t ts;
    logmap_rec* next;

    logmap_rec() : command(0), val(""), epoch(0), ts(0), next(0) {}
    logmap_rec(uint32_t cmd, Str v, kvepoch_t e, kvtimestamp_t t, logmap_rec* n): command(cmd), val(v), epoch(e), ts(t), next(n) {}
};

#if STD_MAP
typedef std::unordered_map<Str, logmap_rec> logmap_t;
#elif STO_MAP
typedef bench::unordered_index<Str, logmap_rec, db_params::db_default_params> logmap_t;
#endif

template <int N_TBLS=1>
class loginfo_map {
  public:
    
    struct logset_info {
        int32_t size_;
        int allocation_offset_;
    };
    
    loginfo_map(int logindex);
    ~loginfo_map();

    void record(int command, const loginfo::query_times& qt, Str key, Str value, int tbl);

    uint32_t current_size(int tbl){
        (void)tbl;
    #if MEASURE_MAP_SZ
        return size[tbl];
    #else
        return 0;
    #endif
    }

    uint32_t cur_log_records(int tbl){
        #if STD_MAP
        return map[tbl].size();
        #elif STO_MAP
        return map[tbl].nbuckets(); // this will return the initial size of the map and not the actual contents (initialized with .resize())
        #endif
    }

    logmap_t& get_map(int tbl){
        return map[tbl];
    }

    private:
    
    logmap_t map [N_TBLS]; // N_TBLS * 56
    #if MEASURE_MAP_SZ
    unsigned size[N_TBLS];
    char cache_line_2_ [ ((N_TBLS * sizeof(logmap_t) + N_TBLS * sizeof(unsigned) + 2*sizeof(threadinfo*) )  % CACHE_LINE_SIZE  > 0? (CACHE_LINE_SIZE -  (N_TBLS * sizeof(logmap_t) + (N_TBLS * sizeof(unsigned)) + 2*sizeof(threadinfo*) )  % CACHE_LINE_SIZE) : 0) ];
    #else
    char cache_line_2_ [ ((N_TBLS * sizeof(logmap_t) + 2*sizeof(threadinfo*) )  % CACHE_LINE_SIZE  > 0? (CACHE_LINE_SIZE -  (N_TBLS * sizeof(logmap_t) + 2*sizeof(threadinfo*) )  % CACHE_LINE_SIZE) : 0) ];
    #endif


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
    // return the position of the log record we just added! Then we will store it in the internal_elem of Masstree and be able to find it easily if there is an update on the same key!
    uint32_t record(int command, const loginfo::query_times& qt, Str key, Str value, int tbl, int pos=-1);
    void record(int command, const loginfo::query_times& qt, Str key,
                const lcdf::Json* req, const lcdf::Json* end_req);

    void record(int command, const loginfo::query_times& qtimes, const void* keyp, uint32_t keylen, const void* valp, uint32_t vallen, int tbl);

    void record_new_epoch(int tbl, kvepoch_t new_epoch);

    uint32_t current_size(uint32_t tbl){
        return pos_[tbl] - start_[tbl];
    }

    void reset_all(){
        for (uint32_t i=0; i<N_TBLS; i++){
            pos_[i]=start_[i];
        }
        log_epoch_ = 1; // reset log epoch
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

    char* get_buf(uint32_t tbl){ // we need the pointer to the buffer of the given table. This will be used for log replay (since this log is not intended to be flushed to disk)
        return buf_  + start_[tbl];
    }

    int get_log_index(){
        return logindex_;
    }

    inline kvepoch_t get_local_epoch(){
        return log_epoch_;
    }

    inline void wait_to_logdrain(){
        //struct timespec abstime;
        //abstime.tv_sec = 0;
        //abstime.tv_nsec = 10000; // 10us
        assert(pthread_mutex_lock(&mutex) == 0);
        //int r = pthread_cond_timedwait(&logdrain_start_cond, &mutex, &abstime);
        //assert(r == 0 || r == ETIMEDOUT);
        int r = pthread_cond_wait(&logdrain_start_cond, &mutex);
        assert(r==0);
        assert(pthread_mutex_unlock(&mutex) == 0);
    }

    inline void signal_to_logdrain(){
        assert(pthread_mutex_lock(&mutex) == 0);
        assert(pthread_cond_signal(&logdrain_start_cond)==0);
        assert(pthread_mutex_unlock(&mutex) == 0);
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
        //#if LOG_DRAIN_REQUEST
        //bool request_log_drain = false; // set by a log drainer thread and checked by the log writer
        //#endif
    };
    
    front f_;

   
    
    #if MEASURE_LOG_RECORDS
    uint32_t records_num_; // Dimos - number of log records currently stored
    char padding1_[CACHE_LINE_SIZE - sizeof(front) - 1* sizeof(kvepoch_t) - sizeof(uint32_t)]; // subtract the size of records_num_ also!
    #else
    // we don't need quiescent, wake, flushed for in-memory log
    char padding1_[CACHE_LINE_SIZE - sizeof(front) - 1* sizeof(kvepoch_t)]; // we can fit all these in one cache line.
    #endif

    kvepoch_t log_epoch_=1;       // epoch written to log (non-quiescent)
    // TODO: might not needed since we don't flush this log to disk!
    //kvepoch_t quiescent_epoch_; // epoch we went quiescent
    //kvepoch_t wake_epoch_;      // epoch for which we recorded a wake command
    //kvepoch_t flushed_epoch_;   // epoch fsync()ed to disk


    pthread_mutex_t mutex; // 40 bytes
    pthread_cond_t logdrain_start_cond; //48 bytes

    char cache_line_cond [2 * CACHE_LINE_SIZE - sizeof(pthread_mutex_t) - sizeof(pthread_cond_t)];

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

    #if LOG_DRAIN_REQUEST
    //inline bool has_logdrain_request();
    //inline void set_logdrain_request(bool req);
    #endif

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
    logcmd_wake = 0x4B41576B,            // "kWAK"
    logcmd_wraparound = 0x4152576B              // "kWRA"
};


// replay the in-memory log.
// buf:         the pointer in the log buffer from where to start replaying (it could be any location within the buffer)
// to_epoch:    the epoch until which it should replay. When reaches an epoch greater than to_epoch, it will stop and store the location in buf.
class logmem_replay {
    public:
    logmem_replay(char * buf) : buf_(buf) {
        info.next_epoch = 0;
    }
    ~logmem_replay(){}

    struct info_type {
        kvepoch_t next_epoch; // the epoch after the one we applied
        std::unordered_map<uint64_t, const char*> epoch_ptr; // we need to know where each epoch starts in the buffer. That way, we don't have to traverse the entire buffer to learn about the epoch's location, but 
        // rather learn them as we go. We need to store where each epoch starts so that next time to be able to start from that location in the buffer, rather than the beginning.
    };

    info_type get_info() const;
    // to_epoch:    until which epoch to play the log. As soon as we encounter a log epoch to_epoch, we mark the location in the buffer in epoch_ptr, store that epoch in next_epoch and return
    // tbl:         the table that we want to replay: if used a per table log, -1 otherwise
    // size:        the size of the log to be played, starting from buf.
    template <typename Callback>
    uint64_t replay(kvepoch_t to_epoch, off_t size, Callback callback, int tbl);

  private:
    char *buf_;
    info_type info;

};

class logmap_replay {

    public:
    logmap_replay(logmap_t& m): map(m){}

    template <typename Callback>
    uint64_t replay(kvepoch_t to_epoch, Callback callback, int tbl);

    private:
    kvepoch_t to_epoch;
    logmap_t & map;

};


template <typename Callback>
uint64_t logmap_replay::replay(kvepoch_t to_epoch, Callback callback, int tbl){
    uint64_t recs_replayed=0;
    #if STD_MAP
    for(auto & elem : map){
        if(to_epoch.value() == 0 || elem.second.epoch.value() <= to_epoch.value()){ // only process log records with epoch within the given epoch
            callback(elem.first, elem.second.val, elem.second.ts, elem.second.command, tbl);
            recs_replayed++;
        }
    }
    #elif STO_MAP
    auto map_callback = [&to_epoch, &callback, &recs_replayed, &tbl] (const Str& k, logmap_rec* v) -> void {
        if(to_epoch.value() == 0 || v->epoch.value() <= to_epoch.value()){ // only process log records with epoch within the given epoch
            callback(k, v->val, v->ts, v->command, tbl);
            recs_replayed++;
        }
    };
    map.traverse_all(map_callback);
    #endif
    return recs_replayed;
}

// LOG RECORD

struct logmem_record {
    uint32_t command;
    Str key;
    Str val;
    kvtimestamp_t ts;
    kvtimestamp_t prev_ts;
    kvepoch_t epoch;
    int tbl;

    const char *extract(const char *buf, const char *end);
    const char *extract(const char *buf, const char *end, uint32_t value_offset, bool extract_tbl); // ignore the '/' between key and val when LOG_RECS == 3!
    // the extract_tbl is for LOGGING 1 where we have a global log (not one log per table). We also need to store for which table this log record is!
};

// replays the in-memory log up to to_epoch and returns the number of entries found.
// replays the entire log if to_epoch == 0
// starts playing either from the beginning of the log buffer, or from the location where it left (epoch_ptr[next_epoch])
// tbl is the table number if selected type of log is per table, otherwise -1
template <typename Callback>
uint64_t logmem_replay::replay(kvepoch_t to_epoch, off_t size, Callback callback, int tbl){
    uint64_t nr = 0;
    const char *pos;
    const char *end = buf_ + size;
    logmem_record lr;
    bool play_till_epoch = to_epoch > 0;

    if(play_till_epoch && info.epoch_ptr.count(info.next_epoch.value()) > 0){
        pos = info.epoch_ptr[info.next_epoch.value()]; // start where we left off
        //printf("Replaying from epoch %lu, tbl %d, pos %p\n", info.next_epoch.value(), tbl, pos);
    }
    else
        pos = buf_; // start from the beginning
    
    //std::cout<<"Replaying log of size "<< size <<std::endl;
    // XXX
    //while (pos < end) {
    while(true){
        if(!play_till_epoch && pos >= end) // play till the end only if to_epoch == 0! If we play till a specific epoch, we don't know exactly the end since it is still being updated by the log writers!
            break;
        #if LOG_RECS < 3
        const char *nextpos = lr.extract(pos, end, 0, (tbl==-1));
        #elif LOG_RECS == 3
        const char *nextpos = lr.extract(pos, end, 1, (tbl==-1));
        #endif
        if (lr.command == logcmd_none) {
            fprintf(stderr, "replay: %" PRIu64 " entries replayed, CORRUPT @%zu\n",
                    nr, pos - buf_);
            always_assert(false, "Corrupted Log!\n");
            break;
        }
        if(lr.command == logcmd_epoch){
            if(play_till_epoch && lr.epoch > to_epoch){ // played everything till to_epoch: stop playing and save location of that epoch to next_epoch!
                //assert(lr.epoch == to_epoch+1);
                //std::cout<<"Replay reached epoch "<<lr.epoch.value()<<". Stop!\n";
                info.next_epoch = lr.epoch;
                info.epoch_ptr[lr.epoch.value()] = nextpos;
                return nr;
            }
        }
        if(lr.command == logcmd_wraparound){ // wraparound
            nextpos = buf_; // start replaying from the beginning!
            pos = nextpos;
            continue;
        }
        if (lr.key.len) { // skip empty entry
            if (lr.command == logcmd_put
                || lr.command == logcmd_replace
                || lr.command == logcmd_modify
                || lr.command == logcmd_remove) {
                if(!callback(lr.key, lr.val, lr.ts, lr.command, (tbl==-1 ? lr.tbl : tbl))){
                    std::cout<<"Error in callback @Log record "<< nr << std::endl;
                    return nr;
                }
            }
            ++nr;
        }
        // XXX RCU
        pos = nextpos;
    }
    always_assert(!play_till_epoch, "Reached log end and couldn't find given epoch!");
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

// support wrapping around when log is full and log readers already caught up (periodic log drains)!
struct logrec_wraparound {
    uint32_t command_;
    uint32_t size_;

    static size_t size() {
        return sizeof (logrec_wraparound);
    }

    static size_t store(char* buf, uint32_t command){
        logrec_wraparound* lr = reinterpret_cast<logrec_wraparound*>(buf);
        lr->command_ = command;
        lr->size_ = sizeof(*lr);
        return sizeof(*lr);
    }

    static bool check(const char* buf){
        const logrec_wraparound* lr = reinterpret_cast<const logrec_wraparound*>(buf);
        return lr->size_ >= sizeof(*lr);
    }
};


// mark for which table this record is!
struct logrec_kv_tbl  {
    uint32_t command_;
    uint32_t size_;
    kvtimestamp_t ts_;
    uint32_t keylen_;
    char tbl_; // the table ID
    char buf_[0];
    // LOG_RECS == 1
    static size_t store(char *buf, uint32_t command,
                        Str key, Str val, char tbl,
                        kvtimestamp_t ts) {
        // XXX check alignment on some architectures
        logrec_kv_tbl *lr = reinterpret_cast<logrec_kv_tbl*>(buf);
        lr->command_ = command;
        lr->size_ = sizeof(*lr) + key.len + val.len;
        lr->ts_ = ts;
        lr->keylen_ = key.len;
        lr->tbl_ = tbl;
        memcpy(lr->buf_, key.s, key.len);
        memcpy(lr->buf_ + key.len, val.s, val.len);
        return sizeof(*lr) + key.len + val.len;
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
    // Use logrec_kv_tbl for LOG_RECS == 1 and LOGGING 1!
    static size_t store(char *buf, uint32_t command,
                        Str key, Str val,
                        kvtimestamp_t ts) {
        // XXX check alignment on some architectures
        logrec_kv *lr = reinterpret_cast<logrec_kv *>(buf);
        lr->command_ = command;
        lr->size_ = sizeof(*lr) + key.len + val.len;
        lr->ts_ = ts;
        lr->keylen_ = key.len;
        // TODO: check where is the highest cost of storing.
        memcpy(lr->buf_, key.s, key.len);
        memcpy(lr->buf_ + key.len, val.s, val.len);
        return sizeof(*lr) + key.len + val.len;
    }
    // LOG_RECS == 2
    static size_t store(char *buf, uint32_t command,
                        const void* keyp, uint32_t keylen, const void* valp, uint32_t vallen,
                        kvtimestamp_t ts){
        logrec_kv *lr = reinterpret_cast<logrec_kv *>(buf);
        lr->command_ = command;
        lr->size_ = sizeof(*lr) + keylen + vallen;
        lr->ts_ = ts;
        lr->keylen_ = keylen;
        memcpy(lr->buf_, keyp, keylen);
        memcpy(lr->buf_ + keylen, valp, vallen);
        return sizeof(*lr) + keylen + vallen;
    }
    // LOG_RECS == 3
    static void store_digits(char* buf, uint64_t num, uint32_t& pos){
        uint32_t pos_lcl = 0;
        uint32_t div=1;
        // store digits in reverse order so that to read them easier on log replay! We will simply multiply
        // with 1 and keep multiplying with 10 on every digit we encounter!
        if(num==0){
            buf[pos_lcl++] = 0;
        }
        while((num / div) > 0){
            char digit = (num / div) % 10;
            buf[pos_lcl++] = digit;
            div *= 10;
        }
        pos += pos_lcl;
    }
    // LOG_RECS == 3
    static size_t store_str(char *buf, uint32_t command,
                        Str key, Str val,
                        kvtimestamp_t ts, uint32_t tbl){
        // XXX check alignment on some architectures
        logrec_kv *lr = reinterpret_cast<logrec_kv *>(buf);
        lr->command_ = command;
        lr->ts_ = ts;
        if(tbl==3){ // orders table
            struct order_key {
                uint64_t w_id;
                uint64_t d_id;
                uint64_t o_id;

            };
            struct order_value {
                uint64_t o_c_id;
                uint64_t o_carrier_id;
                uint32_t o_entry_d;
                uint32_t o_ol_cnt;
                uint32_t o_all_local;
            };
            order_key * k = (order_key*) key.s;
            uint32_t pos = 0;
            // store each portion of the key separately, digit by digit
            store_digits(lr->buf_+pos, __bswap_64(k->w_id), pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, __bswap_64(k->d_id), pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, __bswap_64(k->o_id), pos);
            lr->keylen_ = pos;
            lr->buf_[pos++] = '/';
            
            order_value * v = (order_value*) val.s;
            // store each portion of the value separately, digit by digit
            store_digits(lr->buf_+pos, v->o_c_id, pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, v->o_carrier_id, pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, v->o_entry_d, pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, v->o_ol_cnt, pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, v->o_all_local, pos);
            lr->buf_[pos++] = '/';
            
            lr->size_ = sizeof(*lr) + pos;
            return sizeof(*lr) + pos;
        }
        else if (tbl==4){ // orderline table
            class __attribute__((packed)) fix_string {
            public:
                fix_string() {
                    memset(s_, ' ', 24);
                }
                explicit operator std::string() {
                    return std::string(s_, 24);
                }

            private:
                char s_[24];
            };
                
            struct orderline_key {
                uint64_t ol_w_id;
                uint64_t ol_d_id;
                uint64_t ol_o_id;
                uint64_t ol_number;
            };
            struct orderline_value {
                uint64_t       ol_i_id;
                uint64_t       ol_supply_w_id;
                uint32_t       ol_delivery_d;
                uint32_t       ol_quantity;
                int32_t        ol_amount;
                fix_string ol_dist_info;
            };
            orderline_key * k = (orderline_key*) key.s;
            uint32_t pos = 0;
            // store each portion of the key separately, digit by digit
            store_digits(lr->buf_+pos, __bswap_64(k->ol_w_id), pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, __bswap_64(k->ol_d_id), pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, __bswap_64(k->ol_o_id), pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, __bswap_64(k->ol_number), pos);
            lr->keylen_ = pos;
            lr->buf_[pos++] = '/';
            
            orderline_value * v = (orderline_value*) val.s;
            // store each portion of the value separately, digit by digit
            store_digits(lr->buf_+pos, v->ol_i_id, pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, v->ol_supply_w_id, pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, v->ol_delivery_d, pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, v->ol_quantity, pos);
            lr->buf_[pos++] = ',';
            store_digits(lr->buf_+pos, v->ol_amount, pos);
            lr->buf_[pos++] = ',';
            memcpy(lr->buf_+pos, std::string(v->ol_dist_info).c_str(), 24);
            pos+=24;
            lr->buf_[pos++] = '/';
            
            lr->size_ = sizeof(*lr) + pos;
            return sizeof(*lr) + pos;
        }
        return 0;
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
    static size_t store(char *buf, uint32_t command,
                        const void* keyp, uint32_t keylen, const void* valp, uint32_t vallen,
                        kvtimestamp_t prev_ts, kvtimestamp_t ts) {
        // XXX check alignment on some architectures
        logrec_kvdelta *lr = reinterpret_cast<logrec_kvdelta *>(buf);
        lr->command_ = command;
        lr->size_ = sizeof(*lr) + keylen + vallen;
        lr->ts_ = ts;
        lr->prev_ts_ = prev_ts;
        lr->keylen_ = keylen;
        memcpy(lr->buf_, keyp, keylen);
        memcpy(lr->buf_ + keylen, valp, vallen);
        return sizeof(*lr) + keylen + vallen;
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
    for(int i=0; i<N_TBLS; i++) {
        map[i] = logmap_t(1024 * 1024);
        #if MEASURE_MAP_SZ
        size[i]=0;
        #endif
    }
    ti_ = 0;
    logindex_ = logindex;
}

template <int N_TBLS>
loginfo_map<N_TBLS>::~loginfo_map(){

}
template <int N_TBLS>
void loginfo_map<N_TBLS>::record(int command, const loginfo::query_times& qt, Str key, Str value, int tbl){
    auto & m = map[tbl];
    #if STD_MAP
    auto elem = m.find(key);
    if( elem!= m.end()){ // found - check epoch!
        logmap_rec & rec = elem->second;
        if(rec.epoch < qt.epoch){ // new log record has a newer epoch than existing log record! Create a new one and chain it! - we expect to only have up to 2 elements in the chain since the log drainer creates new epoch and once done we can discard the older epochs.
            logmap_rec * newrec = new logmap_rec(command, value, qt.epoch, qt.ts, nullptr);
            rec.next = newrec;
            #if MEASURE_MAP_SZ
            size[tbl] += sizeof(logmap_rec);
            #endif
        }
    }
    else {
        m[key] = logmap_rec(command, value, qt.epoch, qt.ts, nullptr);
        #if MEASURE_MAP_SZ
            size[tbl] += sizeof(logmap_rec);
        #endif
    }
    #elif STO_MAP
    auto * elem = m.nontrans_get(key);
    if (elem != nullptr){ // found - check epoch!
        if(elem->epoch < qt.epoch){
            logmap_rec * newrec = new logmap_rec(command, value, qt.epoch, qt.ts, nullptr);
            elem->next = newrec;
            #if MEASURE_MAP_SZ
            size[tbl] += sizeof(logmap_rec);
            #endif
        }
    }
    else{
        m.nontrans_put_no_lock(key, logmap_rec(command, value, qt.epoch, qt.ts, nullptr));
        #if MEASURE_MAP_SZ
        size[tbl] += sizeof(logmap_rec);
        #endif
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

#if LOG_DRAIN_REQUEST
/*template <int N_TBLS>
inline bool logset_tbl<N_TBLS>::has_logdrain_request(){
    return li_[-1].lsi_.request_log_drain;
}

template <int N_TBLS>
inline void logset_tbl<N_TBLS>::set_logdrain_request(bool req){
    li_[-1].lsi_.request_log_drain = req;
}
*/
#endif

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

    #if LOG_DRAIN_REQUEST
    // initialize mutex and condition variable for this log
    int r = pthread_mutex_init(&mutex, 0);
    always_assert(r>=0, "Error creating mutex!\n"); 
    r = pthread_cond_init(&logdrain_start_cond, 0);
    if (r< 0){
        pthread_mutex_destroy(&mutex);
        always_assert(false, "Error creating condition variable!\n");
    }
    #endif

    // std::cout<<"Total log size: "<< total_len<<std::endl;
    for(int tbl=0; tbl<N_TBLS; tbl++){
        start_[tbl] = pos_[tbl] = tbl==0 ? 0 : (pos_[tbl-1] + len_[tbl-1]);
        //std::cout<<"Table "<< tbl<<": pos_: "<< pos_[tbl]<<", len_: "<< len_[tbl]<<std::endl;
    }
    //std::cout<<"sizeof loginfo_tbl: "<<sizeof(*this)<<std::endl;
    log_epoch_ = 1;
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
    #if LOG_DRAIN_REQUEST
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&logdrain_start_cond);
    #endif
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

// Record log entry by storing the to_str() representation, which converts key/val to char*
// LOG_RECS == 2
template <int N_TBLS>
void loginfo_tbl<N_TBLS>::record(int command, const loginfo::query_times& qtimes, const void* keyp, uint32_t keylen, const void* valp, uint32_t vallen, int tbl){
    size_t n = logrec_kvdelta::size(keylen, vallen)
        + logrec_epoch::size();
    int stalls = 0;
    while (1) {
        if ((start_[tbl]+len_[tbl]) - pos_[tbl] >= n) {
            // Potentially record a new epoch.
            if (qtimes.epoch != log_epoch_) {
                log_epoch_ = qtimes.epoch;
                //std::cout<<"changing log_epoch to: "<< log_epoch_.value()<<std::endl;
                pos_[tbl] += logrec_epoch::store(buf_ + pos_[tbl], logcmd_epoch, qtimes.epoch);
            }
            /*if (command == logcmd_put && qtimes.prev_ts
                && !(qtimes.prev_ts & 1))
                pos_[tbl] += logrec_kvdelta::store(buf_ + pos_[tbl],
                                              logcmd_modify, keyp, keylen, valp, vallen,
                                              qtimes.prev_ts, qtimes.ts);
            else*/
            pos_[tbl] += logrec_kv::store(buf_ + pos_[tbl], command, keyp, keylen, valp, vallen, qtimes.ts);
            assert(command != logcmd_none);
            #if MEASURE_LOG_RECORDS
            incr_cur_log_records();
            #endif
            return;
        }
        if (stalls == 0)
            printf("stall\n");
    }
}

// Record a new epoch entry! This is required when we want to signal the log drainers that we are already at the new epoch and it is safe to drain all records belonging to the previous epoch.
template <int N_TBLS>
void loginfo_tbl<N_TBLS>::record_new_epoch(int tbl, kvepoch_t new_epoch){
    size_t n = logrec_epoch::size() + logrec_wraparound::size();
    if ((start_[tbl]+len_[tbl]) - pos_[tbl] >= n) {
        pos_[tbl] += logrec_epoch::store(buf_ + pos_[tbl], logcmd_epoch, new_epoch);
    }
    else { // wraparound
        std::cout<<"Log wraparound. Make sure to run logdrainers!\n";
        assert(pos_[tbl] + logrec_wraparound::size() <= start_[tbl] + len_[tbl]);
        logrec_wraparound::store(buf_ + pos_[tbl], logcmd_wraparound);
        pos_[tbl] = start_[tbl];
        pos_[tbl] += logrec_epoch::store(buf_ + pos_[tbl], logcmd_epoch, new_epoch);
    }
}


// Record log entry by storing a string representation of key and value
// tbl: The table where the log record belongs to
// LOG_RECS == 1,3
template <int N_TBLS>
uint32_t loginfo_tbl<N_TBLS>::record(int command, const loginfo::query_times& qtimes, Str key, Str value, int tbl, int pos){
    //pos_[tbl];    the position in the buffer where the next log record for this table should be stored
    //start_[tbl];  the position in the buffer where the log records for this table start
    //len_[tbl];    the capacity of the log for this table

    // needed for LOGREC_OVERWRITE
    #if LOGREC_OVERWRITE
    uint32_t logrec_start_pos=pos_[tbl];
    uint32_t log_offset = (pos>=0 ? pos : pos_[tbl] );
    #else
    uint32_t logrec_start_pos=0;
    (void)pos;
    #endif
    //size_t n = logrec_kvdelta::size(key.len, value.len)
    //    + logrec_epoch::size() + logrec_base::size();
    // not needed for in-memory log
    size_t n = logrec_kvdelta::size(key.len, value.len)
        + logrec_epoch::size() + logrec_wraparound::size();
    //int stalls = 0;
    while (1) {
        //TODO: no need for locks since we use one log per thread!
        if ((start_[tbl]+len_[tbl]) - pos_[tbl] >= n) {
            // Potentially record a new epoch.
            if (qtimes.epoch != log_epoch_) {
                log_epoch_ = qtimes.epoch;
                //std::cout<<"changing log_epoch to: "<< log_epoch_.value()<<std::endl;
                pos_[tbl] += logrec_epoch::store(buf_ + pos_[tbl], logcmd_epoch, qtimes.epoch);
                #if LOGREC_OVERWRITE
                // update log pos after we added a new epoch logrec!
                logrec_start_pos = pos_[tbl];
                if(pos<0)
                    log_offset = pos_[tbl];
                #endif
            }
            /*if (command == logcmd_put && qtimes.prev_ts
                && !(qtimes.prev_ts & 1))
                pos_[tbl] += logrec_kvdelta::store(buf_ + pos_[tbl],
                                                logcmd_modify, key, value,
                                                qtimes.prev_ts, qtimes.ts);
            else {*/
            #if LOG_RECS == 1
            #if LOGREC_OVERWRITE
            uint32_t rec_sz = logrec_kv::store(buf_ + log_offset,
                                        command, key, value, qtimes.ts);
            if(pos < 0) // do not advance the pos pointer if we run replace on an existing log record! Log record was updated in-place. pos >=0 means that we want to update in-place
                pos_[tbl] += rec_sz;
            #else
            pos_[tbl] += logrec_kv::store(buf_ + pos_[tbl],
                                        command, key, value, qtimes.ts);
            #endif
            #elif LOG_RECS == 3 // convert key/val to string digit by digit
            if(tbl == 3 || tbl == 4)
                pos_[tbl] += logrec_kv::store_str(buf_ + pos_[tbl],
                                        command, key, value, qtimes.ts, tbl);
            else 
                pos_[tbl] += logrec_kv::store(buf_ + pos_[tbl],
                                        command, key, value, qtimes.ts);
            #endif
            //}
            assert(command != logcmd_none);
            #if MEASURE_LOG_RECORDS
            incr_cur_log_records();
            #endif
            return logrec_start_pos;
        }
        else { // wraparound!
            std::cout<<"Log wraparound. Make sure to run logdrainers!\n";
            assert(pos_[tbl] + logrec_wraparound::size() <= start_[tbl] + len_[tbl]);
            logrec_wraparound::store(buf_ + pos_[tbl], logcmd_wraparound);
            pos_[tbl] = start_[tbl];
        }
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
