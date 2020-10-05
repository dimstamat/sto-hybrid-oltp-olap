#pragma once

#include "SystemProfiler.hh"
#include "Transaction.hh"
#include "DB_params.hh"

namespace bench {

class db_profiler {
public:
    using constants = db_params::constants;
    explicit db_profiler(bool spawn_perf)
            : spawn_perf_(spawn_perf), perf_pid_(),
              start_tsc_(), end_tsc_() {}

    void start(Profiler::perf_mode mode) {
        if (spawn_perf_)
            perf_pid_ = Profiler::spawn("perf", mode);
        start_tsc_ = read_tsc();
    }

    uint64_t start_timestamp() const {
        return start_tsc_;
    }


    void finish(size_t num_txns) {
        end_tsc_ = read_tsc();
        if (spawn_perf_) {
            bool ok = Profiler::stop(perf_pid_);
            always_assert(ok, "killing profiler");
        }
        // print elapsed time
        uint64_t elapsed_tsc = end_tsc_ - start_tsc_;
        double elapsed_time = (double) elapsed_tsc / constants::million / constants::processor_tsc_frequency;
        std::cout << "Elapsed time: " << elapsed_tsc << " ticks" << std::endl;
        std::cout << "Real time: " << elapsed_time << " ms" << std::endl;
        std::cout << "Throughput: " << (double) num_txns / (elapsed_time / 1000.0) << " txns/sec" << std::endl;
        // print STO stats
        Transaction::print_stats();
    }

    // we don't want to count the time for TPC-C! TPC-H runs 1 sec less than TPC-C, so we need to know when TPC-H ended!
    void finish_tpch(size_t num_queries, uint64_t end, uint32_t db_size) {
        end_tsc_ = end;
        if (spawn_perf_) {
            bool ok = Profiler::stop(perf_pid_);
            always_assert(ok, "killing profiler");
        }
        // print elapsed time
        uint64_t elapsed_tsc = end_tsc_ - start_tsc_;
        double elapsed_time = (double) elapsed_tsc / constants::million / constants::processor_tsc_frequency;
        std::cout << "Elapsed time: " << elapsed_tsc << " ticks" << std::endl;
        std::cout << "Real time: " << elapsed_time << " ms" << std::endl;
        std::cout << "Throughput (TPCH): " << (double) num_queries / (elapsed_time / 1000.0) << " q/sec @"+ std::to_string(db_size/1024/1024) + "MB" << std::endl;
    }

private:
    bool spawn_perf_;
    pid_t perf_pid_;
    uint64_t start_tsc_;
    uint64_t end_tsc_;
};


class log_replay_profiler {
    public:
    using constants = db_params::constants;
    void start(){
        start_tsc_ = read_tsc();
    }

    uint64_t start_timestamp() const {
        return start_tsc_;
    }

    void finish(float log_sz, size_t log_recs, size_t logical_recs) {
        end_tsc_ = read_tsc();
        // print elapsed time
        uint64_t elapsed_tsc = end_tsc_ - start_tsc_;
        double elapsed_time = (double) elapsed_tsc / constants::million / constants::processor_tsc_frequency;
        std::cout << "Elapsed time: " << elapsed_tsc << " ticks" << std::endl;
        std::cout << "Real time: " << elapsed_time << " ms" << std::endl;
        std::cout << "Log size: "<< log_sz<<", Log records: " << log_recs<<std::endl;
        std::cout<< "Log size read throughput (MB/sec): "<< ((float)log_sz / (elapsed_time/1000)) << " MB/s\n";
        std::cout<< "Log rec read throughput (recs/sec): "<< (float) (log_recs / (elapsed_time/1000) )/1000000 <<" M recs/s\n";
        std::cout<< "Log logical rec read throughput (recs/sec): "<< (float) (logical_recs / (elapsed_time/1000) )/1000000 <<" M recs/s\n";
    }


    private:
    uint64_t start_tsc_;
    uint64_t end_tsc_;
};

}; // namespace bench