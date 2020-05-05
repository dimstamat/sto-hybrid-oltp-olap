#pragma once

namespace tpcc {
    template <typename DBParams>
    class tpcc_db;
    class tpcc_input_generator;
    template <typename DBParams>
    class tpcc_access;
}


namespace tpch {

template <typename DBParams>
class tpch_runner {
public:
    static constexpr bool Commute = DBParams::Commute;
    enum class query_type : int {
        Q1=1,  Q2,  Q3,  Q4,  Q5,  Q6,
        Q7,    Q8,  Q9,  Q10, Q11, Q12,
        Q13,   Q14, Q15, Q16, Q17, Q18,
        Q19,   Q20, Q21, Q22
    };

    tpch_runner(int id, tpcc::tpcc_input_generator& ig, tpcc::tpcc_db<DBParams>& database, int mix)
        : ig(ig), db(database), mix(mix), runner_id(id) {}

    // finds the next query to be run for this thread (round-robin) and runs it
    void run_next_query();

    void run_query4();
    
private:
    tpcc::tpcc_input_generator &ig;
    tpcc::tpcc_db<DBParams>& db;
    int mix;
    int runner_id;

    friend class tpcc::tpcc_access<DBParams>;
};


};