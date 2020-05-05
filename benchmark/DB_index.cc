#include "DB_index.hh"

volatile mrcu_epoch_type active_epoch = 1;
volatile uint64_t globalepoch = 1;
volatile bool recovering = false;

// Dimos - in order to use log in STO/Silo
kvtimestamp_t initial_timestamp;