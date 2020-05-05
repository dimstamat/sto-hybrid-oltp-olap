#pragma once

#define MEASURE_LATENCIES 0

 #if MEASURE_LATENCIES > 0
     #define INIT_COUNTING struct timespec start_time, end_time;
     #define START_COUNTING clock_gettime(CLOCK_MONOTONIC, &start_time);
 #else
     #define INIT_COUNTING {}
     #define START_COUNTING {}
 #endif
 
#if MEASURE_LATENCIES > 0
      #define STOP_COUNTING(array_sum, tid) { clock_gettime(CLOCK_MONOTONIC, &end_time); \
                               array_sum[tid][0] += ((end_time.tv_sec > start_time.tv_sec ? (1e9-start_time.tv_nsec + end_time.tv_nsec ) : end_time.tv_nsec - start_time.tv_nsec) /     1000.0) ; \
                               array_sum[tid][1] += 1;}
#endif

#if MEASURE_LATENCIES == 1
     #define STOP_COUNTING_RAW(array_sum, array_all, tid) STOP_COUNTING(array_sum, tid)
#elif MEASURE_LATENCIES == 2
     #define STOP_COUNTING_RAW(array_sum, array_all, tid) { clock_gettime(CLOCK_MONOTONIC, &end_time); \
                            double duration_ns = ((end_time.tv_sec > start_time.tv_sec ? (1e9-start_time.tv_nsec + end_time.tv_nsec ) : end_time.tv_nsec - start_time.tv_nsec));\
                            array_sum[tid][0] += duration_ns / 1000.0 ; \
                            array_all[tid][((int)array_sum[tid][1])] = duration_ns;\
                            array_sum[tid][1] +=1;}
#else
    #define STOP_COUNTING(array_sum, tid) {}
    #define STOP_COUNTING_RAW(array_sum, array_all, tid) {}
#endif

