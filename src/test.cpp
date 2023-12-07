

// Goal: Given a min hash value, max hash value, and number of buckets, initialize a hash table with the appropriate number of buckets

#include "../include/semisort.h"
#include <xxhash.h>
#include <parlay/primitives.h>
#include <atomic>
#include <map>
#include <cmath>

int n = records.size();


// allocate light buckets


    // Allocate light key buckets
    long num_light_buckets = 65536/16; //256; // 2^16 -- NOT USED at the moment
    long long bucket_range = 281474976710656 * 256; 
    // long long bucket_range = 281474976710656*64; //72057594037927940; // 2^64/2^16 = 2^48
    parlay::sequence<int> num_light_keys_prefix = parlay::scan_inclusive(light_bitmap);
    parlay::sequence<int> light_bucket_sizes(sample.size());
    parlay::sequence<bool> light_key_range_end_bitmap(sample.size());
    parlay::parallel_for(0, sample.size(), [&] (size_t i) {
        light_key_range_end_bitmap[i] = (i == sample.size()-1 || sample[i]/bucket_range != sample[i+1]/bucket_range);
    });

    parlay::sequence<long> filtered_sample = parlay::pack(sample, light_key_range_end_bitmap);
    parlay::sequence<int> filtered_sizes = parlay::pack(num_light_keys_prefix, light_key_range_end_bitmap);
    
    std::map<long,std::atomic<int>*> light_buckets;
    std::map<long, int> light_bucket_allocated_sizes; 
    
    for (int i = 0; i < filtered_sizes.size(); i++) {
        long bucket_id = filtered_sample[i]/bucket_range;
        int s = (i==0) ? filtered_sizes[i] : filtered_sizes[i]-filtered_sizes[i-1];
        size_t size = (size_t)(alpha * (s + c*log2(n) + sqrt(c*c*log2(n)*log2(n)+2*s*c*log2(n)))*probability);
        light_buckets[bucket_id] = (std::atomic<int>*) malloc((sizeof(std:: atomic<int>)) * size);
        light_bucket_allocated_sizes[bucket_id] = size; 
        std::cout << "ADDED LIGHT BUCKET: " << bucket_id << " WITH SIZE: " << size << std::endl;
    }
    std::cout << "ALLOCATED LIGHT BUCKETS SUCCESFULLY!" << std::endl;
