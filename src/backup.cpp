#include "../include/semisort.h"
#include <xxhash.h>
#include <parlay/primitives.h>
#include <atomic>
#include <map>
#include <cmath>

parlay::sequence<int> parallel_semisort(parlay::sequence<int> records) {
    
    // break semisort.cpp:94

    long seed = time(0);
    int n = records.size();
    
    parlay::random_generator generator(seed);
    int probability = log2(n);
    int heavy_threshold = log2(n);
    std::uniform_int_distribution<int> random(0, probability-1);

    // Hash all of the records into the range n^3
    parlay::sequence<long> hashed_keys(n);
    parlay::sequence<bool> pack_table(n);

    parlay::parallel_for(0, hashed_keys.size(), [&] (size_t i) {
        long hash = (XXH3_64bits_withSeed(&records[i], sizeof(int), seed));
        hashed_keys[i] = hash;
        auto gen = generator[i];
        if (random(gen) == 0) pack_table[i] = true;
    });

    // Take a random sample of the hashed keys with p=1/log(n) and sort it
    parlay::sequence<long> sample = pack(hashed_keys, pack_table);
    sort_inplace(sample);

    // Partition the sample into heavy and light keys
    parlay::sequence<int> key_sizes(sample.size());
    parlay::sequence<int> light_bitmap(sample.size());
    parlay::parallel_for(0, sample.size(), [&] (size_t i) {
        // key sizes stores the starting index of each key in the sample
        // std::cout << "KEY SIZES: " << i << " " << sample[i] << " " << sample[i-1] << std::endl;
        key_sizes[i] = (i == 0 || sample[i] != sample[i-1]) ? i : 0;
        light_bitmap[i] = key_sizes[i] <= heavy_threshold ? 1 : 0;
    });

    parlay::sequence<int> offsets = filter(key_sizes, [&](int i) {return key_sizes[i] != 0;});

    /** HANDLE HEAVY BUCKETS **/

    std::map<long,std::atomic<int>*> heavy_keys;
    for (int i = 0; i < offsets.size(); i++) { 
        int s = (i==0) ? offsets[i] : offsets[i]-offsets[i-1];
        if (s > heavy_threshold) { 
            // std::cout << "ADDED HEAVY KEY: " << " HASH=" << sample[offsets[i]-1] << " SIZE=" << s << std::endl; 
            std::cout << "ADDED HEAVY KEY: " << i << " " << offsets[i] << " " << offsets[i-1] << " WITH SIZE: " << s << std::endl; 
            heavy_keys[sample[offsets[i] - 1]] = nullptr;
        }
    } 

    // Allocate heavy key buckets
    int i = 0;
    double alpha = 2;//1.1;
    double c = 2;//0.8664;
    std::map<long, int> heavy_bucket_allocated_sizes; 
    for(auto key : heavy_keys) {
        int s = (i==0) ? offsets[i] : offsets[i]-offsets[i-1];
        size_t size = (size_t)(alpha * (s + c*log2(records.size()) + sqrt(c*c*log2(records.size())*log2(records.size())+2*s*c*log2(records.size())))*probability);
        heavy_keys[key.first] = (std::atomic<int>*) malloc((sizeof(std:: atomic<int>))*size);
        heavy_bucket_allocated_sizes[key.first] = size; 
        i++;
        // std::cout << "ALLOCATING HEAVY BUCKETS..." << std::endl;
    }
    std::cout << "ALLOCATED HEAVY BUCKETS SUCCESFULLY!" << std::endl;

    /**  HANDLE LIGHT BUCKETS  **/

    long num_buckets = 65536;  // n / (log2(n) * log2(n)); // O(n/log^2(n)) buckets
    long min_hash = -72057594037927940*64; // -9223372036854775808; // -2^63
    long max_hash = 72057594037927940*64; // 9223372036854775807; // 2^63 - 1
    long bucket_range = max_hash / num_buckets * 2; // range of each bucket

    std::cout << "PART 1: NUM BUCKETS: " << num_buckets << std::endl;

    std::map<long, std::atomic<int>*> light_buckets; 
    std::map<long, int> light_bucket_allocated_sizes;

    // Initialize light buckets
    parlay::parallel_for(0, num_buckets, [&] (size_t i) {
        long bucket_id = i + min_hash / bucket_range; 
        light_buckets[bucket_id] = nullptr;
        light_bucket_allocated_sizes[bucket_id] = 0; 
    });

    std::cout << "PART 2: Initialized: " << num_buckets << std::endl;

    // count number of keys from the sample that fall into each bucket
    parlay::sequence<long> filtered_light_sample = parlay::pack(sample, light_bitmap);
    std::map<long, int> light_bucket_counts;
    parlay::parallel_for(0, filtered_light_sample.size(), [&] (size_t i) {
        long bucket_id = filtered_light_sample[i]/bucket_range; // bucket_id is between 0 and num_buckets-1
        std::cout << "BUCKET ID: " << bucket_id << " WITH FILTERED LIGHT SAMPLE: " << filtered_light_sample[i] << std::endl;
        light_bucket_counts[bucket_id]++;
    });

    // allocate light buckets - if there are no keys in the bucket, then we allocate a bucket of size heavy_threshold
    for (int i = 0; i < num_buckets; i++) { 
        long bucket_id = (min_hash + i*bucket_range) / bucket_range;
        if (light_bucket_counts[bucket_id] == 0) { 
            light_buckets[bucket_id] = (std::atomic<int>*) malloc((sizeof(std:: atomic<int>)) * heavy_threshold);
            light_bucket_allocated_sizes[bucket_id] = heavy_threshold; 
        } else { 
            size_t size = (size_t)(alpha * (light_bucket_counts[i] + c*log2(records.size()) + sqrt(c*c*log2(records.size())*log2(records.size())+2*light_bucket_counts[i]*c*log2(records.size())))*probability);
            light_buckets[bucket_id] = (std::atomic<int>*) malloc((sizeof(std:: atomic<int>)) * size);
            light_bucket_allocated_sizes[bucket_id] = size; 
        }
    }

    std::cout << "ALLOCATED LIGHT BUCKETS SUCCESFULLY!" << std::endl;

    /** INSERT INTO BUCKETS  **/

    // Parallel loop through all original records and insert into appropriate heavy array or light bucket with CAS
    int expected = 0;
    // parlay::parallel_for(0, n, [&] (size_t i) {
    for (int i = 0; i < n; i++) {
        if(heavy_keys.find(hashed_keys[i]) != heavy_keys.end()) {
            int k = 0;
            while(!heavy_keys[hashed_keys[i]][k].compare_exchange_strong(expected, records[i])) k++;
            std::cout << "ADDED TO HEAVY BUCKET: " << hashed_keys[i] << " WITH VALUE: " << records[i] << std::endl;
        } else {
            int k = 0;
            long bucket_id = hashed_keys[i]/bucket_range;
            std::cout << "TRYING TO ADD TO LIGHT BUCKET: " << bucket_id << " WITH VALUE: " << records[i] << std::endl;
            while(!light_buckets[bucket_id][k].compare_exchange_strong(expected, records[i])) k++;
        }
    }
    // });

    std::cout << "ADDED VALUES TO LIGHT AND HEAVY BUCKETS!" << std::endl; 

    /** SEMISORT BUCKETS AND COMBINE  **/

    // convert sequence of atomic ints to ints for light bucket range 
    std::vector<parlay::sequence<int>> full_buckets_range;
    
    for (auto key: light_buckets) {
        parlay::sequence<int> bucket(light_bucket_allocated_sizes[key.first]);
        parlay::parallel_for(0, bucket.size(), [&] (size_t i) {
            bucket[i] = (int)light_buckets[key.first][i].load(std::memory_order_relaxed);
        });
        bucket = parlay::filter(bucket, [&](int i) {return bucket[i] == 0;}); 
        full_buckets_range.push_back(bucket);
    }
    
    std::cout << "CONVERTED LIGHT KEYS!" << std::endl;

    // Semisort all light buckets (we will just sort)
    parlay::parallel_for(0, full_buckets_range.size(), [&] (size_t i) { 
        parlay::sort_inplace(full_buckets_range[i]); 
    });

    std::cout << "SORTED LIGHT KEYS!" << std::endl;

    // convert sequence of atomic ints to ints for heavy bucket range (this is to make sure that we 
    // are able to combine the heavy key sequences with the light key sequences)   
    for (auto key: heavy_keys) { 
        // separator for each bucket (-------------------)
        // std::cout << "---------- NEW BUCKET " << key.first << " ---------" << std::endl;
        parlay::sequence<int> bucket((heavy_bucket_allocated_sizes[key.first]));
        parlay::parallel_for(0, bucket.size(), [&] (size_t i) { 
            // std::cout << "COPYING: " << heavy_keys[key.first][i] << std::endl;
            bucket[i] = (int) heavy_keys[key.first][i].load(std::memory_order_relaxed);
        });
        if (bucket.size() > 0) { 
            // std::cout << "VALUE: " << bucket[0] << std::endl; 
            full_buckets_range.push_back(bucket);
        }  
    }

    std::cout << "HEAVY KEYS ADDED" << std::endl; 

    // parlay flatten - takes a list of parlay sequences and then combine them 
    parlay::sequence<int> semisorted_records = parlay::flatten(full_buckets_range); 

    // filter out all 0s - i can't get this to work with parlay::filter  
    parlay::sequence<bool> semisorted_bitmap(semisorted_records.size());
    parlay::parallel_for(0, semisorted_records.size(), [&] (size_t i) {
        semisorted_bitmap[i] = (semisorted_records[i] != 0); // filter out all 0s
    });

    semisorted_records = parlay::pack(semisorted_records, semisorted_bitmap); // pack the semisorted records

    // print out the semisorted records
    std::cout << "SEMI-SORTED RECORDS: " << std::endl;
    for (int i = 0; i < semisorted_records.size(); i++) { 
        std::cout << semisorted_records[i] << " "; 
    }
    std::cout << std::endl;

    return semisorted_records;
}

parlay::sequence<int> sequential_semisort(parlay::sequence<int> records) {
    // Hash the n elements and get their counts
    std::unordered_map<int,int> counts;
    for (auto record : records) {
        if (counts.find(record) != counts.end())
            counts[record] += 1;
        else
            counts[record] = 1;
    }
    // Add them to a new sequence in semisorted order
    parlay::sequence<int> semisorted_records(records.size());
    int index = 0;
    for (auto entry : counts)
        for (int i = 0; i < entry.second; i++)
            semisorted_records[index++] = entry.first;
    return semisorted_records;
}