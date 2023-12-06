#include "../include/semisort.h"
#include <xxhash.h>
#include <parlay/primitives.h>
#include <atomic>
#include <map>
#include <cmath>

parlay::sequence<int> parallel_semisort(parlay::sequence<int> records) {
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
        long hash = (XXH3_64bits_withSeed(&records[i], sizeof(long), seed));
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
        key_sizes[i] = (i == sample.size()-1 || sample[i] != sample[i+1]) ? i+1 : 0;
        light_bitmap[i] = key_sizes[i] <= heavy_threshold ? 1 : 0;
    });

    parlay::sequence<int> offsets = filter(key_sizes, [&](int i) {return key_sizes[i] != 0;});
    std::map<int,std::atomic<int>*> heavy_keys;
    for (int i = 0; i < offsets.size(); i++)
        if ((i == 0 && offsets[i] > heavy_threshold) || (i != 0 && offsets[i]-offsets[i-1] > heavy_threshold))
            heavy_keys[sample[offsets[i]]] = nullptr;

    // Allocate heavy key buckets
    int i = 0;
    double alpha = 2;//1.1;
    double c = 2;//0.8664;
    for(auto key : heavy_keys) {
        int s = (i==0) ? offsets[i] : offsets[i]-offsets[i-1];
        size_t size = (size_t)(alpha * (s + c*log2(records.size()) + sqrt(c*c*log2(records.size())*log2(records.size())+2*s*c*log2(records.size())))*probability);
        heavy_keys[key.first] = (std::atomic<int>*) malloc((sizeof(std:: atomic<int>))*size);
        i++;
    }
    std::cout << "ALLOCATED HEAVY BUCKETS SUCCESFULLY!" << std::endl;

    // Allocate light key buckets
    long num_light_buckets = 65536; //256; // 2^16
    long long bucket_range = 281474976710656; //72057594037927940; // 2^64/2^16 = 2^48
    parlay::sequence<int> num_light_keys_prefix = parlay::scan_inclusive(light_bitmap);
    parlay::sequence<int> light_bucket_sizes(sample.size());
    parlay::sequence<bool> light_key_range_end_bitmap(sample.size());
    parlay::parallel_for(0, sample.size(), [&] (size_t i) {
        light_key_range_end_bitmap[i] = (i == sample.size()-1 || sample[i]%bucket_range != sample[i+1]%bucket_range);
    });
    parlay::sequence<long> filtered_sample = parlay::pack(sample, light_key_range_end_bitmap);
    parlay::sequence<int> filtered_sizes = parlay::pack(num_light_keys_prefix, light_key_range_end_bitmap);
    std::map<int,std::atomic<int>*> light_buckets;
    for (int i = 0; i < filtered_sizes.size(); i++) {
        long bucket_id = filtered_sample[i]/bucket_range;
        int s = (i==0) ? filtered_sizes[i] : filtered_sizes[i]-filtered_sizes[i-1];
        size_t size = (size_t)(alpha * (s + c*log2(n) + sqrt(c*c*log2(n)*log2(n)+2*s*c*log2(n)))*probability);
        std::cout << "Allocating light bucket with key: " << bucket_id << " size: " << size << std::endl;
        light_buckets[bucket_id] = (std::atomic<int>*) malloc((sizeof(std:: atomic<int>))*size);
    }
    std::cout << "ALLOCATED LIGHT BUCKETS SUCCESFULLY!" << std::endl;
    
    // Parallel loop through all original records and insert into appropriate heavy array or light bucket with CAS
    int expected = 0;
    parlay::parallel_for(0, records.size(), [&] (size_t i) {
        if(heavy_keys.find(records[i]) != heavy_keys.end()) {
            int k = 0;
            while(!heavy_keys[hashed_keys[i]][k].compare_exchange_strong(expected, records[i])) k++;
        } else {
            int k = 0;
            while(!light_buckets[hashed_keys[i]/bucket_range][k].compare_exchange_strong(expected, records[i])) k++;
        }
    });
    
    // Semisort all light buckets (we will just sort)

    return records;
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
