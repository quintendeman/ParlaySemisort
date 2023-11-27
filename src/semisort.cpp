#include "../include/semisort.h"
#include <xxhash.h>
#include <parlay/primitives.h>


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
    parlay::parallel_for(0, sample.size(), [&] (size_t i) {
        key_sizes[i] = (i == sample.size()-1 || sample[i] != sample[i+1]) ? i+1 : 0;
    });
    parlay::sequence<int> offsets = filter(key_sizes, [&](int i) {return key_sizes[i] != 0;});
    std::unordered_map<int,int*> heavy_keys;
    for (int i = 0; i < offsets.size(); i++)
        if ((i == 0 && offsets[i] > heavy_threshold) || (i != 0 && offsets[i]-offsets[i-1] > heavy_threshold))
            heavy_keys[sample[offsets[i]]] = nullptr;
    // Semisort heavy keys

    // Semisort light keys

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
