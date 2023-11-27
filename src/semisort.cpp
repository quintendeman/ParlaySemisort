#include "../include/semisort.h"
#include <xxhash.h>
#include <parlay/primitives.h>


parlay::sequence<int> parallel_semisort(parlay::sequence<int> records) {
    long seed = time(0);
    int n = records.size();
    parlay::random_generator generator(seed);
    int probability = log2(n);
    std::uniform_int_distribution<int> random(0, probability-1);
    // Hash all of the records into the range n^3
    parlay::sequence<int> hashed_keys(n);
    parlay::sequence<bool> pack_table(n);
    parlay::parallel_for(0, hashed_keys.size(), [&] (size_t i) {
        int hash = (XXH3_64bits_withSeed(&records[i], sizeof(int), seed))%(n*n*n);
        hashed_keys[i] = hash;
        auto gen = generator[i];
        if (random(gen) == 0) pack_table[i] = true;
    });
    // Take a random sample of the hashed keys with p=1/log(n) and sort it
    parlay::sequence<int> sample = pack(hashed_keys, pack_table);
    sort_inplace(sample);
    // Partition the sample into heavy and light keys
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
