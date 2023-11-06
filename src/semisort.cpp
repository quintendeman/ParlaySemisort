#include "../include/semisort.h"


parlay::sequence<int> semisort(parlay::sequence<int> records) {
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
