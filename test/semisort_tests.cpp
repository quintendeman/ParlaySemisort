#include <gtest/gtest.h>
#include <random>
#include "../include/semisort.h"


bool semisorted(parlay::sequence<int> records) {
    int last = records[0];
    std::set<int> records_seen;
    for (int i = 0; i < records.size(); i++) {
        if (records[i] != last) {
            if (records_seen.find(records[i]) != records_seen.end())
                return false;
            last = records[i];
            records_seen.insert(records[i]);
        }
    }
    return true;
}

TEST(SemisortSuite, parallel_correctness_test) {
    // Test parameters
    int input_size = 100;
    int min_value = 0;
    int max_value = 10;

    // Generate a random sequence of ints and call the semisort function
    parlay::sequence<int> input(input_size);
    for (int i = 0; i < input.size(); i++)
        input[i] = min_value+(rand()%(max_value-min_value));
    parlay::sequence<int> output = parallel_semisort(input);

    // Check that the results are in semisorted order
    std::cout << "[";
    for (auto record : output)
        std::cout << record << ",";
    std::cout << "]" << std::endl;
    if (!semisorted(output))
        FAIL();
}

TEST(SemisortSuite, sequential_correctness_test) {
    // Test parameters
    int input_size = 100;
    int min_value = 0;
    int max_value = 10;

    // Generate a random sequence of ints and call the semisort function
    parlay::sequence<int> input(input_size);
    for (int i = 0; i < input.size(); i++)
        input[i] = min_value+(rand()%(max_value-min_value));
    parlay::sequence<int> output = sequential_semisort(input);

    // Check that the results are in semisorted order
    std::cout << "[";
    for (auto record : output)
        std::cout << record << ",";
    std::cout << "]" << std::endl;
    if (!semisorted(output))
        FAIL();
}
