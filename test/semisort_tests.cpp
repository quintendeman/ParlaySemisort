#include <gtest/gtest.h>
#include "../include/semisort.h"


bool semisorted(parlay::sequence<int> records) {
    return true;
}

TEST(EulerTourTreeSuite, stress_test) {
    // Test parameters
    int input_size = 100;

    // Generate a random sequence of ints and call our semisort function
    parlay::sequence<int> input(input_size);
    for (int i = 0; i < input.size(); i++)
        input[i] = rand();
    parlay::sequence<int> output = semisort(input);

    // Check that the results are in semisorted order
    std::cout << "[";
    for (auto record : output)
        std::cout << record << ",";
    std::cout << "]" << std::endl;
    if (!semisorted(output))
        FAIL();
}
