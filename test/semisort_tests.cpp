#include <gtest/gtest.h>
#include "../include/semisort.h"


TEST(EulerTourTreeSuite, stress_test) {
    parlay::sequence<int> input(10);
    semisort(input);
}
