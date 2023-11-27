#include <parlay/sequence.h>


parlay::sequence<int> parallel_semisort(parlay::sequence<int> records);

parlay::sequence<int> sequential_semisort(parlay::sequence<int> records);
