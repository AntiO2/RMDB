#ifndef RANDOM_H
#define RANDOM_H

#include <ctime>
#include <cstdlib>

class RandomGenerator {
public:
    static void init() { srand(1219); }
    static int generate_random_int(int min, int max);
    static float generate_random_float(int min, int max);
    static void generate_random_str(char* str, int len);
    static void generate_random_numer_str(char* str, int len);
    static void generate_random_varchar(char* str, int min_len, int max_len);
    static void generate_randome_address(char* street_1, char* street_2, char* city, char* state, char* zip);
    static int NURand(int A, int x, int y);
    static void generate_random_lastname(int num, char* name);
};

#endif