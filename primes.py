#!/usr/bin/env python3

import concurrent.futures
import math

PRIMES = [
    421_132_112_272_535_095_293,
    421_132_115_797_848_077_099,
    421_132_115_797_848_077_099,
    421_132_115_797_848_077_099,
    421_132_115_797_848_077_099,
    421_132_115_797_848_077_099,
    421_132_115_797_848_077_099,
    1_099_726_899_285_419]

def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False

    sqrt_n = int(math.floor(math.sqrt(n)))
    for i in range(3, sqrt_n + 1, 2):
        if n % i == 0:
            return False
    return True

def main():
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for number, prime in zip(PRIMES, executor.map(is_prime, PRIMES)):
            print('%d is prime: %s' % (number, prime))

if __name__ == '__main__':
    main()
