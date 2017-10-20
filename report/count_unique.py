#! /usr/bin/python3

import itertools

if __name__ == '__main__':
    with open('crawled_anya.txt', 'r') as f:
        lines_a = f.readlines()

    with open('crawled_boris.txt', 'r') as f:
        lines_b = f.readlines()

    unique = set()
    for line in itertools.chain(lines_a, lines_b):
        unique.add(line)

    print("unique ", len(unique))
    print("all", len(lines_a) + len(lines_b))
