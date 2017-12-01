#! /bin/bash

echo "processing.."
./processer.py
echo "indexing.."
./indexer.py
echo "building bm25.."
./bm25_calculator.py
echo "searching.."
./search_engine.py