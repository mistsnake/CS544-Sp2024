#!/bin/bash

# Download zipfile
wget https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip

# Extract csv file
unzip hdma-wi-2021.zip 

# Count the number of lines containing 'Multifamily'
grep -c "Multifamily" hdma-wi-2021.csv 

# Remove files at the end, to allow for multiple times of running
rm hdma-wi-2021.zip hdma-wi-2021.csv
