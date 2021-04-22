#!/usr/bin/env bash

# This script needs to be run once in order for compile time macros to not
# complain about a missing DB

# We can trust that our program will initialize the db at runtime the same way
# as it pulls from the same file for initialization

sqlite3 cache/metadata.sqlite < db_queries/init.sql