# Experimental SQL compiler

Compiles Calcite query plans to Truffle/Graal programs.
The focus is on emitting efficient specialized Truffle programs, not on distributed query execution.

## Installation

* Download Graal VM Development Kit from
  http://www.oracle.com/technetwork/oracle-labs/program-languages/downloads
* Unpack the downloaded `graalvm_*.tar.gz` into `truffle-sql/graalvm`.
* Verify that the file `truffle-sql/bin/java` exists and is executable

## Run using graal

You can run GraalSuite tests with GraalVM using ./graal_tests.sh