package com.fivetran.truffle;

/**
 * CompileRel uses this interface to compile row sources like TableScan
 */
interface CompileRowSource {
    RowSource compile();
}
