package com.fivetran.truffle;

import org.apache.calcite.sql.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SQL operator table that searches a sequence of operator tables.
 * The first table that contains an operator shadows all the rest.
 */
public class ShadowOperatorTable implements SqlOperatorTable {
    private final SqlOperatorTable[] delegates;

    public ShadowOperatorTable(SqlOperatorTable... delegates) {
        this.delegates = delegates;
    }

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName,
                                        SqlFunctionCategory category,
                                        SqlSyntax syntax,
                                        List<SqlOperator> operatorList) {
        int initialSize = operatorList.size();

        for (SqlOperatorTable each : delegates) {
            each.lookupOperatorOverloads(opName, category, syntax, operatorList);

            // Once you find a match, stop
            if (operatorList.size() > initialSize)
                return;
        }
    }

    @Override
    public List<SqlOperator> getOperatorList() {
        // Operator names that have been added and should be skipped
        Set<String> skip = new HashSet<>();
        List<SqlOperator> result = new ArrayList<>();

        for (SqlOperatorTable each : delegates) {
            // Operator names that have been seen in this delegate
            Set<String> seen = new HashSet<>();

            for (SqlOperator op : each.getOperatorList()) {
                // If we haven't seen the operator in an earlier *delegate*
                if (!skip.contains(op.getName())) {
                    result.add(op);
                    seen.add(op.getName()); // add to the list of operator we've seen in this delegate
                }
            }

            // Skip this operator in future delegates
            skip.addAll(seen);
        }

        return result;
    }
}
