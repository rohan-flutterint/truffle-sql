package com.fivetran.truffle;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ComparableOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

class ForgivingComparableOperandTypeChecker extends ComparableOperandTypeChecker {
    public ForgivingComparableOperandTypeChecker(int nOperands,
                                                 RelDataTypeComparability requiredComparability,
                                                 Consistency consistency) {
        super(nOperands, requiredComparability, consistency);
    }

    @Override
    public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
        return checkOperandTypesImpl(
                callBinding,
                throwOnFailure,
                callBinding);
    }

    private boolean checkOperandTypesImpl(
            SqlOperatorBinding operatorBinding,
            boolean throwOnFailure,
            SqlCallBinding callBinding) {
        int nOperandsActual = nOperands;
        if (nOperandsActual == -1) {
            nOperandsActual = operatorBinding.getOperandCount();
        }
        assert !(throwOnFailure && (callBinding == null));
        RelDataType[] types = new RelDataType[nOperandsActual];
        final List<Integer> operandList =
                getOperandList(operatorBinding.getOperandCount());
        for (int i : operandList) {
            /*
            if (operatorBinding.isOperandNull(i, false)) {
                if (throwOnFailure) {
                    throw callBinding.getValidator().newValidationError(
                            callBinding.operand(i), RESOURCE.nullIllegal());
                } else {
                    return false;
                }
            }
            */
            types[i] = operatorBinding.getOperandType(i);
        }
        int prev = -1;
        for (int i : operandList) {
            if (prev >= 0) {
                if (!SqlTypeUtil.isComparable(types[i], types[prev])) {
                    if (!throwOnFailure) {
                        return false;
                    }

                    // REVIEW jvs 5-June-2005: Why don't we use
                    // newValidationSignatureError() here?  It gives more
                    // specific diagnostics.
                    throw callBinding.newValidationError(
                            RESOURCE.needSameTypeParameter());
                }
            }
            prev = i;
        }
        return true;
    }

}
