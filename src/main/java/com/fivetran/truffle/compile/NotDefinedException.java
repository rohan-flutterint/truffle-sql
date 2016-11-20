package com.fivetran.truffle.compile;

/**
 * Thrown when an ExprColumn tries to read a value that isn't defined because definitionLevel < maxDefinitionLevel
 */
class NotDefinedException extends RuntimeException {
}
