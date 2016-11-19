package com.fivetran.truffle;

/**
 * Thrown when an ExprColumn tries to read a value that isn't defined because definitionLevel < maxDefinitionLevel
 */
public class NotDefinedException extends RuntimeException {
}
