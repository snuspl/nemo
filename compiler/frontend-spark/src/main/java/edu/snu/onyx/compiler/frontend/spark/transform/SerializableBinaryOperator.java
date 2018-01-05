package edu.snu.onyx.compiler.frontend.spark.transform;

import java.io.Serializable;
import java.util.function.BinaryOperator;

/**
 * Serializable Binary Operator.
 * @param <T> type of the element.
 */
public interface SerializableBinaryOperator<T> extends BinaryOperator<T>, Serializable {
}
