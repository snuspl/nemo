package edu.snu.onyx.compiler.frontend.spark.transform;

import java.io.Serializable;
import java.util.function.BinaryOperator;

public interface SerializableBinaryOperator<T> extends BinaryOperator<T>, Serializable {
}
