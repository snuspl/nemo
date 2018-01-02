package edu.snu.onyx.compiler.frontend.spark.transform;

import java.io.Serializable;
import java.util.function.Function;

public interface SerializableFunction<T, R> extends Serializable, Function<T, R> {
}
