package dag;

public interface Operator<I, O> {
  Iterable<O> compute(Iterable<I> input);
}
