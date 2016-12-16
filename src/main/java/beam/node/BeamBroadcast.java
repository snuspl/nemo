package beam.node;

import dag.node.Broadcast;
import org.apache.beam.sdk.values.PCollectionView;

public class BeamBroadcast<I, O> extends Broadcast<I, O, PCollectionView> {
  private final PCollectionView view;

  public BeamBroadcast(final PCollectionView view) {
    this.view = view;
  }

  @Override
  public PCollectionView getTag() {
    return view;
  }
}
