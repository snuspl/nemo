package edu.snu.vortex.runtime.common.channel;


import edu.snu.vortex.runtime.executor.DataTransferManager;

/**
 * A local channel writer that stores data records in the local memory.
 * It does not serialize the records and passes them over to the respective {@link LocalChannelReader}
 * via {@link DataTransferManager}.
 * @param <T> The type of data records.
 */
public class LocalChannelWriter<T> implements ChannelWriter<T> {
  private final String channelId;
  private final String srcTaskId;
  private String dstTaskId;
  private final ChannelType channelType;
  private final ChannelMode channelMode;
  private DataTransferManager dataTransferManager;

  public LocalChannelWriter(final String channelId, final String srcTaskId, final String dstTaskId) {
    this.channelId = channelId;
    this.srcTaskId = srcTaskId;
    this.dstTaskId = dstTaskId;
    this.channelType = ChannelType.LOCAL;
    this.channelMode = ChannelMode.OUTPUT;
  }

  @Override
  public void write(final Iterable<T> data) {

  }

  @Override
  public void commit() {

  }

  @Override
  public String getId() {
    return channelId;
  }

  @Override
  public ChannelType getType() {
    return channelType;
  }

  @Override
  public ChannelMode getMode() {
    return channelMode;
  }

  @Override
  public String getSrcTaskId() {
    return srcTaskId;
  }

  @Override
  public String getDstTaskId() {
    return dstTaskId;
  }

  @Override
  public void setDstTaskId(final String newDstTaskId) {
    dstTaskId = newDstTaskId;
  }

  @Override
  public void initialize(final ChannelConfig config) {
    dataTransferManager = config.getDataTransferManager();
  }
}
