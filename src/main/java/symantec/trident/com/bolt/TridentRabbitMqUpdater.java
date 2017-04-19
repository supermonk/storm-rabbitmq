package symantec.trident.com.bolt;

import java.util.List;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

public class TridentRabbitMqUpdater extends BaseStateUpdater<TridentRabbitMqState> {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public void updateState(TridentRabbitMqState state, List<TridentTuple> tuples, TridentCollector collector) {
      state.updateState(tuples, collector);
  }
}
