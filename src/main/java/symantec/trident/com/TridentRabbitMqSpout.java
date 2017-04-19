package symantec.trident.com;

import java.util.Map;

import org.apache.storm.spout.Scheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.MessageScheme;
import symantec.storm.rabbitmq.util.RABBITMQCONSTANTS;

@SuppressWarnings("rawtypes")
public class TridentRabbitMqSpout implements ITridentSpout<RabbitMQBatch> {

 
  private static final long serialVersionUID = 1L;

  private String name;
  private static int nameIndex = 1;
  private String streamId;
  private Map consumerMap;
  private Declarator declarator;
  private MessageScheme scheme;

  public TridentRabbitMqSpout(Scheme scheme, String streamId,Map consumerMap){
    this(MessageScheme.Builder.from(scheme), null,new Declarator.NoOp(), streamId,consumerMap);
  }

  @SuppressWarnings("unchecked")
  public TridentRabbitMqSpout(MessageScheme scheme, final TopologyContext context,
      Declarator declarator, String streamId, Map consumerMap) {
    this.scheme = scheme;
    this.declarator = declarator;
    this.streamId = streamId;
    this.consumerMap = consumerMap;
    this.name = "RabbitMQSpout" + (nameIndex++);
    this.consumerMap.put(RABBITMQCONSTANTS.SPOUT_NAME, this.name);
    
  }

  @Override
  public Map getComponentConfiguration() {
    return consumerMap;
  }

//  @Override
//  public storm.trident.spout.ITridentSpout.BatchCoordinator<RabbitMQBatch> getCoordinator(String txStateId,
//      Map conf, TopologyContext context) {
//    return new RabbitMQBatchCoordinator(name);
//  }
//
//  @Override
//  public storm.trident.spout.ITridentSpout.Emitter<RabbitMQBatch> getEmitter(String txStateId,
//  Map conf, TopologyContext context) {
//    // separate Emitter for StatsD
//    if(consumerMap!=null && consumerMap.containsKey(RABBITMQCONSTANTS.STATSD)){
//      context.setTaskData(RABBITMQCONSTANTS.STATSD, this.name);
//      return new RabbitMQTridentEmitterWithStatsD(scheme, context, declarator, streamId, consumerMap);
//    }
//    return new RabbitMQTridentEmitter(scheme, context, declarator, streamId, consumerMap);
//  }

  @Override
  public Fields getOutputFields() {
    return this.scheme.getOutputFields();
  }

  @Override
  public org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator<RabbitMQBatch> getCoordinator(
      String txStateId, Map conf, TopologyContext context) {
    return new RabbitMQBatchCoordinator(name);
  }

  @Override
  public org.apache.storm.trident.spout.ITridentSpout.Emitter<RabbitMQBatch> getEmitter(
      String txStateId, Map conf, TopologyContext context) {
 // separate Emitter for StatsD
  if(consumerMap!=null && consumerMap.containsKey(RABBITMQCONSTANTS.STATSD)){
    context.setTaskData(RABBITMQCONSTANTS.STATSD, this.name);
    return new RabbitMQTridentEmitterWithStatsD(scheme, context, declarator, streamId, consumerMap);
  }
  return new RabbitMQTridentEmitter(scheme, context, declarator, streamId, consumerMap);
  }

}
