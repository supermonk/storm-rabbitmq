package symantec.trident.com.bolt;


import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.config.ProducerConfig;
import symantec.storm.rabbitmq.util.RABBITMQCONSTANTS;

public class TridentRabbitMqStateFactory implements StateFactory {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(TridentRabbitMqStateFactory.class);

  @SuppressWarnings("rawtypes")
  private TridentTupleToRabbitMqMapper mapper;
  private RabbitMqTopicSelector topicSelector;

  private ProducerConfig sinkConfig = null;

  private TridentTupleToMessage scheme = null;
  private Declarator declarator = null;

  @SuppressWarnings("rawtypes")
  private Map internalconf = null;

  private String name = null;



  public TridentRabbitMqStateFactory withTridentTupleToRabbitMqMapper(
      @SuppressWarnings("rawtypes") TridentTupleToRabbitMqMapper mapper) {
    this.mapper = mapper;
    return this;
  }

  public TridentRabbitMqStateFactory withRabbitMqTopicSelector(RabbitMqTopicSelector selector) {
    this.topicSelector = selector;
    return this;
  }


  public TridentRabbitMqStateFactory withRabbitMqDeclarator(Declarator declarator) {
    this.declarator = declarator;
    return this;
  }

  public TridentRabbitMqStateFactory withRabbitMqScheme(TridentTupleToMessage scheme) {
    this.scheme = scheme;
    return this;
  }

  public TridentRabbitMqStateFactory withSinkConfig(ProducerConfig sinkConfig) {
    this.sinkConfig = sinkConfig;
    return this;
  }

  public TridentRabbitMqStateFactory withInternalConf(
      @SuppressWarnings("rawtypes") Map internalConf) {
    this.internalconf = internalConf;
    return this;
  }



  @SuppressWarnings("rawtypes")
  @Override
  public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
    LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);

    TopologyContext context = (TopologyContext) metrics;

    this.name = "RabbitMQBolt";

    Map rabbitMqConfig = null;

    if (this.internalconf != null) {
      LOG.info("Loading from Internal Conf");
      rabbitMqConfig = internalconf;
    } else {
      rabbitMqConfig = conf;
    }

    TridentRabbitMqState state =
        new TridentRabbitMqState().withRabbitMqTopicSelector(this.topicSelector)
            .withTridentTupleToRabbitMqMapper(this.mapper).withRabbitMqDeclarator(this.declarator)
            .withRabbitMqScheme(this.scheme).withSinkConfig(this.sinkConfig);
    // additional step for statsD
    if (rabbitMqConfig != null && rabbitMqConfig.containsKey(RABBITMQCONSTANTS.STATSD)) {
      context.setTaskData(RABBITMQCONSTANTS.STATSD, this.name);
      state.withStatsD(rabbitMqConfig, context);

    } 
    state.prepare(rabbitMqConfig);
    return state; 

  }



}
