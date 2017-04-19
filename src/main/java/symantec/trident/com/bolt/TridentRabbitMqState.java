package symantec.trident.com.bolt;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.symantec.storm.metrics.statsd.StatsdMetricConsumer;
import com.symantec.storm.metrics.statsd.Util;

import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.RabbitMQProducer;
import io.latent.storm.rabbitmq.config.ProducerConfig;
import symantec.storm.rabbitmq.util.RABBITMQCONSTANTS;

public class TridentRabbitMqState implements State {
  private static final Logger LOG = LoggerFactory.getLogger(TridentRabbitMqState.class);



  private TridentTupleToMessage scheme = null;
  private Declarator declarator = null;

  private transient Logger logger;
  private transient RabbitMQProducer producer;
  private transient ProducerConfig sinkConfig;


  @SuppressWarnings("rawtypes")
  private TridentTupleToRabbitMqMapper mapper;
  private RabbitMqTopicSelector topicSelector;

  @SuppressWarnings("rawtypes")
  private transient Map rabbitMqConfig;
  private StatsdMetricConsumer statsdConsumer = null;
  private String baseMetricName;

  public TridentRabbitMqState withTridentTupleToRabbitMqMapper(
      @SuppressWarnings("rawtypes") TridentTupleToRabbitMqMapper mapper) {
    LOG.info("Setting Mapper " + mapper.toString());
    this.mapper = mapper;

    return this;
  }

  public TridentRabbitMqState withRabbitMqTopicSelector(RabbitMqTopicSelector selector) {
    LOG.info("Setting selector " + selector.toString());
    this.topicSelector = selector;
    return this;
  }

  public TridentRabbitMqState withRabbitMqDeclarator(Declarator declarator) {
    LOG.info("Setting Declarator " + declarator.toString());
    this.declarator = declarator;
    return this;
  }

  public TridentRabbitMqState withRabbitMqScheme(TridentTupleToMessage scheme) {
    LOG.info("Setting State " + scheme.toString());
    this.scheme = scheme;
    return this;
  }

  public TridentRabbitMqState withSinkConfig(ProducerConfig sinkConfig) {
    LOG.info("Setting SinkConfig " + sinkConfig.toString());
    this.sinkConfig = sinkConfig;
    return this;
  }

  public TridentRabbitMqState withStatsD(@SuppressWarnings("rawtypes") Map rabbitMqConfig,
      TopologyContext context) {
    this.rabbitMqConfig = rabbitMqConfig;
    LOG.info("Setting SinkConfig " + rabbitMqConfig.toString());



    // assign specific TaskId
    baseMetricName =
        Util.clean( context.getTaskData(RABBITMQCONSTANTS.STATSD) + "." + Util.getLocalIpAddress() + "."
            + context.getThisWorkerPort());

    // assign specific TaskId

    return this;
  }

  @Override
  public void beginCommit(Long txid) {
    LOG.debug("beginCommit is Noop.");
  }

  @Override
  public void commit(Long txid) {
    LOG.debug("commit is Noop.");
  }


  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf) {
    if (mapper == null) {
      LOG.error("Mapper is Null" + mapper);
    } else {
      LOG.info(mapper.toString());
    }
    if (stormConf == null) {
      LOG.error("StormCOnf is null" + stormConf);
    } else {
      LOG.info(stormConf.toString());
    }
    if (topicSelector == null) {
      LOG.error("topicSelector is null" + topicSelector);
    } else {
      LOG.info(topicSelector.toString());
    }



    producer = new RabbitMQProducer(declarator);
    producer.open(stormConf);
    logger = LoggerFactory.getLogger(this.getClass());
    this.scheme.prepare(this.sinkConfig.asMap());

    if (rabbitMqConfig != null && rabbitMqConfig.containsKey(RABBITMQCONSTANTS.STATSD)) {


      // statsd Prepare
      statsdConsumer = new StatsdMetricConsumer();
      statsdConsumer.prepare(null, rabbitMqConfig, null, null);
      logger.info("Successfully prepared StatsD");
    }
    logger.info("Successfully prepared RabbitMQBolt");

  }

  public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
    if (rabbitMqConfig != null && rabbitMqConfig.containsKey(RABBITMQCONSTANTS.STATSD)) {
      updateStateWithStatsD(tuples, collector);
    } else {
      updateStateWithoutStatsD(tuples, collector);
    }
  }

  public void updateStateWithoutStatsD(List<TridentTuple> tuples, TridentCollector collector) {


    String queue = null;
    for (TridentTuple tuple : tuples) {
      try {
        publish(tuple);
      } catch (Exception ex) {
        String errorMsg = "Could not send message with key = " + mapper.getKeyFromTuple(tuple)
            + " to topic = " + queue;
        LOG.warn(errorMsg, ex);
        throw new FailedException(errorMsg, ex);
      }


      // tuples are always acked, even when transformation by scheme yields Message.NONE as
      // if it failed once it's unlikely to succeed when re-attempted (i.e.
      // serialization/deserilization errors).

    }

  }

  public void updateStateWithStatsD(List<TridentTuple> tuples, TridentCollector collector) {


    String queue = null;
    for (TridentTuple tuple : tuples) {
      statsdConsumer.incrementCounter(baseMetricName + RABBITMQCONSTANTS.ATTEMPT);
      try {
        publish(tuple);
      } catch (Exception ex) {
        statsdConsumer.incrementCounter(baseMetricName + RABBITMQCONSTANTS.FAIL);
        String errorMsg = "Could not send message with key = " + mapper.getKeyFromTuple(tuple)
            + " to topic = " + queue;
        LOG.warn(errorMsg, ex);
        throw new FailedException(errorMsg, ex);
      }
      statsdConsumer.incrementCounter(baseMetricName + RABBITMQCONSTANTS.SUCCESS);


      // tuples are always acked, even when transformation by scheme yields Message.NONE as
      // if it failed once it's unlikely to succeed when re-attempted (i.e.
      // serialization/deserilization errors).

    }



  }

  private void publish(TridentTuple tuple) {
    producer.send(scheme.produceMessage(tuple));

  }



}
