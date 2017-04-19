package symantec.trident.com.bolt;

import java.util.Map;

import org.apache.storm.trident.tuple.TridentTuple;

import io.latent.storm.rabbitmq.config.ProducerConfig;

public abstract class TridentTupleToMessageNonDynamic extends TridentTupleToMessage
{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String exchangeName;
  private String routingKey;
  private String contentType;
  private String contentEncoding;
  private boolean persistent;

  @SuppressWarnings("unchecked")
  @Override
  protected void prepare(@SuppressWarnings("rawtypes") Map stormConfig)
  {
    ProducerConfig producerConfig = ProducerConfig.getFromStormConfig(stormConfig);
    exchangeName = producerConfig.getExchangeName();
    routingKey = producerConfig.getRoutingKey();
    contentType = producerConfig.getContentType();
    contentEncoding = producerConfig.getContentEncoding();
    persistent = producerConfig.isPersistent();
  }

  @Override
  protected String determineExchangeName(TridentTuple input)
  {
    return exchangeName;
  }

  @Override
  protected String determineRoutingKey(TridentTuple input)
  {
    return routingKey;
  }

  @Override
  protected String specifyContentType(TridentTuple input)
  {
    return contentType;
  }

  @Override
  protected String specifyContentEncoding(TridentTuple input)
  {
    return contentEncoding;
  }

  @Override
  protected boolean specifyMessagePersistence(TridentTuple input)
  {
    return persistent;
  }
}
