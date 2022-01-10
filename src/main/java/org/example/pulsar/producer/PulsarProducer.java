package org.example.pulsar.producer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class PulsarProducer {
  public static void main (String[] args) throws PulsarClientException {
    PulsarClient client = PulsarClient. builder()
      .serviceUrl("pulsar://localhost:6650")
      .build();
    final Producer<String> producer = client.newProducer(Schema.STRING).topic("test").create();
    for (int i = 0; i < 1000; i++) {
      producer.send("TEST" + i);
      System.out.println("sent TEST" + i);
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException ignored) {
      }
    }
    producer.close();
    client.close();
  }

}
