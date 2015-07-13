#How to

<ol>
  <li>Run 'mvn clean install'</li>
  <li>Run "Consumer" 'java -cp target/kafkatry-1.0-SNAPSHOT-jar-with-dependencies.jar com.zanox.DemoConsumer'</li>
  <li>Run "Producer" 'java -cp target/kafkatry-1.0-SNAPSHOT-jar-with-dependencies.jar com.zanox.DemoProducer'</li>
  <li>Have fun with groovy console! Send messages with command like: producer.sendNotification('magda','ruslan', 'ahoj')</li>
  <li>Profit!</li>
</ol>
