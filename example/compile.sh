javac -d bin -cp example:libs/* -Xlint:deprecation example/*.java

java -cp bin:bin/*:libs/* ProducerTest
java -cp bin:bin/*:libs/* ConsumerTest
