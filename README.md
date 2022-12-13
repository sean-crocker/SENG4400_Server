Part One - The Server
==================

Server application to publish to a publish and subscribe queue using Kafka.

## Requirements

* [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Maven](https://maven.apache.org/download.cgi) (at least 3.5)
* [Kafka 3.2.0](https://kafka.apache.org/downloads)

## Setup

1.  Under the Kafka installation directory, create a folder called data.
2.  In the data folder, create two more folders: kafka and zookeeper.
3.  In the config folder and edit the zookeeper.properties file.
    Set the dataDir path to the full absolute path of that new data\zookeeper
    folder you just created. In the same folder, edit the server.properties file
    Set the log.dirs path to the full absolute path to that new data\kafka folder.
4.  Start the Zookeeper using the `bin\windows\zookeeper-server-start.bat` command.
    You need to pass in a single parameter which is the path to the zookeeper.properties file.
5.  Open a new command prompt and start the Kafka server using the
    `bin\windows\kafka-server-start.bat` command. Pass in a single parameter which is the
    path to the server.properties file.
6.  Open a new command prompt, execute
    `bin\windows\kafka-topics.bat --zookeeper localhost:2181 --topic seng4400 --create
    --replication-factor 1 --partitions 1`.

## Compile

From the project directory (folder containing `pom.xml`), run the following commands:

    mvn clean install

## Run

If you want to run the Server with default parameters (`delay=1000` and `max=1000000`) run the command:

    mvn exec:java -Dexec.mainClass=com.seng4400.Server

If you want to run the Server with only a given delay then run the command:

    mvn exec:java -Dexec.mainClass=com.seng4400.Server -Dexec.args="DELAY_IN_MS"

If you want to run the Server with both a delay and maximum range given then run the command:

    mvn exec:java -Dexec.mainClass=com.seng4400.Server -Dexec.args="DELAY_IN_MS MAX"
