import ballerinax/kafka;
import ballerina/graphql;
import ballerina/docker;

@docker:Expose {}
listener graphql:Listener fieListener = new(9050);

kafka:ProducerConfiguration producerConfiguration = {
    bootstrapServers: "localhost:9092",
    clientId: "HODProducer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = checkpanic new (producerConfiguration);

@docker:Config {
    name: "fie",
    tag: "v1.0"
}
service graphql:Service /graphql on fieListener {

    resource function get proposalSanction(string studentNumber, string approved) returns string {
        
        string fieAssign = ({studentNumber, approved}).toString();

        checkpanic kafkaProducer->sendProducerRecord({
                                    topic: "proposalSanction",
                                    value: fieAssign.toBytes() });

        checkpanic kafkaProducer->flushRecords();
        return "Sanctioned Proposal";
    }
}