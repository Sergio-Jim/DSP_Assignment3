import ballerinax/kafka;
import ballerina/graphql;
import ballerina/docker;

@docker:Expose {}
listener graphql:Listener hdcListener = new(9060);

kafka:ProducerConfiguration producerConfiguration = {
    bootstrapServers: "localhost:9092",
    clientId: "HODProducer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = checkpanic new (producerConfiguration);

@docker:Config {
    name: "hdc",
    tag: "v1.0"
}
service graphql:Service /graphql on hdcListener {

    resource function get evaluateProposal(string studentNumber, string approved) returns string {

        string hghDegComEvaluation = ({studentNumber, approved}).toString();

        checkpanic kafkaProducer->sendProducerRecord({
                topic: "hghDegComEvaluation",
                value: hghDegComEvaluation.toBytes() });

        checkpanic kafkaProducer->flushRecords();
        return "Proposal is being evaluated";
    }
}