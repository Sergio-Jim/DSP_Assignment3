import ballerinax/kafka;
import ballerina/io;
import ballerina/log;

service graphql:Service /graphql on new graphql:Listener(9090) {

    resource function get selectApplicant(string name, int voterID) returns string {
        
        string message = "Hello World, Ballerina";

        check kafkaProducer->sendProducerRecord({
            topic: "studentApplication",
            value: message.toBytes() });

        check kafkaProducer->flushRecords();
        return "Registered candidate, " + name;
    }
}