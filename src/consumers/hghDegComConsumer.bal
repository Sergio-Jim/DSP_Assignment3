import ballerina/io;
import ballerina/http;
import ballerinax/kafka;
import ballerina/log;

kafka:ConsumerConfiguration consumerConfiguration = {

    bootstrapServers: "localhost:9092",

    groupId: "hdc-group",
    offsetReset: "earliest",

    topics: ["proposalSanction", "facExtExmThesisAssesment"]
};

kafka:Consumer consumer = checkpanic new (consumerConfiguration);
http:Client clientEndpoint = check new ("http://localhost:9060");

map<json> sanctionedProposals = {};

public function main() {
    while(true){
        io:println("        Higher Degree Committee         ");

        extractProposal("proposalSanction");
        io:println(sanctionedProposals);

        string applicant = io:readln("studentNumber: ");
        string approved = io:readln("approved: ");

        var  response = clientEndpoint->post("/graphql",{ query: " { evaluateProposal(studentNumber: \""+ applicant +"\", approved: \"" + approved +"\") }" });
        if (response is  http:Response) {
            var jsonResponse = response.getJsonPayload();

            if (jsonResponse is json) {
                
                io:println(jsonResponse);
            } else {
                io:println("Invalid payload:", jsonResponse.message());
            }
        }
    }
}

function extractProposal(string topic){
    kafka:ConsumerRecord[] records = checkpanic consumer->poll(1000);

    foreach var kafkaRecord in records {
        if(kafkaRecord.offset.partition.topic == topic){
            byte[] messageContent = kafkaRecord.value;
            string|error message = string:fromBytes(messageContent);

            if (message is string) {
                json|error jsonContent = message.fromJsonString();

                if(jsonContent is json){
                    json|error stN = jsonContent.studentNumber;
                    json|error appr = jsonContent.approved;

                    if(stN is json && appr is json){
                        int|error studentNumber = int:fromString(stN.toString());
                        string|error approved = appr.toString();

                        if(studentNumber is int && approved is string ){
                            sanctionedProposals[studentNumber.toString()] = {studentNumber, approved};
                        }
                    }
                }
            } else {
                log:printError("Error occurred while converting message data",
                    err = message);
            }
        }
    }
}
