import ballerina/io;
import ballerina/http;
import ballerinax/kafka;
import ballerina/log;

kafka:ConsumerConfiguration consumerConfiguration = {

    bootstrapServers: "localhost:9092",

    groupId: "sp-group",
    offsetReset: "earliest",

    topics: ["studentApplication", "studentProposal", "studentThesis"]

};

kafka:Consumer consumer = checkpanic new (consumerConfiguration);
http:Client clientEndpoint = check new ("http://localhost:9080");

map<json> applicants = {};
map<json> proposals = {};

public function main() {
    while(true){
        io:println("        Supervisor Menu           ");
        io:println("1. Select Appicant ID \n"
        + "2. Review Proposal \n"
        + "3. Approve Thesis \n");

        string choice = io:readln("Enter choice available below: ");
        int choose = checkpanic int:fromString(choice);

        if(choose == 1){
            getMessages("studentApplication");
            io:println(applicants);

            string applicant = io:readln("Choose Student: ");
            string supervisor = io:readln("You ID: ");
            var  response = clientEndpoint->post("/graphql",{ query: " { selectApplicant(studentNumber: "+ applicant + ", supervisorID: "+ supervisor +") }" });
            if (response is  http:Response) {
                var jsonResponse = response.getJsonPayload();

                if (jsonResponse is json) {
                    
                    io:println(jsonResponse);
                } else {
                    io:println("Invalid payload received:", jsonResponse.message());
                }
            }
        }

        if(choose == 2){
            extractProposal("studentProposal");
            io:println(proposals);

            string applicant = io:readln("Choose Student: ");
            string approved = io:readln("approved: ");
            var  response = clientEndpoint->post("/graphql",{ query: " { reviewProposal(studentNumber: "+ applicant + ", proposalApproved: \""+ approved +"\") }" });
            if (response is  http:Response) {
                var jsonResponse = response.getJsonPayload();

                if (jsonResponse is json) {
                    
                    io:println(jsonResponse);
                } else {
                    io:println("Invalid payload received:", jsonResponse.message());
                }
            }
        }
    }
}

function getMessages(string topic){
    kafka:ConsumerRecord[] records = checkpanic consumer->poll(1000);

    foreach var kafkaRecord in records {
        if(kafkaRecord.offset.partition.topic == topic){
            byte[] messageContent = kafkaRecord.value;
            string|error message = string:fromBytes(messageContent);

            if (message is string) {
                json|error jsonContent = message.fromJsonString();

                if(jsonContent is json){
                    json|error stN = jsonContent.studentNumber;
                    json|error nm = jsonContent.name;
                    json|error crs = jsonContent.course;
                    json|error app = jsonContent.application;

                    if(stN is json && nm is json && crs is json && app is json){
                        int|error studentNumber = int:fromString(stN.toString());
                        string|error name = nm.toString();
                        string|error course = crs.toString();
                        string|error application = app.toString();

                        if(studentNumber is int && name is string && course is string && application is string){
                            applicants[studentNumber.toString()] = {studentNumber, name, course, application};
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
                    json|error prop = jsonContent.proposal;

                    if(stN is json && prop is json){
                        int|error studentNumber = int:fromString(stN.toString());
                        string|error proposal = prop.toString();

                        if(studentNumber is int && proposal is string ){
                            proposals[studentNumber.toString()] = {studentNumber, proposal};
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