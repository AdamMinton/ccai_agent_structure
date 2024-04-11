# Get DFCX Agent Structure in BQ Tables

This project allows you to extract the structure of a Dialogflow CX (DFCX) agent and store it in BigQuery tables.

## Prerequisites

- Google Cloud SDK (gcloud)
- Access to a Google Cloud project with Pub/Sub, Cloud Scheduler, and BigQuery enabled
- Service Account with roles of BigQuery Data Editor, BigQuery Job User, Dialogflow API Reader

## Usage

To trigger the extraction process for a specific agent, publish a message to the `agent-structure-topic` with the agent's ID. For example, for the golden chat stable in `att-ccai-chat-dev`:

## BQ Output

**Dataset:** agent_structure

**Tables:**

* entity_types
* flows
* intents
* pages
* parameters
* training_phrases
* transition_route_groups
* transition_routes
* webhooks

**Schemas:**

**entity_types**

| Field Name     | Data Type    | Mode        |
|----------------|--------------|-------------|
| date           | DATETIME     | NULLABLE    |
| agentId        | STRING       | NULLABLE    |
| agentName      | STRING       | NULLABLE    |
| entityTypeId   | STRING       | NULLABLE    |
| entityTypeName | STRING       | NULLABLE    |
| entity         | STRING       | NULLABLE    |
| synonym        | STRING       | NULLABLE    |

**flows**

| Field Name | Data Type | Mode        |
|------------|-----------|-------------|
| date       | DATETIME  | NULLABLE    |
| agentId    | STRING    | NULLABLE    |
| agentName  | STRING    | NULLABLE    |
| flowId     | STRING    | NULLABLE    |
| flowName   | STRING    | NULLABLE    |
| pages      | STRING    | REPEATED    |

**intents** 

| Field Name     | Data Type    | Mode        |
|----------------|--------------|-------------|
| date           | DATETIME     | NULLABLE    |
| agentId        | STRING       | NULLABLE    |
| agentName      | STRING       | NULLABLE    |
| intentId       | STRING       | NULLABLE    |
| intentName     | STRING       | NULLABLE    |
| description    | STRING       | NULLABLE    |
| parameters     | STRING       | REPEATED    |
| labels         | STRING       | REPEATED    |

**pages** 

| Field Name      | Data Type    | Mode         |
|-----------------|--------------|--------------|
| date            | DATETIME     | NULLABLE     |
| agentId         | STRING       | NULLABLE     |
| agentName       | STRING       | NULLABLE     |
| flowId          | STRING       | NULLABLE     |
| flowName        | STRING       | NULLABLE     |
| pageId          | STRING       | NULLABLE     |
| pageName        | STRING       | NULLABLE     |
| webhookId       | STRING       | NULLABLE     |
| webhookName     | STRING       | NULLABLE     |
| webhookTag      | STRING       | NULLABLE     |
| fulfillment     | STRING       | REPEATED     |
| partialResponse | BOOLEAN      | NULLABLE     |
| parameterPresets| STRING       | REPEATED     |
| parameters      | STRING       | REPEATED     |
| routes          | STRING       | REPEATED     |
| routeGroups     | STRING       | REPEATED     |


## Local Development

Encode your string that would be part of your Cloud Scheduler message.
```
echo -n '[{"agent_id":"agent_id","agent_location":"agent_location","agent_project_id":"agent_project_id","bq_project_id":"bq_project_id"}]' | base64
```

Run the Functions Framework 
```
functions-framework --target main --signature-type=event
```

Run the curl command to mimic a pub/sub notifcation
```
curl localhost:8080 \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
        "context": {
          "eventId":"1144231683168617",
          "timestamp":"2020-05-06T07:33:34.556Z",
          "eventType":"google.pubsub.topic.publish",
          "resource":{
            "service":"pubsub.googleapis.com",
            "name":"projects/sample-project/topics/gcf-test",
            "type":"type.googleapis.com/google.pubsub.v1.PubsubMessage"
          }
        },
        "data": {
          "@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
          "data": "base_64_encoded_string"
        }
      }'
```