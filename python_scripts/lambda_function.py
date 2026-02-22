import json
from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

BOOTSTRAP = "b-2.uberkafkacluster.9wk1ik.c9.kafka.us-east-1.amazonaws.com:9098,b-1.uberkafkacluster.9wk1ik.c9.kafka.us-east-1.amazonaws.com:9098,b-3.uberkafkacluster.9wk1ik.c9.kafka.us-east-1.amazonaws.com:9098"
TOPIC = "uber-topic"
REGION = "us-east-1"


class TokenProvider:
    def token(self):
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token, expiry_ms


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="AWS_MSK_IAM",
    sasl_oauth_token_provider=TokenProvider(),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def lambda_handler(event, context):
    try:
        body = event.get("body", "{}")
        data = json.loads(body) if isinstance(body, str) else body

        producer.send(TOPIC, value=data)
        producer.flush()

        return {"statusCode": 200, "body": "Message sent"}

    except Exception as e:
        return {"statusCode": 500, "body": str(e)}