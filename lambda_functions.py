import boto3
import geonamescache


sns = boto3.client('sns')  # Creates an SNS client using Boto3
TOPIC_ARN = "arn:aws:sns:eu-central-1:445567088654:InvalidCityAlert"



def city_exists(city_name):
    gc = geonamescache.GeonamesCache(min_city_population=15000)
    cities = gc.get_cities()
    return(len(gc.get_cities_by_name(city_name)) > 0)


def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_user = record['dynamodb']['NewImage']
            city = new_user.get('city', {}).get('S', None)
            # check if city field is populated and the city exists
            if city and not city_exists(city):
                sns.publish(
                    TopicArn=TOPIC_ARN,
                    Message=f'🚨 Invalid city detected: {city} for user ID {new_user["user_id"]["S"]}'
                )
    return {
        'statusCode': 200,
        'body': 'Processed DynamoDB stream event.'
    }



sns.publish(
    TopicArn=TOPIC_ARN,
    Message='🚨 Invalid city detected: Atlantis for user ID u009'
)