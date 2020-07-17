import boto3

client = boto3.client('ecr')

response = client.describe_repositories(
    # registryId='string',
    # nextToken='string',
    maxResults=1000
)


for item in response['repositories']:
    print(item['registryId'])
    print(item['repositoryName'])
    response = client.delete_repository(
        registryId=item['registryId'],
        repositoryName=item['repositoryName'],
        force=True
    )
