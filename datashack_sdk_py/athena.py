import time
import boto3
from urllib.parse import quote_plus
from sqlalchemy.engine import create_engine

def get_var_char_values(d):
    return [obj['VarCharValue'] for obj in d['Data']]


def query_results(params, wait=True):
    client = boto3.client("athena")

    ## This function executes the query and returns the query execution ID
    response_query_execution_id = client.start_query_execution(
        QueryString=params['query'],
        QueryExecutionContext={
            'Database': params["ctx_db"]
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )

    if not wait:
        return response_query_execution_id['QueryExecutionId']
    else:
        response_get_query_details = client.get_query_execution(
            QueryExecutionId=response_query_execution_id['QueryExecutionId']
        )
        status = 'RUNNING'
        iterations = 360  # 30 mins

        while (iterations > 0):
            iterations = iterations - 1
            response_get_query_details = client.get_query_execution(
                QueryExecutionId=response_query_execution_id['QueryExecutionId']
            )
            status = response_get_query_details['QueryExecution']['Status']['State']

            if (status == 'FAILED') or (status == 'CANCELLED'):
                failure_reason = response_get_query_details['QueryExecution']['Status']['StateChangeReason']
                print(failure_reason)
                return False, False

            elif status == 'SUCCEEDED':
                location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

                ## Function to get output results
                response_query_result = client.get_query_results(
                    QueryExecutionId=response_query_execution_id['QueryExecutionId']
                )
                result_data = response_query_result['ResultSet']

                if len(response_query_result['ResultSet']['Rows']) > 1:
                    header = response_query_result['ResultSet']['Rows'][0]
                    rows = response_query_result['ResultSet']['Rows'][1:]

                    header = [obj['VarCharValue'] for obj in header['Data']]
                    result = [dict(zip(header, get_var_char_values(row))) for row in rows]

                    return location, result
                else:
                    return location, None
        else:
            time.sleep(5)

        return False


def get_sqlalchemy_engine(database_name, region, s3_bucket_path, access_key=None, secret_access_key=None):
    conn_str = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/" \
               "{schema_name}?s3_staging_dir={s3_staging_dir}"
    engine = create_engine(conn_str.format(
        aws_access_key_id=quote_plus(access_key),
        aws_secret_access_key=quote_plus(secret_access_key),
        region_name=region,
        schema_name=database_name,
        s3_staging_dir=quote_plus(s3_bucket_path)))
    return engine
