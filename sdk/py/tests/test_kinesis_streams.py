import os
import boto3
import pytest
from datetime import datetime
from unittest import mock
from jsonschema.exceptions import ValidationError
from moto import mock_kinesis
from datashack_glue_sdk import StreamingTable
from datashack_glue_sdk.resource_schemas import Column


def get_test_kinesis_stream():
    client = boto3.client("kinesis", region_name="eu-west-2")
    client.create_stream(StreamName="test_stream", ShardCount=1)
    return {"client": client, "stream": client.describe_stream(StreamName="test_stream")["StreamDescription"]}


@mock_kinesis
@mock.patch("datashack_glue_sdk.StreamingTable.get_runtime_context")
@mock.patch("datashack_glue_sdk.StreamingTable.get_kinesis_client")
def test_insert_to_kinesis_happy_flow(table_kinesis_client, rtc_mock):
    kinesis_stream = get_test_kinesis_stream()
    table_kinesis_client.return_value = kinesis_stream.get("client")
    rtc_mock.return_value = {'kinesis_name': kinesis_stream.get("stream").get('StreamName')}

    os.environ['DATASHACK_SERVER_URL'] = "http://localhost:8000"
    os.environ['DATASHACK_ENV'] = "test"

    Table = StreamingTable(
        "test_table",
        "test_db"
    )
    Table['id'] = Column('string')
    Table['age'] = Column('int')
    Table['ts_date'] = Column('date')

    fake_data = {"id": "abcd1234", "age": 123, "ts_date": datetime.now().date().isoformat()}

    res = Table.insert(fake_data)

    assert res['ResponseMetadata']['HTTPStatusCode'] == 200
    assert res['SequenceNumber'] == '1'


@mock_kinesis
@mock.patch("datashack_glue_sdk.StreamingTable.get_runtime_context")
@mock.patch("datashack_glue_sdk.StreamingTable.get_kinesis_client")
def test_incorrect_schema(table_kinesis_client, rtc_mock):
    kinesis_stream = get_test_kinesis_stream()
    table_kinesis_client.return_value = kinesis_stream.get("client")
    rtc_mock.return_value = {'kinesis_name': kinesis_stream.get("stream").get('StreamName')}

    os.environ['DATASHACK_SERVER_URL'] = "http://localhost:8000"
    os.environ['DATASHACK_ENV'] = "test"

    Table = StreamingTable(
        "test_table",
        "test_db"
    )
    Table['id'] = Column('string')
    Table['age'] = Column('int')
    Table['ts_date'] = Column('date')

    with pytest.raises(ValidationError) as exc:
        # missing columns test
        res = Table.insert({"id": "abcd"})
    with pytest.raises(ValidationError) as exc:
        # type error age is string
        res = Table.insert({"id": "abcd", "age": "123", "ts_date": datetime.now().date().isoformat()})
    with pytest.raises(ValidationError) as exc:
        # wrong columns test (additionalProperty)
        res = Table.insert({"id1": "abcd", "age": "123", "ts_date": datetime.now().date().isoformat()})
    with pytest.raises(ValidationError) as exc:
        # extra columns test (additionalProperty)
        res = Table.insert({"extra_col":"123","id": "abcd", "age": "123", "ts_date": datetime.now().date().isoformat()})