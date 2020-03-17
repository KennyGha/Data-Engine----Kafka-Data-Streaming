import json
import logging

import requests
import time
import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"
#KSQL_URL = "http://0.0.0.0:8088" #kenny added

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    kafka_topic = 'org.chicago.cta.station.turnstile.v1',
    value_format = 'avro',
    key = 'station_id'
);
CREATE TABLE turnstile_summary
WITH (value_format = 'json') AS
    SELECT station_id, COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        print('Topic Existed...............................')
        time.sleep(0.5)
        return
    else :
        print('Not Existed................................')


    logging.debug("executing ksql statement...")


    # TODO: Complete the following KSQL Statement
    # Directions: Use KSQL to combine the Stations Topic and the Turnstile Topic
    time.sleep(0.5)
    print('Line  52' + str(KSQL_URL))

    resp = requests.post(

        #f"{KSQL_URL}/ksql",
        str(KSQL_URL)+'/ksql',
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )
    print('Line  64')
    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


def contains_substring(to_test, substr):
    _before, match, _after = to_test.partition(substr)
    return len(match) > 0

def topic_pattern_match(pattern):
    """
        Takes a string `pattern`
        Returns `True` if one or more topic names contains substring `pattern`.
        Returns `False` if not.
    """
    topic_metadata = client.list_topics()
    topics = topic_metadata.topics
    filtered_topics = {key: value for key, value in topics.items() if contains_substring(key, pattern)}
    return len(filtered_topics) > 0


if __name__ == "__main__":
    execute_statement()
