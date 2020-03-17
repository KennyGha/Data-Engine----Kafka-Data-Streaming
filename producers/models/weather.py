# -*- coding: utf-8 -*
"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse
#from urlparse import urlparse
from pathlib import Path
import requests

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://rest-proxy:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        # TODO: Complete the below by deciding on a topic name
        # , number of partitions, and number of replicas ✅
        super().__init__(
            "org.chicago.cta.weather.v1", # TODO: Come up with a better topic name ✅
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=1,
            num_replicas=1
        )
        filelocation=os.path.dirname(__file__)
        self.status = Weather.status.sunny
        self.temp = 30.0
        if month in Weather.winter_months:
            self.temp = 3.0
        elif month in Weather.summer_months:
            self.temp = 20.0

        if Weather.key_schema is None:
            with open(filelocation+"/schemas/weather_key.json") as f:
            #with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        # TODO: Define this value schema in `schemas/weather_value.json ✅
        if Weather.value_schema is None:
            with open(filelocation+"/schemas/weather_value.json") as f:
            #with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)
        # TODO: Complete the function by posting a weather event to REST Proxy. Make sure to ✅
        # specify the Avro schemas and verify that you are using the correct Content-Type header. ✅

        resp = requests.post(
            # TODO: What URL should be POSTed to? ✅
            #f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            str(Weather.rest_proxy_url)+'/topics/'+str(self.topic_name),
            # TODO: What Headers need to bet set? ✅
            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            data=json.dumps({
               # TODO: Provide key schema, value schema, and records ✅
               "key_schema": json.dumps(Weather.key_schema),
                "value_schema": json.dumps(Weather.value_schema),
                "records": [{
                    "key": {"timestamp": self.time_millis()},
                    "value": { "temperature": self.temp, "status": self.status.name }
                }]
            })
        )
        resp.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
