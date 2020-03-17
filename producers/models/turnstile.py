"""Creates a turnstile data producer"""
import logging
from pathlib import Path
import os
from confluent_kafka import avro

from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile():
    #key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    #filelocation=os.path.abspath(__file__)
    filelocation=os.path.dirname(__file__)



    key_schema = avro.load(filelocation+"/schemas/turnstile_key.json")
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    value_schema = avro.load(
        filelocation+"/schemas/turnstile_value.json"
        #f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # TODO: Complete the below by deciding on a
        # topic name, number of partitions, and number of replicas
        super().__init__(
            #f"com.udacity.station.turnstile.v1", # TODO: Come up with a better topic name
            "org.chicago.cta.station.turnstile.v1",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=4,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        # TODO: Complete this function by emitting a message
        # to the turnstile topic for the number of entries that were calculated

        #logger.debug(f"Passenger count :{num_entries} on {self.station.name} | {timestamp.isoformat()}")
        #Kenny Added
        logger.debug("Passenger count :"+str(num_entries)+"on"+ str(self.station.name) +'|'+str(timestamp.isoformat()))


        # produce a message from 0 to the number of entries
        for _ in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.name,
                        "line": self.station.color.name,
                    }
                )
            except Exception as e:
                logger.fatal(e)
                raise e
