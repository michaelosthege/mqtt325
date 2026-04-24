import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Generator

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)


@dataclass
class Retainer:
    """Base type of message retention router."""

    input_topic: str
    """Topic to subscribe, with a `+` where to expect the client ID."""
    output_topic: str
    """Where to publish retained messages."""

    @property
    def input_pattern(self) -> str:
        """RegEx pattern to match actual client ID in the input topic."""
        return self.input_topic.replace("+", r"(.+?)") + "$"

    def to_output_topic(self, in_topic: str) -> str | None:
        """Match using the input topic pattern to determine the output topic."""
        cmatch = re.match(self.input_pattern, in_topic)
        if cmatch:
            client_id = cmatch.group(1)
            return self.output_topic.replace("+", client_id)
        return None


@dataclass
class Heartbeat(Retainer):
    """Sends retained ONLINE/OFFLINE messages to based on activity in the input topic."""

    timeout: int = 15
    """Seconds to wait before sending OFFLINE to the output topic."""

    _beats: dict[str, float] = field(default_factory=dict)

    def register_beat(self, source_topic: str):
        """Register a heartbeat from a matching source topic."""
        self._beats[source_topic] = time.time()

    def yield_timed_out(self) -> Generator[str, None, None]:
        """Iterate timed-out source topics, removing them from the cache."""
        buffer = dict(self._beats)
        for src, last_beat in buffer.items():
            if time.time() > last_beat + self.timeout:
                yield src
                self._beats.pop(src)
        return


# required
MQTT_HOST = os.environ["MQTT_HOST"]
# optional
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_USER = os.environ.get("MQTT_USER")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_TLS_CHAIN = os.environ.get("MQTT_TLS_CHAIN")

# what to retain
HEARTBEAT_ROUTERS = [
    Heartbeat("heartbeat/+", "availability/+"),
]

# app-specific topics
MQTT325_AVAILABILITY_TOPIC = "availability/mqtt325"


def on_connect(
    mqttc: mqtt.Client,
    userdata,
    flags: mqtt.ConnectFlags,
    reason_code: ReasonCode,
    properties,
):
    if reason_code == 0:
        logger.info("Connected. Subscribing...")
        # for debug: mqttc.subscribe("#")
        for hb in HEARTBEAT_ROUTERS:
            logger.debug(
                "Subscribing (%s) to match (%s) and publish ONLINE/OFFLINE at (%s)",
                hb.input_topic,
                hb.input_pattern,
                hb.output_topic,
            )
            mqttc.subscribe(hb.input_topic)
        logger.info("Subscriptions done.")
        mqttc.publish(MQTT325_AVAILABILITY_TOPIC, "ONLINE")
    else:
        logger.warning("Connection failed with reason code %s", reason_code)
    return


def on_disconnect(
    client: mqtt.Client,
    userdata,
    flags: mqtt.ConnectFlags,
    reason_code: ReasonCode,
    properties,
):
    if reason_code == 0:
        logger.info("Disconnected gracefully.")
    elif reason_code > 0:
        logger.warning("Disonnected unseccessfully. Reason code %s", reason_code)
    return


def on_message(client: mqtt.Client, userdata, message: mqtt.MQTTMessage):
    msg = message.payload.decode("utf-8")
    top = message.topic

    logger.info("IN (%s) → %s", top, msg)
    for hb in HEARTBEAT_ROUTERS:
        ptopic = hb.to_output_topic(top)
        if ptopic:
            logger.info("💚 (%s) → (%s)", top, ptopic)
            hb.register_beat(top)
            client.publish(ptopic, "ONLINE", retain=True)
    return


def process_lost_heartbeats(client: mqtt.Client):
    for hb in HEARTBEAT_ROUTERS:
        for src in hb.yield_timed_out():
            ptopic = hb.to_output_topic(src)
            if ptopic:
                logger.info("💔 (%s) → (%s) OFFLINE", src)
                client.publish(hb, "OFFLINE")
    return


async def main_async():
    logger.info("Connecting MQTT")
    client = mqtt.Client(
        client_id="mqtt-325",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    if MQTT_USER and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    if MQTT_TLS_CHAIN:
        client.tls_set(ca_certs=os.environ["MQTT_TLS_CHAIN"])
    client.will_set(MQTT325_AVAILABILITY_TOPIC, "OFFLINE")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=5)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    logger.info("Starting MQTT loop")
    client.loop_start()
    while True:
        try:
            process_lost_heartbeats(client)
            await asyncio.sleep(1)
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info("Shutting down...")
            # Disconnect asking the server to publish the last will message
            client.disconnect(ReasonCode(PacketTypes.DISCONNECT, "Disconnect", 4))
            client.loop_stop()
            break
        except Exception:
            logger.error("Error encountered.", exc_info=True)
    return


if __name__ == "__main__":
    asyncio.run(main_async())
    logger.info("Exiting.")
