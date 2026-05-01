import asyncio
import importlib
import logging
import os
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

from mqtt325.models import AppConfig

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)


# required
MQTT_HOST = os.environ["MQTT_HOST"]
# optional
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_USER = os.environ.get("MQTT_USER")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_TLS_CHAIN = os.environ.get("MQTT_TLS_CHAIN")
MQTT325_CONFIG_PATH = os.environ.get(
    "MQTT325_CONFIG", str(Path(__file__).parent.absolute() / "config.py")
)


class Mqtt325App:
    def __init__(self, config: AppConfig):
        self.config = config

    def on_connect(
        self,
        mqttc: mqtt.Client,
        userdata,
        flags: mqtt.ConnectFlags,
        reason_code: ReasonCode,
        properties,
    ):
        if reason_code == 0:
            logger.info("Connected. Subscribing...")
            for rr in self.config.retain_routes:
                logger.debug(
                    "Subscribing (%s) to match (%s) and retain at (%s)",
                    rr.input_topic,
                    rr.input_pattern,
                    rr.output_topic,
                )
                mqttc.subscribe(rr.input_topic)
            for hb in self.config.heartbeat_routes:
                logger.debug(
                    "Subscribing (%s) to match (%s) and publish ONLINE/OFFLINE at (%s)",
                    hb.input_topic,
                    hb.input_pattern,
                    hb.output_topic,
                )
                mqttc.subscribe(hb.input_topic)
            logger.info("Subscriptions done.")
            mqttc.publish(self.config.availability_topic, "ONLINE")
        else:
            logger.warning("Connection failed with reason code %s", reason_code)
        return

    def on_disconnect(
        self,
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

    def on_message(self, client: mqtt.Client, userdata, message: mqtt.MQTTMessage):
        top = message.topic

        logger.info("IN (%s)", top)
        # process retention routes
        for rr in self.config.retain_routes:
            if ptopic := rr.to_output_topic(top):
                logger.info("⏩ (%s) → (%s)", top, ptopic)
                client.publish(ptopic, message.payload, retain=True)
        # process heartbeats into ONLINE messages
        for hb in self.config.heartbeat_routes:
            if ptopic := hb.to_output_topic(top):
                logger.info("💚 (%s) → (%s)", top, ptopic)
                hb.register_beat(top)
                client.publish(ptopic, "ONLINE", retain=True)
        return

    def process_lost_heartbeats(self, client: mqtt.Client):
        for hb in self.config.heartbeat_routes:
            for src in hb.yield_timed_out():
                # beats are only registered from messages that match
                # this heartbeat route, therefore the publish topic
                # will always be available:
                ptopic = hb.to_output_topic(src)
                assert ptopic is None
                logger.info("💔 (%s) → (%s) OFFLINE", src)
                client.publish(ptopic, "OFFLINE")
        return

    async def run_async(self):
        logger.info("Connecting MQTT to %s:%s", MQTT_HOST, MQTT_PORT)
        client = mqtt.Client(
            client_id="mqtt-325",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        if MQTT_USER and MQTT_PASSWORD:
            client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        if MQTT_TLS_CHAIN:
            client.tls_set(ca_certs=os.environ["MQTT_TLS_CHAIN"])
        client.will_set(self.config.availability_topic, "OFFLINE")
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=5)
        client.on_connect = self.on_connect
        client.on_disconnect = self.on_disconnect
        client.on_message = self.on_message

        logger.info("Starting MQTT loop")
        client.loop_start()
        while True:
            try:
                self.process_lost_heartbeats(client)
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


def run():
    logger.info("⚙️ Importing config from '%s'", MQTT325_CONFIG_PATH)
    config_module = importlib.machinery.SourceFileLoader(
        "config", MQTT325_CONFIG_PATH
    ).load_module()
    config: AppConfig = getattr(config_module, "config")
    app = Mqtt325App(config)
    asyncio.run(app.run_async())
    logger.info("Exiting.")


if __name__ == "__main__":
    run()
