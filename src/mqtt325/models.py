"""Data models for mqtt325 app configuration."""

import re
import time
from dataclasses import dataclass, field
from typing import Generator, Sequence


@dataclass
class Retainer:
    """Base type of message retention router."""

    input_topic: str
    """Topic to subscribe.

    Optionally use `+` where to expect the client ID.
    Optionally append `#` to match any subtopic.
    """

    output_topic: str
    """Where to publish retained messages."""

    def __post_init__(self):
        if self.input_topic == self.output_topic:
            raise ValueError("Input and output topic must be different.")

    @property
    def input_pattern(self) -> str:
        """RegEx pattern to match topics of actual messages."""
        ip = self.input_topic.replace("+", r"(?P<client_id>.+?)")
        ip = ip.replace("#", r"(?P<subtopics>.+?)")
        return ip + "$"

    def to_output_topic(self, in_topic: str) -> str | None:
        """Determine the publish topic by matching the message topic with the ``input_pattern``."""
        cmatch = re.match(self.input_pattern, in_topic)
        out = None
        if cmatch:
            out = self.output_topic
            gd = cmatch.groupdict()
            if client_id := gd.get("client_id"):
                out = out.replace("+", client_id)
            if subtopics := gd.get("subtopics"):
                out = out.replace("#", subtopics)
        return out


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


@dataclass(frozen=True)
class AppConfig:
    availability_topic: str
    """Where the app will publish ONLINE/OFFLINE as retained messages."""

    heartbeat_routes: Sequence[Heartbeat]
    """List of heartbeat routing configurations."""

    retain_routes: Sequence[Retainer]
    """Routes for re-publishing with message retention without altering the payload."""
