from mqtt325.models import AppConfig, Heartbeat, Retainer

config = AppConfig(
    availability_topic="availability/mqtt325",
    heartbeat_routes=[
        # Process anything under heartbeat/ into an availability message
        Heartbeat("heartbeat/#", "availability/#"),
    ],
    retain_routes=[
        Retainer("retain/#", "#"),
    ],
)
"""Programmatic configuration, must be called "config" at module level."""
