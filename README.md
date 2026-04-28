[![PyPI version](https://img.shields.io/pypi/v/mqtt325)](https://pypi.org/project/mqtt325)
[![pipeline](https://github.com/michaelosthege/mqtt325/workflows/pipeline/badge.svg)](https://github.com/michaelosthege/mqtt325/actions)

# MQTT 3 → 5

This small Python app provides MQTT message re-routing to create retained messages from devices
that don't implement MQTT v5 themselves.

## Installation

```bash
pip install mqtt325
```

## Configuration

Configure these environment variables:

* `MQTT_HOST`
* `MQTT_PORT` (optional)
* `MQTT_USER` (optional)
* `MQTT_PASSWORD` (optional)
* `MQTT_TLS_CHAIN` (optional PEM encoded certificate chain of the broker)
* `MQTT325_CONFIG_PATH` (optional path to a [`config.py` file](src/mqtt325/config.py) to use instead of the default)
