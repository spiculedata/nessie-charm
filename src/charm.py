#!/usr/bin/env python3
# Copyright 2023 Ubuntu
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Charm the service.

Refer to the following tutorial that will help you
develop a new k8s charm using the Operator Framework:

https://juju.is/docs/sdk/create-a-minimal-kubernetes-charm
"""

import logging

import ops

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


class NessieCharm(ops.CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.nessie_pebble_ready, self._on_nessie_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)

    def _on_nessie_pebble_ready(self, event: ops.PebbleReadyEvent):
        """Define and start a workload using the Pebble API.

        Change this example to suit your needs. You'll need to specify the right entrypoint and
        environment configuration for your specific workload.

        Learn more about interacting with Pebble at at https://juju.is/docs/sdk/pebble.
        """
        # Get a reference the container attribute on the PebbleReadyEvent
        container = event.workload
        # Add initial Pebble config layer using the Pebble API
        container.add_layer("nessie", self._pebble_layer, combine=True)
        # Make Pebble reevaluate its plan, ensuring any services are started if enabled.
        container.replan()
        # Learn more about statuses in the SDK docs:
        # https://juju.is/docs/sdk/constructs#heading--statuses
        self.unit.status = ops.ActiveStatus()

    def _on_config_changed(self, event: ops.ConfigChangedEvent):
        """Handle changed configuration.

        Change this example to suit your needs. If you don't need to handle config, you can remove
        this method.

        Learn more about config at https://juju.is/docs/sdk/config
        """

        self._handle_ports()
        # Fetch the new config value
        log_level = self.model.config["log-level"].lower()

        # Do some validation of the configuration option
        if log_level in VALID_LOG_LEVELS:
            # The config is good, so update the configuration of the workload
            container = self.unit.get_container("nessie")
            # Verify that we can connect to the Pebble API in the workload container
            if container.can_connect():
                # Push an updated layer with the new config
                container.add_layer("nessie", self._pebble_layer, combine=True)
                container.replan()

                logger.debug("Log level for gunicorn changed to '%s'", log_level)
                self.unit.status = ops.ActiveStatus()
            else:
                # We were unable to connect to the Pebble API, so we defer this event
                event.defer()
                self.unit.status = ops.WaitingStatus("waiting for Pebble API")
        else:
            # In this case, the config option is bad, so block the charm and notify the operator.
            self.unit.status = ops.BlockedStatus("invalid log level: '{log_level}'")

    @property
    def _pebble_layer(self) -> ops.pebble.LayerDict:
        """Return a dictionary representing a Pebble layer."""
        return {
            "summary": "nessie layer",
            "description": "pebble config layer for nessie",
            "services": {
                "nessie": {
                    "override": "replace",
                    "summary": "nessie",
                    "command": "/usr/local/s2i/run",
                    "startup": "enabled",
                    #"environment": {
                    #    "GUNICORN_CMD_ARGS": f"--log-level {self.model.config['log-level']}"
                    #},
                }
            },
        }

    def _handle_ports(self):
        port = int(self.config["webui-port"])
        self.unit.set_ports(port)


if __name__ == "__main__":  # pragma: nocover
    ops.main(NessieCharm)  # type: ignore
