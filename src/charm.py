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
from charms.data_platform_libs.v0.data_interfaces import DatabaseCreatedEvent
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


class NessieCharm(ops.CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.nessie_pebble_ready, self._on_nessie_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.database = DatabaseRequires(self, relation_name="database", database_name="names_db")
        self.framework.observe(self.database.on.database_created, self._on_database_created)
        self.framework.observe(self.database.on.endpoints_changed, self._on_database_created)
        self.framework.observe(self.on.database_relation_broken, self._on_database_relation_removed)

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

    def _on_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event is fired when postgres database is created."""
        self._update_layer_and_restart(None)

    def _on_database_relation_removed(self, event) -> None:
        """Event is fired when relation with postgres is broken."""
        self.unit.status = ops.WaitingStatus("Waiting for database relation")
    @property
    def app_environment(self):
        """This property method creates a dictionary containing environment variables
        for the application. It retrieves the database authentication data by calling
        the `fetch_postgres_relation_data` method and uses it to populate the dictionary.
        If any of the values are not present, it will be set to None.
        The method returns this dictionary as output.
        """
        db_data = self.fetch_postgres_relation_data()
        env = {
            "DEMO_SERVER_DB_HOST": db_data.get("db_host", None),
            "DEMO_SERVER_DB_PORT": db_data.get("db_port", None),
            "DEMO_SERVER_DB_USER": db_data.get("db_username", None),
            "DEMO_SERVER_DB_PASSWORD": db_data.get("db_password", None),
        }
        return env
    def fetch_postgres_relation_data(self) -> dict:
        """Fetch postgres relation data.

        This function retrieves relation data from a postgres database using
        the `fetch_relation_data` method of the `database` object. The retrieved data is
        then logged for debugging purposes, and any non-empty data is processed to extract
        endpoint information, username, and password. This processed data is then returned as
        a dictionary. If no data is retrieved, the unit is set to waiting status and
        the program exits with a zero status code."""
        relations = self.database.fetch_relation_data()
        logger.debug("Got following database data: %s", relations)
        for data in relations.values():
            if not data:
                continue
            logger.info("New PSQL database endpoint is %s", data["endpoints"])
            host, port = data["endpoints"].split(":")
            db_data = {
                "db_host": host,
                "db_port": port,
                "db_username": data["username"],
                "db_password": data["password"],
            }
            return db_data
        raise DatabaseNotReady()

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
                    "environment": self.app_environment,
                    #"environment": {
                    #    "GUNICORN_CMD_ARGS": f"--log-level {self.model.config['log-level']}"
                    #},
                }
            },
        }

    def _handle_ports(self):
        port = int(self.config["webui-port"])
        self.unit.set_ports(port)

    def _update_layer_and_restart(self, event) -> None:
        """Define and start a workload using the Pebble API.

        You'll need to specify the right entrypoint and environment
        configuration for your specific workload. Tip: you can see the
        standard entrypoint of an existing container using docker inspect

        Learn more about Pebble layers at https://github.com/canonical/pebble
        """
        # Learn more about statuses in the SDK docs:
        # https://juju.is/docs/sdk/constructs#heading--statuses
        self.unit.status = ops.MaintenanceStatus("Assembling pod spec")
        if self.container.can_connect():
            try:
                new_layer = self._pebble_layer.to_dict()
            except DatabaseNotReady:
                self.unit.status = ops.WaitingStatus("Waiting for database relation")
                return
            # Get the current pebble layer config
            services = self.container.get_plan().to_dict().get("services", {})
            if services != new_layer["services"]:
                # Changes were made, add the new layer
                self.container.add_layer("fastapi_demo", self._pebble_layer, combine=True)
                logger.info("Added updated layer 'fastapi_demo' to Pebble plan")

                self.container.restart(self.pebble_service_name)
                logger.info(f"Restarted '{self.pebble_service_name}' service")

            # add workload version in juju status
            self.unit.set_workload_version(self.version)
            self.unit.status = ops.ActiveStatus()
        else:
            self.unit.status = ops.WaitingStatus("Waiting for Pebble in workload container")

class DatabaseNotReady(Exception):
    """Signals that the database cannot yet be used."""



if __name__ == "__main__":  # pragma: nocover
    ops.main(NessieCharm)  # type: ignore

