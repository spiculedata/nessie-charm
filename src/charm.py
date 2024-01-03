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

import requests
import ops
from charms.data_platform_libs.v0.data_interfaces import DatabaseCreatedEvent
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires
from ops.model import WaitingStatus
# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


class NessieCharm(ops.CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.pebble_service_name = "nessie-service"
        self.container = self.unit.get_container("nessie")
        self.framework.observe(self.on.nessie_pebble_ready, self._on_nessie_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.database = DatabaseRequires(self, relation_name="database", database_name="names_db")
        self.framework.observe(self.database.on.database_created, self._on_database_created)
        self.framework.observe(self.database.on.endpoints_changed, self._on_database_created)
        self.framework.observe(self.on.database_relation_broken, self._on_database_relation_removed)
        self.unit.set_workload_version(self.version)

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
        logger.info("Collecting relation data")
        db_data = self.fetch_postgres_relation_data()
        logger.info("Populating env")
        env = {
            "NESSIE_VERSION_STORE_TYPE": "JDBC",
            "QUARKUS_DATASOURCE_JDBC_URL": "jdbc:postgresql://"+db_data.get("db_host", None)+":"+db_data.get("db_port", None)+"/"+db_data.get("db_database", None),
            "DEMO_SERVER_DB_HOST": db_data.get("db_host", None),
            "DEMO_SERVER_DB_PORT": db_data.get("db_port", None),
            "QUARKUS_DATASOURCE_USERNAME": db_data.get("db_username", None),
            "QUARKUS_DATASOURCE_PASSWORD": db_data.get("db_password", None),
        }
        logger.info('Dict: %s', env)
        logger.info("Returning env")
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
                "db_database": "names_db"
            }
            return db_data
        self.unit.status = WaitingStatus("Waiting for database relation")
        raise DatabaseNotReady()

    @property
    def _pebble_layer(self) -> ops.pebble.LayerDict:
        """Return a dictionary representing a Pebble layer."""
        return {
            "summary": "nessie layer",
            "description": "pebble config layer for nessie",
            "services": {
                self.pebble_service_name: {
                    "override": "replace",
                    "summary": "nessie",
                    "command": "printenv",
                    "startup": "enabled",
                    "environment": self.app_environment,
                }
            },
        }

    @property
    def version(self) -> str:
        """Reports the current workload (FastAPI app) version."""
        if self.container.can_connect() and self.container.get_services(self.pebble_service_name):
            try:
                return self._request_version()
            # Catching Exception is not ideal, but we don't care much for the error here, and just
            # default to setting a blank version since there isn't much the admin can do!
            except Exception as e:
                logger.warning("unable to get version from API: %s", str(e))
                logger.exception(e)
        return ""

    def _request_version(self) -> str:
        """Helper for fetching the version from the running workload using the API."""
        #resp = requests.get(f"http://localhost:{self.config['server-port']}/version", timeout=10)
        #return resp.json()["version"]
        return "1.0"

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
                new_layer = self._pebble_layer
            except DatabaseNotReady:
                self.unit.status = ops.WaitingStatus("Waiting for database relation")
                return
            # Get the current pebble layer config
            services = self.container.get_plan().to_dict().get("services", {})
            if services != new_layer["services"]:
                # Changes were made, add the new layer
                self.container.add_layer("nessie", self._pebble_layer, combine=True)
                logger.info("Added updated layer 'nessie' to Pebble plan")

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

