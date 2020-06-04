from collections import defaultdict
from redash.query_runner import *
from redash.utils import json_dumps, json_loads

import logging

logger = logging.getLogger(__name__)


try:
    from pyhive import presto
    from pyhive.exc import DatabaseError

    enabled = True

except ImportError:
    enabled = False

PRESTO_TYPES_MAPPING = {
    "integer": TYPE_INTEGER,
    "tinyint": TYPE_INTEGER,
    "smallint": TYPE_INTEGER,
    "long": TYPE_INTEGER,
    "bigint": TYPE_INTEGER,
    "float": TYPE_FLOAT,
    "double": TYPE_FLOAT,
    "boolean": TYPE_BOOLEAN,
    "string": TYPE_STRING,
    "varchar": TYPE_STRING,
    "date": TYPE_DATE,
}


class Presto(BaseQueryRunner):
    noop_query = "SHOW TABLES"

    @classmethod
    def name(cls):
        return "Presto"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string",
                    "title": "Host: The hostname to connect to e.g. `presto.example.com`"
                },
                "port": {
                    "type": "number",
                    "default": "8080",
                    "title": "Port to connect to. Defaults to 8080"
                },
                "protocol": {
                    "type": "string",
                    "default": "http",
                    "title": "Protocol: Network protocol. Valid options at `http` and `https`. Defaults to `http`"
                },
                "catalog": {
                    "type": "string",
                    "default": "hive",
                    "title": "Catalog: Catalog within Presto. Defaults to `hive`"
                },
                "schema": {
                    "type": "string",
                    "default": "default",
                    "title": "Schema: Defaults to `default`"
                },
                "source": {
                    "type": "string",
                    "default": "redash",
                    "title": "Source: Artibrary identifiier. Defaults to `redash`"
                },
                "kerberos_remote_service_name": {
                    "type": "string",
                    "title": "Kerberos Remote Service Name"
                },
                "kerberos_principal": {
                    "type": "string",
                    "title": "Kerberos Principal."
                },
                "kerberos_config_path": {
                    "type": "string",
                    "default": "/etc/krb5.conf",
                    "title": "Kerberos Config Path"
                },
                "kerberos_keytab_path": {
                    "type": "string",
                    "title": "Kerberos Keytab Path"
                },
                "kerberos_credential_cache_path": {
                    "type": "string",
                    "title": "Kerberos Credential Cache Path"
                },
                "kerberos_use_canonical_hostname": {
                    "type": "string",
                    "default": "true",
                    "title": "Kerberos Use Canonical Hostname: Enabled by default"
                },
                "user_impersonation": {
                    "type": "boolean",
                    "default": "true",
                    "title": "Enable User Impersonation. Enabled by default",
                },
            },
            "order": [
                "host",
                "port",
                "protocol",
                "catalog",
                "schema",
                "username",
                "password",
                "source",
                "auth_mechanism",
                "kerberos_remote_service_name",
                "kerberos_principal",
                "kerberos_config_path",
                "kerberos_keytab_path",
                "kerberos_credential_cache_path",
                "kerberos_use_canonical_hostname",
                "user_impersonation",
            ],
            "required": ["host"],
            "secret": ["password"]
        }

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def type(cls):
        return "presto"

    def get_schema(self, get_stats=False):
        schema = {}
        query = """        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """

        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        results = json_loads(results)

        for row in results["rows"]:
            table_name = "{}.{}".format(row["table_schema"], row["table_name"])

            if table_name not in schema:
                schema[table_name] = {"name": table_name, "columns": []}

            schema[table_name]["columns"].append(row["column_name"])

        return list(schema.values())

    def run_query(self, query, user):
        query = query.encode('utf-8')

        enable_user_impersonation = self.configuration.get("user_impersonation")
        principal_username = None
        if enable_user_impersonation and user is not None:
            principal_username = user.name

        connection = presto.connect(
            host=self.configuration.get("host"),
            port=self.configuration.get("port", 8080),
            username=self.configuration.get("username"),
            principle_username=principal_username,
            catalog=self.configuration.get("catalog", "hive"),
            schema=self.configuration.get("schema", "default"),
            source=self.configuration.get("source", "redash"),
            protocol=self.configuration.get("protocol", "http"),
            password=self.configuration.get("password"),
            KerberosRemoteServiceName = self.configuration.get("kerberos_remote_service_name"),
            KerberosPrincipal = self.configuration.get("kerberos_principal"),
            KerberosConfigPath = self.configuration.get("kerberos_config_path"),
            KerberosKeytabPath = self.configuration.get("kerberos_keytab_path"),
            KerberosCredentialCachePath = self.configuration.get("kerberos_credential_cache_path"),
            KerberosUseCanonicalHostname = self.configuration.get("kerberos_use_canonical_hostname"),
        )

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            column_tuples = [
                (i[0], PRESTO_TYPES_MAPPING.get(i[1], None)) for i in cursor.description
            ]
            columns = self.fetch_columns(column_tuples)
            rows = [
                dict(zip(([column["name"] for column in columns]), r))
                for i, r in enumerate(cursor.fetchall())
            ]
            data = {"columns": columns, "rows": rows}
            json_data = json_dumps(data)
            error = None
        except DatabaseError as db:
            json_data = None
            default_message = "Unspecified DatabaseError: {0}".format(str(db))
            if isinstance(db.args[0], dict):
                message = db.args[0].get("failureInfo", {"message", None}).get(
                    "message"
                )
            else:
                message = None
            error = default_message if message is None else message
        except (KeyboardInterrupt, InterruptException, JobTimeoutException):
            cursor.cancel()
            raise

        return json_data, error


register(Presto)
