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
    noop_query = 'SHOW TABLES'

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'host': {
                    'type': 'string'
                },
                'port': {
                    'type': 'number',
                    'default': '8080'
                },
                'protocol': {
                    'type': 'string',
                    'default': 'http'
                },
                'catalog': {
                    'type': 'string',
                    'default': 'hive'
                },
                'schema': {
                    'type': 'string'
                },
                'source': {
                    'type': 'string',
                    'default': 'redash'
                },
                "kerberos_remote_service_name": {
                    "type": "string"
                },
                "kerberos_principal": {
                    "type": "string"
                },
                "kerberos_config_path": {
                    "type": "string",
                    "default": "/etc/krb5.conf"
                },
                "kerberos_keytab_path": {
                    "type": "string"
                },
                "kerberos_credential_cache_path": {
                    "type": "string"
                },
                "kerberos_use_canonical_hostname": {
                    "type": "boolean",
                    "default": "true"
                },
                "user_impersonation": {
                    "type": "boolean",
                    "default": "true"
                },
            },
            'order': [
                'host',
                'port',
                'protocol',
                'catalog',
                'schema',
                'source',
                'kerberos_remote_service_name',
                'kerberos_principal',
                'kerberos_config_path',
                'kerberos_keytab_path',
                'kerberos_credential_cache_path',
                'kerberos_use_canonical_hostname',
                'user_impersonation'
                ],
            'required': ['host']
        }

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def type(cls):
        return "presto"

    def get_schema(self, get_stats=False):
        schema = {}
        query = """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """

        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        results = json_loads(results)

        for row in results['rows']:
            table_name = '{}.{}'.format(row['table_schema'], row['table_name'])

            if table_name not in schema:
                schema[table_name] = {'name': table_name, 'columns': []}

            schema[table_name]['columns'].append(row['column_name'])

        return schema.values()

    def run_query(self, query, user):

        enable_user_impersonation = self.configuration.get("user_impersonation")
        principal_username = None
        if enable_user_impersonation and user is not None:
            principal_username = user.name

        connection = presto.connect(
            host=self.configuration.get('host', ''),
            port=self.configuration.get('port', 8080),
            protocol=self.configuration.get('protocol', 'http'),
            catalog=self.configuration.get('catalog', 'hive'),
            schema=self.configuration.get('schema', 'default'),
            source=self.configuration.get("source", "redash"),
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
            column_tuples = [(i[0], PRESTO_TYPES_MAPPING.get(i[1], None))
                             for i in cursor.description]
            columns = self.fetch_columns(column_tuples)
            rows = [dict(zip(([c['name'] for c in columns]), r))
                    for i, r in enumerate(cursor.fetchall())]
            data = {'columns': columns, 'rows': rows}
            json_data = json_dumps(data)
            error = None
        except DatabaseError as db:
            json_data = None
            default_message = 'Unspecified DatabaseError: {0}'.format(
                db.message)
            if isinstance(db.message, dict):
                message = db.message.get(
                    'failureInfo', {'message', None}).get('message')
            else:
                message = None
            error = default_message if message is None else message
        except (KeyboardInterrupt, InterruptException) as e:
            cursor.cancel()
            error = "Query cancelled by user."
            json_data = None
        except Exception as ex:
            json_data = None
            error = ex.message
            if not isinstance(error, basestring):
                error = unicode(error)

        return json_data, error


register(Presto)
