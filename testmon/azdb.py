import base64
import json
import uuid

from collections import defaultdict, namedtuple

from azure.data.tables import TableServiceClient, UpdateMode

from testmon.process_code import (
    blob_to_checksums,
    checksums_to_blob,
    Fingerprint,
    Fingerprints,
)

DATA_VERSION = 0

ChangedFileData = namedtuple(
    "ChangedFileData", "filename name method_checksums id failed"
)


class TestmonDbException(Exception):
    pass


class AzDB:
    def __init__(self, endpoint, credential, environment="default"):
        self.endpoint = endpoint
        self.credential = credential

        self.env = environment

        self.init_tables()

    def create_table_service_client(self):
        return TableServiceClient(self.endpoint, credential=self.credential)

    # def _check_data_version(self, datafile):
    #     stored_data_version = self._fetch_data_version()

    #     if int(stored_data_version) == DATA_VERSION:
    #         return False

    #     self.con.close()
    #     os.remove(datafile)
    #     self.con = connect(datafile)
    #     return True

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def update_mtimes(self, new_mtimes):
        if not new_mtimes:
            return

        ops = {}
        for mtime, checksum, fingerprint_id in new_mtimes:
            ops[f"{self.env}:{fingerprint_id}"] = (
                    "update",
                    {
                        "PartitionKey": self.env,
                        "RowKey": fingerprint_id,
                        "mtime": mtime,
                        "checksum": checksum,
                    },
                    {"mode": UpdateMode.MERGE},
                )

        if ops:
            with self.create_table_service_client() as table_service, table_service.get_table_client(
                "fingerprint"
            ) as fingerprint:
                fingerprint.submit_transaction(ops.values())

    def remove_unused_fingerprints(self):
        # TODO:
        # with self.con as con:
        #     con.execute(
        #         """
        #         DELETE FROM fingerprint
        #         WHERE id NOT IN (
        #             SELECT DISTINCT fingerprint_id FROM node_fingerprint
        #         )
        #         """
        #     )
        pass

    def insert_node_fingerprints(
        self, nodeid, fingerprints, failed=False, duration=None
    ):
        node_id = base64.b64encode(nodeid.encode()).decode()

        with self.create_table_service_client() as table_service:
            with table_service.get_table_client("node") as node:
                entities = node.query_entities(
                    "PartitionKey eq @pk and RowKey eq @rk",
                    parameters={"pk": self.env, "rk": node_id},
                )
                if entities:
                    with table_service.get_table_client(
                        "nodefingerprint"
                    ) as node_fingerprint:
                        ops = []
                        for nfp in node_fingerprint.query_entities(
                            "PartitionKey eq @pk",
                            parameters={"pk": f"{self.env}:{node_id}"},
                        ):
                            ops.append(("delete", {"PartitionKey": nfp["PartitionKey"], "RowKey": nfp["RowKey"]}))

                        if ops:
                            node_fingerprint.submit_transaction(ops)

                entity = {
                    "PartitionKey": self.env,
                    "RowKey": node_id,
                    "duration": duration,
                    "failed": 1 if failed else 0,
                }
                node.upsert_entity(entity, mode=UpdateMode.REPLACE)

            # record: Fingerprint

            ops_fp = []
            ops_nf = []

            fingerprint_ids = []

            with table_service.get_table_client("fingerprint") as fingerprint:
                for record in fingerprints:
                    method_checksums_encoded = base64.b64encode(
                        checksums_to_blob(record["method_checksums"])
                    ).decode()

                    results = fingerprint.query_entities(
                        "PartitionKey eq @pk and filename eq @fn and method_checksums eq @mc",
                        parameters={
                            "pk": self.env,
                            "fn": record["filename"],
                            "mc": method_checksums_encoded,
                        },
                    )
                    for res in results:
                        fingerprint_ids.append(res["RowKey"])
                        break
                    else:
                        fingerprint_id = str(uuid.uuid4())
                        fingerprint.create_entity(
                            {
                                "PartitionKey": self.env,
                                "RowKey": fingerprint_id,
                                "filename": record["filename"],
                                "method_checksums": method_checksums_encoded,
                                "mtime": record["mtime"],
                                "checksum": record["checksum"],
                            }
                        )
                        fingerprint_ids.append(fingerprint_id)

            for fingerprint_id, record in zip(fingerprint_ids, fingerprints):
                ops_fp.append(
                    (
                        "update",
                        {
                            "PartitionKey": self.env,
                            "RowKey": fingerprint_id,
                            "mtime": record["mtime"],
                            "checksum": record["checksum"],
                        },
                        {"mode": UpdateMode.MERGE},
                    )
                )

                ops_nf.append(
                    (
                        "upsert",
                        {
                            "PartitionKey": f"{self.env}:{node_id}",
                            "RowKey": fingerprint_id,
                        },
                    )
                )

            with table_service.get_table_client("fingerprint") as fingerprint:
                fingerprint.submit_transaction(ops_fp)
            with table_service.get_table_client("nodefingerprint") as node_fingerprint:
                node_fingerprint.submit_transaction(ops_nf)

    # def _fetch_data_version(self):
    #     con = self.con

    #     return con.execute("PRAGMA user_version").fetchone()[0]

    def _write_attribute(self, attribute, data, environment=None):
        with self.create_table_service_client() as table_service, table_service.get_table_client(
            "metadata"
        ) as metadata:
            metadata.upsert_entity(
                {
                    "PartitionKey": environment or self.env,
                    "RowKey": attribute,
                    "data": json.dumps(data),
                }
            )

    def _fetch_attribute(self, attribute, default=None, environment=None):
        environment or self.env
        with self.create_table_service_client() as table_service, table_service.get_table_client(
            "metadata"
        ) as metadata:
            entity = metadata.get_entity(environment or self.env, attribute)

            if entity:
                return json.loads(entity["data"])

        return default

    def init_tables(self):
        with self.create_table_service_client() as table_service:
            table_service.create_table_if_not_exists("metadata")
            table_service.create_table_if_not_exists("node")
            table_service.create_table_if_not_exists("nodefingerprint")
            table_service.create_table_if_not_exists("fingerprint")

        # connection.execute("CREATE TABLE metadata (dataid TEXT PRIMARY KEY, data TEXT)")

        # connection.execute(
        #     """
        #     CREATE TABLE node (
        #         id INTEGER PRIMARY KEY ASC,   -
        #         environment TEXT,             PartitionKey
        #         name TEXT,                    RowKey
        #         duration FLOAT,               *
        #         failed BIT,                   *
        #         UNIQUE (environment, name)
        #     )
        #     """
        # )

        # connection.execute(
        #     """
        #     CREATE TABLE node_fingerprint (
        #         node_id INTEGER,
        #         fingerprint_id INTEGER,
        #         FOREIGN KEY(node_id) REFERENCES node(id) ON DELETE CASCADE,
        #         FOREIGN KEY(fingerprint_id) REFERENCES fingerid)
        #     )
        #     """
        # )

        # connection.execute(
        #     """
        #     CREATE table fingerprint
        #     (
        #         id INTEGER PRIMARY KEY,
        #         filename TEXT,            PartitionKey
        #         method_checksums BLOB,    RowKey
        #         mtime FLOAT,
        #         checksum TEXT,
        #         UNIQUE (filename, method_checksums)
        #     )
        #     """
        # )

        # connection.execute(f"PRAGMA user_version = {DATA_VERSION}")

    def get_changed_file_data(self, changed_fingerprints):
        if not changed_fingerprints:
            return []

        fingerprint_by_id = {}

        with self.create_table_service_client() as table_service:
            with table_service.get_table_client("fingerprint") as fingerprint:
                for row in fingerprint.query_entities(
                    "PartitionKey eq @pk and ({})".format(
                        " or ".join(f"RowKey eq '{fp}'" for fp in changed_fingerprints)
                    ),
                    parameters={"pk": self.env},
                ):
                    fingerprint_by_id[row["RowKey"]] = {
                        "filename": row["filename"],
                        "method_checksums": base64.b64decode(row["method_checksums"]),
                        "id": row["RowKey"],
                    }

            ids = []
            node_names = set()

            with table_service.get_table_client("nodefingerprint") as node_fingerprint:
                for row in node_fingerprint.query_entities(
                    " or ".join(f"RowKey eq '{fp}'" for fp in changed_fingerprints),
                    parameters={"pk": self.env},
                ):
                    env, node_name = row["PartitionKey"].split(":", 1)
                    if env != self.env:
                        continue

                    node_names.add(node_name)
                    ids.append((env, node_name, row["RowKey"]))

            nodes_by_name = {}

            with table_service.get_table_client("node") as node:
                for row in node.query_entities(
                    "PartitionKey eq @pk and ({})".format(
                        " or ".join(f"RowKey eq '{fp}'" for fp in node_names)
                    ),
                    parameters={"pk": self.env},
                ):
                    nodes_by_name[row["RowKey"]] = {
                        "name": base64.b64decode(row["RowKey"]).decode(),
                        "failed": row["failed"],
                        "duration": row.get("duration"),
                    }

        result = []
        for env, node_name, f_id in ids:
            node = nodes_by_name[node_name]
            f = fingerprint_by_id[f_id]
            result.append(
                [
                    f["filename"],
                    node["name"],
                    blob_to_checksums(f["method_checksums"]),
                    f["id"],
                    node["failed"],
                    node["duration"],
                ]
            )

        return result

    def delete_nodes(self, nodeids):
        if not nodeids:
            return

        ops = []
        for nodeid in nodeids:
            node_id  =base64.b64encode(nodeid.encode()).decode()
            ops.append(("delete", {"PartitionKey": self.env, "RowKey": node_id}))

        with self.create_table_service_client() as table_service, table_service.get_table_client(
            "node"
        ) as node:
            node.submit_transaction(ops)

    def all_nodes(self):
        with self.create_table_service_client() as table_service, table_service.get_table_client(
            "node"
        ) as node:
            return {
                base64.b64decode(row["RowKey"]).decode(): {
                    "duration": row.get("duration"),
                    "failed": row["failed"],
                }
                for row in node.query_entities(
                    "PartitionKey eq @pk",
                    parameters={
                        "pk": self.env,
                    },
                )
            }

    def filenames_fingerprints(self):
        with self.create_table_service_client() as table_service:
            fingerprint_to_node = defaultdict(list)
            with table_service.get_table_client("nodefingerprint") as node_fingerprint:
                for row in node_fingerprint.list_entities():
                    env, node_name = row["PartitionKey"].split(":", 1)
                    if env != self.env:
                        continue
                    fingerprint_to_node[row["RowKey"]].append(node_name)

            failed_counter = defaultdict(int)

            with table_service.get_table_client("node") as node:
                for row in node.query_entities(
                    f"PartitionKey eq @pk", parameters={"pk": self.env}
                ):
                    failed_counter[row["RowKey"]] += row["failed"]

            result = []

            with table_service.get_table_client("fingerprint") as fingerprint:
                for row in fingerprint.list_entities():
                    fingerprint_id = row["RowKey"]
                    if fingerprint_id not in fingerprint_to_node:
                        continue

                    failed = 0
                    for node in fingerprint_to_node[fingerprint_id]:
                        failed += failed_counter[node]

                    result.append(
                        {
                            "filename": row["filename"],
                            "mtime": row.get("mtime"),
                            "checksum": row.get("checksum"),
                            "fingerprint_id": fingerprint_id,
                            "failed": failed,
                        }
                    )

        return result
