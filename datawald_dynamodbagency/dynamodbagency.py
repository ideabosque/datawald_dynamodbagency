#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
from time import sleep

__author__ = "bibow"

import uuid, traceback
from datetime import datetime
from deepdiff import DeepDiff
from datawald_agency import Agency
from datawald_connector import DatawaldConnector
from dynamodb_connector import DynamoDBConnector
from silvaengine_utility import Utility
from boto3.dynamodb.types import TypeDeserializer
from pytz import timezone

datetime_format = "%Y-%m-%dT%H:%M:%S%z"


class DynamoDBAgency(Agency):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.dynamodbconnector = DynamoDBConnector(logger, **setting)
        self.datawald = DatawaldConnector(logger, **setting)
        Agency.__init__(self, logger, datawald=self.datawald)
        if setting.get("tx_type"):
            Agency.tx_type = setting.get("tx_type")

        self.map = setting.get("TXMAP", {})

    def tx_entity_tgt(self, entity):
        tx_type = entity.get("tx_type_src_id").split("-")[0]
        table_name = self.setting["tgt_metadata"][entity.get("source")][tx_type][
            "table_name"
        ]
        key = self.setting["tgt_metadata"][entity.get("source")][tx_type]["key"]

        new_entity = dict(entity, **{"tgt_id": str(uuid.uuid1().int >> 64)})
        value = entity.get("tx_type_src_id").replace(f"{tx_type}-", "")
        if entity["data"].get(key) and entity["data"].get(key) != value:
            value = f"{entity['data'].get(key)}-{value}"

        item = self.dynamodbconnector.get_item(
            entity.get("source"), value, table_name=table_name, key=key
        )
        if item is not None:
            id = item["id"]
            new_entity.update({"tgt_id": id, "created_at": item["created_at"]})

            # Record the change in history.
            has_history = self.setting["tgt_metadata"][entity["source"]][tx_type][
                "history"
            ]
            data_diff = DeepDiff(
                Utility.json_loads(Utility.json_dumps(entity["data"])),
                Utility.json_loads(Utility.json_dumps(item["data"])),
            )
            if data_diff != {} and has_history:
                self.insert_history_entity(
                    tx_type,
                    entity.get("source"),
                    value,
                    item["updated_at"],
                    item["data"],
                )

        return new_entity

    def insert_history_entity(self, tx_type, source, value, updated_at, data):
        timestamp = datetime.timestamp(datetime.strptime(updated_at, datetime_format))
        self.dynamodbconnector.put_item(
            {
                "tx_type_source": f"{tx_type}-{source}",
                "value_timestamp": f"{value}_{int(timestamp)}",
                "updated_at": updated_at,
                "data": data,
            },
            table_name=self.setting["history_table"],
        )

    def insert_update_entity(self, entity):
        tx_type = entity.get("tx_type_src_id").split("-")[0]
        table_name = self.setting["tgt_metadata"][entity.get("source")][tx_type][
            "table_name"
        ]
        key = self.setting["tgt_metadata"][entity.get("source")][tx_type]["key"]
        try:
            value = entity.get("tx_type_src_id").replace(f"{tx_type}-", "")
            if entity["data"].get(key) and entity["data"].get(key) != value:
                value = f"{entity['data'].get(key)}-{value}"

            if isinstance(entity.get("created_at"), str):
                created_at = datetime.strptime(
                    entity.get("created_at"), datetime_format
                )
                created_at = created_at.strftime(datetime_format)
            else:
                created_at = (
                    entity.get("created_at")
                    .replace(tzinfo=timezone("UTC"))
                    .strftime(datetime_format)
                )

            if isinstance(entity.get("updated_at"), str):
                updated_at = datetime.strptime(
                    entity.get("updated_at"), datetime_format
                )
                updated_at = updated_at.strftime(datetime_format)
            else:
                updated_at = (
                    entity.get("updated_at")
                    .replace(tzinfo=timezone("UTC"))
                    .strftime(datetime_format)
                )

            _entity = {
                "id": entity.get("tgt_id"),
                "source": entity.get("source"),
                key: value,
                "data": Utility.json_loads(Utility.json_dumps(entity.get("data"))),
                "created_at": created_at,
                "updated_at": updated_at,
            }

            self.dynamodbconnector.put_item(_entity, table_name=table_name)
            entity.update(
                {
                    "tx_status": "S",
                    "tx_note": f"datawald -> {entity['target']}",
                    "tgt_id": entity.get("tgt_id"),
                }
            )
        except Exception:
            log = traceback.format_exc()
            entity.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
            self.logger.exception(log)

    def tx_transaction_tgt(self, transaction):
        return self.tx_entity_tgt(transaction)

    def tx_transaction_tgt_ext(self, new_transaction, transaction):
        pass

    def insert_update_transactions(self, transactions):
        for transaction in transactions:
            self.insert_update_entity(transaction)
        return transactions

    def tx_person_tgt(self, person):
        return self.tx_entity_tgt(person)

    def tx_person_tgt_ext(self, new_person, person):
        pass

    def insert_update_persons(self, persons):
        for person in persons:
            self.insert_update_entity(person)
        return persons

    def tx_asset_tgt(self, asset):
        return self.tx_entity_tgt(asset)

    def tx_asset_tgt_ext(self, new_asset, asset):
        pass

    def insert_update_assets(self, assets):
        for asset in assets:
            self.insert_update_entity(asset)
        return assets

    def ddb_deserialize(self, r, type_deserializer=TypeDeserializer()):
        return type_deserializer.deserialize({"M": r})

    def stream_handle(self, **params):
        entities = []
        table_name = None
        for record in params.get("records"):
            if record["eventName"] not in ("INSERT", "MODIFY"):
                continue

            entity = self.ddb_deserialize(record["dynamodb"]["NewImage"])
            table_name = record["eventSourceARN"].split("/")[1]

            self.logger.info(entity)

            entity["data"].update(
                {
                    "created_at": entity["created_at"],
                    "updated_at": entity["updated_at"],
                }
            )
            entities.append(entity)

        if len(entities) == 0:
            return

        for source in set(
            [entity["source"] for entity in entities if entity.get("source")]
        ):
            result = list(
                filter(
                    lambda x: x["table_name"] == table_name,
                    [
                        dict(v, **{"tx_type": k})
                        for k, v in self.setting["tgt_metadata"][source].items()
                    ],
                )
            )
            assert (
                len(result) > 0
            ), f"Cannot find the tx_type by the table_name ({table_name})!!!"

            if result[0].get("stream_target") is None:
                return

            _entities = list(filter(lambda x: x["source"] == source, entities))
            if isinstance(result[0].get("stream_target"), list):
                for stream_target in result[0].get("stream_target", []):
                    self.retrieve_entities_from_source(
                        **{
                            "source": source,
                            "target": stream_target,
                            "tx_type": result[0]["tx_type"],
                            "entities": _entities,
                        }
                    )
            else:
                self.retrieve_entities_from_source(
                    **{
                        "source": source,
                        "target": result[0]["stream_target"],
                        "tx_type": result[0]["tx_type"],
                        "entities": _entities,
                    }
                )

    def tx_entities_src(self, **kwargs):
        try:
            raw_entities = kwargs.pop("entities")
            entities = list(
                map(
                    lambda raw_entity: self.tx_entity_src(raw_entity, **kwargs),
                    raw_entities,
                )
            )

            return entities
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def tx_entity_src(self, raw_entity, **kwargs):
        tx_type = kwargs.get("tx_type")
        target = kwargs.get("target")
        entity = {
            "src_id": raw_entity[
                self.setting["src_metadata"][target][tx_type]["src_id"]
            ],
            "created_at": raw_entity[
                self.setting["src_metadata"][target][tx_type]["created_at"]
            ],
            "updated_at": raw_entity[
                self.setting["src_metadata"][target][tx_type]["updated_at"]
            ],
        }

        if type(entity["created_at"]) == str:
            entity["created_at"] = datetime.strptime(
                entity["created_at"], "%Y-%m-%dT%H:%M:%S%z"
            )
        if type(entity["updated_at"]) == str:
            entity["updated_at"] = datetime.strptime(
                entity["updated_at"], "%Y-%m-%dT%H:%M:%S%z"
            )

        try:
            if tx_type == "product":
                metadatas = self.get_product_metadatas(**kwargs)
                entity.update(
                    {"data": self.transform_data(raw_entity["data"], metadatas)}
                )
            else:
                entity.update(
                    {
                        "data": self.transform_data(
                            raw_entity["data"], self.map[target].get(tx_type)
                        )
                    }
                )
        except Exception:
            log = traceback.format_exc()
            entity.update({"tx_status": "F", "tx_note": log})
            self.logger.exception(log)

        return entity

    def tx_transactions_src(self, **kwargs):
        return self.tx_entities_src(**kwargs)

    def tx_persons_src(self, **kwargs):
        return self.tx_entities_src(**kwargs)

    def tx_assets_src(self, **kwargs):
        return self.tx_entities_src(**kwargs)
