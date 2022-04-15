#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import uuid, traceback
from datetime import datetime
from datawald_agency import Agency
from datawald_connector import DatawaldConnector
from dynamodb_connector import DynamoDBConnector
from silvaengine_utility import Utility

datetime_format = "%Y-%m-%dT%H:%M:%S%z"


class DynamoDBAgency(Agency):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.dynamodbconnector = DynamoDBConnector(logger, **setting)
        self.datawald = DatawaldConnector(logger, **setting)
        Agency.__init__(self, logger, datawald=self.datawald)

    def tx_entity_tgt(self, entity):
        tx_type = entity.get("tx_type_src_id").split("-")[0]
        table_name = self.setting["tgt_metadata"][entity.get("source")][tx_type][
            "table_name"
        ]
        key = self.setting["tgt_metadata"][entity.get("source")][tx_type]["key"]

        new_entity = dict(entity, **{"tgt_id": str(uuid.uuid1().int >> 64)})
        value = entity.get("tx_type_src_id").strip(f"{tx_type}-")
        if entity["data"].get(key) and entity["data"].get(key) != value:
            value = f"{entity['data'].get(key)}-{value}"

        item = self.dynamodbconnector.get_item(
            entity.get("source"), value, table_name=table_name, key=key
        )
        if item is not None:
            id = item["id"]
            new_entity.update({"tgt_id": id, "created_at": item["created_at"]})

            # Record the change in history.
            if entity.get("data") != item["data"] and self.setting["tgt_metadata"][
                entity.get("source")
            ][tx_type].get("history"):
                history = item.get("history", {})
                if len(history.keys()) > 9:
                    for key in sorted(history.keys(), reverse=True)[9:]:
                        history.pop(key)

                if entity.get("tx_type") == "inventorylot":
                    lots = [
                        lot
                        for lot in list(
                            map(
                                lambda x: self.get_lot_history(x, item["data"]),
                                entity.get("data"),
                            )
                        )
                        if lot is not None
                    ]
                    if len(lots) > 0:
                        new_entity.update(
                            {"history": history.update({item["updated_at"]: lots})}
                        )
        return new_entity

    def get_lot_history(self, lot, data):
        _lots = list(
            filter(lambda x: (x["inventoryNumber"] == lot["inventoryNumber"]), data)
        )
        if len(_lots) > 0 and lot != _lots[0]:
            return _lots[0]
        return None

    def insert_update_entity(self, entity):
        tx_type = entity.get("tx_type_src_id").split("-")[0]
        table_name = self.setting["tgt_metadata"][entity.get("source")][tx_type][
            "table_name"
        ]
        key = self.setting["tgt_metadata"][entity.get("source")][tx_type]["key"]
        try:
            value = entity.get("tx_type_src_id").strip(f"{tx_type}-")
            if entity["data"].get(key) and entity["data"].get(key) != value:
                value = f"{entity['data'].get(key)}-{value}"

            if isinstance(entity.get("created_at"), str):
                created_at = datetime.strptime(
                    entity.get("created_at"), datetime_format
                )
                created_at = created_at.strftime(datetime_format)
            else:
                created_at = entity.get("created_at").strftime(datetime_format)

            if isinstance(entity.get("updated_at"), str):
                updated_at = datetime.strptime(
                    entity.get("updated_at"), datetime_format
                )
                updated_at = updated_at.strftime(datetime_format)
            else:
                updated_at = entity.get("updated_at").strftime(datetime_format)

            _entity = {
                "id": entity.get("tgt_id"),
                "source": entity.get("source"),
                key: value,
                "data": Utility.json_loads(Utility.json_dumps(entity.get("data"))),
                "created_at": created_at,
                "updated_at": updated_at,
            }
            if entity.get("history"):
                _entity.update({"history": entity.pop("history")})

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
