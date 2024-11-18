# Datawald DynamodbAgency

The `datawald_dynamodbagency` module is a key component within the DataWald integration framework, providing advanced `dynamodb_connector` functionality to facilitate smooth data transformation and efficient data operations. It enables robust data retrieval and insertion processes, ensuring a streamlined and integrated data flow within DynamoDB.

## Configuration Guide

To integrate `datawald_dynamodbagency`, you will need to configure settings within the `se-configdata` table in DynamoDB as described below.

### Source Metadata

Define the metadata structure required for data retrieval and synchronization from various sources:

```json
{
 "setting_id": "datawald_dynamodbagency",
 "variable": "src_metadata",
 "value": {
  "hubspot": {
   "order": {
    "created_at": "created_at",
    "src_id": "ecom_so",
    "updated_at": "updated_at"
   }
  },
  "mage2": {
   "order": {
    "created_at": "created_at",
    "src_id": "ecom_so",
    "updated_at": "updated_at"
   },
   "product": {
    "created_at": "created_at",
    "src_id": "sku",
    "updated_at": "updated_at"
   }
  },
  "ns": {
   "company": {
    "created_at": "created_at",
    "src_id": "entity_id",
    "updated_at": "updated_at"
   }
  },
  "ss3": {
   "product": {
    "created_at": "created_at",
    "src_id": "sku",
    "updated_at": "updated_at"
   }
  }
 }
}
```

### Target Metadata

Define the metadata structure required for data insertion and synchronization across different targets:

```json
{
 "setting_id": "datawald_dynamodbagency",
 "variable": "tgt_metadata",
 "value": {
  "hubspot": {
   "company": {
    "history": false,
    "key": "entity_id",
    "table_name": "datamart_customers_v2"
   }
  },
  "ns": {
   "billcredit": {
    "history": false,
    "key": "bill_credit_number",
    "table_name": "datamart_vendorcredits_v2"
   },
   "creditmemo": {
    "history": false,
    "key": "creditmemo_no",
    "table_name": "datamart_creditmemo_v2"
   },
   "customer": {
    "history": false,
    "key": "entity_id",
    "table_name": "datamart_customers_v2"
   },
   ...
  }
 }
}
```

#### History Table

For tables with `history` enabled, specify the dedicated history table name:

```json
{
 "setting_id": "datawald_dynamodbagency",
 "variable": "history_table",
 "value": "datamart_history_v2"
}
```

### Transformation Data Mapping

Define columns and function rules for data transformation within DynamoDB:

```json
{
 "setting_id": "datawald_dynamodbagency",
 "variable": "TXMAP",
 "value": {
  "hubspot": {
   "order": {
    "accounting_memo": {
     "funct": "src.get('accounting_memo')",
     "src": [
      {
       "key": "accounting_memo",
       "label": "accounting_memo"
      }
     ],
     "type": "attribute"
    },
    "amount": {
     "funct": "str(src.get('total', 0))",
     "src": [
      {
       "key": "total",
       "label": "total"
      }
     ],
     "type": "attribute"
    },
    ...
  },
  "mage2": {
   "order": {
    "billing_address": {
     "funct": {
      "city": {
       "funct": "src['city']",
       "src": [
        {
         "key": "city",
         "label": "city"
        }
       ],
       "type": "attribute"
      },
      ...
     },
     "src": [
      {
       "key": "addresses|billto"
      }
     ],
     "type": "dict"
    },
    ...
   }
  }
 }
}
```

Each transformation rule in the configuration supports flexible data mapping and transformation, enabling effective alignment with target system requirements. 

This configuration will help you seamlessly integrate and transform data across multiple platforms using DynamoDB within the DataWald framework.