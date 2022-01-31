#!/usr/bin/env python
import json
import os
import sqlite3
import sys
import time
from functools import wraps
from pprint import pprint

import hubspot
import pandas as pd
import requests
from hubspot.crm.associations import BatchInputPublicAssociation
from hubspot.crm.products import (ApiException,
                                  BatchInputSimplePublicObjectInput,
                                  BatchReadInputSimplePublicObjectId,
                                  PublicObjectSearchRequest,
                                  SimplePublicObjectInput)
from mimesis import Address, Datetime, Finance, Food, Internet, Numeric, Person
from pandas import json_normalize

from conf.object_config import object_config

conn = sqlite3.connect("hubspot_migration_mappings.sqlite")


def show_time(func):
    """
    timer decorator
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        t0 = time.time()
        print(f"running {func.__name__}.. ", end="")
        sys.stdout.flush()
        v = func(*args, **kwargs)
        m, s = divmod(time.time() - t0, 60)
        st = "elapsed time:"
        if m:
            st += " " + f"{m:.0f} min"
        if s:
            st += " " + f"{s:.3f} sec"
        print(st)
        return v

    return wrapper


def is_sandbox(api_key):
    r = requests.get("https://api.hubapi.com/integrations/v1/me?hapikey=" + api_key)
    return r.json()["accountType"] == "SANDBOX"


def is_production(api_key):
    r = requests.get("https://api.hubapi.com/integrations/v1/me?hapikey=" + api_key)
    return r.json()["accountType"] == "STANDARD"


def get_portal_id(api_key):
    r = requests.get("https://api.hubapi.com/integrations/v1/me?hapikey=" + api_key)
    return r.json()["portalId"]


def test_object_config(object_config):
    return all(
        [object_config[k].get("properties", None) for k in object_config.keys()]
    ) and all(
        [type(object_config[k]["properties"]) == list for k in object_config.keys()]
    )


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


class HubspotSandboxMigrator:
    """Class for migrating data from Hubspot prod to Hubspot sandbox, given API keys for both"""

    def __init__(self, prod_api_key, sandbox_api_key):
        self.prod_api_key = prod_api_key
        self.sandbox_api_key = sandbox_api_key
        self.object_config = object_config

        if not is_sandbox(sandbox_api_key):
            raise ValueError(
                f"Sandbox API Key provided is not for a sandbox Hubspot instance!"
            )

        if not is_production(prod_api_key):
            raise ValueError(
                f"Prod API Key provided is not for a production Hubspot instance!"
            )

        if not test_object_config(object_config):
            raise ValueError(
                f"Every object in the Object Config must have a list or properties associated with it. Please review the example object_config."
            )

        self.prod_portal_id = get_portal_id(prod_api_key)
        self.sandbox_portal_id = get_portal_id(sandbox_api_key)

    def __repr__(self):
        return f"{self.__class__.__name__} for Sandbox Instance {self.sandbox_portal_id} and Prod Instance {self.prod_portal_id}"

    def get_hubspot_client(self, hs_object, environment="sandbox"):

        if environment == "sandbox":
            hs_client = hubspot.Client.create(api_key=self.sandbox_api_key)
        elif environment in ["prod", "production"]:
            hs_client = hubspot.Client.create(api_key=self.prod_api_key)

        if hs_object == "companies":
            hs_object_client = hs_client.crm.companies
        elif hs_object == "deals":
            hs_object_client = hs_client.crm.deals
        elif hs_object == "contacts":
            hs_object_client = hs_client.crm.contacts
        elif hs_object == "products":
            hs_object_client = hs_client.crm.products
        elif hs_object == "line_items":
            hs_object_client = hs_client.crm.line_items
        elif hs_object == "pipelines":
            hs_object_client = hs_client.crm.pipelines

        return hs_object_client

    def get_record_by_id(self, environment, hs_object, object_id):
        
        time.sleep(.4)

        hs_object_client = self.get_hubspot_client(hs_object, environment=environment)

        associations = [obj for obj in object_config.keys() if obj != hs_object]
        properties = object_config[hs_object]["properties"]

        if hs_object == "companies":
            api_response = hs_object_client.basic_api.get_by_id(
                company_id=object_id,
                archived=False,
                associations=associations,
                properties=properties,
            )
        elif hs_object == "deals":
            api_response = hs_object_client.basic_api.get_by_id(
                deal_id=object_id,
                archived=False,
                associations=associations,
                properties=properties,
            )
        elif hs_object == "contacts":
            api_response = hs_object_client.basic_api.get_by_id(
                contact_id=object_id,
                archived=False,
                associations=associations,
                properties=properties,
            )
        elif hs_object == "line_items":
            api_response = hs_object_client.basic_api.get_by_id(
                line_item_id=object_id,
                archived=False,
                associations=associations,
                properties=properties,
            )
        elif hs_object == "products":
            api_response = hs_object_client.basic_api.get_by_id(
                product_id=object_id,
                archived=False,
                associations=associations,
                properties=properties,
            )
        elif hs_object == "tickets":
            api_response = hs_object_client.basic_api.get_by_id(
                ticket_id=object_id,
                archived=False,
                associations=associations,
                properties=properties,
            )
        elif hs_object == "quotes":
            api_response = hs_object_client.basic_api.get_by_id(
                quote_id=object_id,
                archived=False,
                associations=associations,
                properties=properties,
            )
        else:
            print(
                f"""No Get Object by ID method incorporated for objects of type {hs_object}.
    Only the following objects are currently supported: companies, deals, contacts, line_items, products, tickets, quotes
                    """
            )
            return []

        return api_response.to_dict()

    def delete_record_by_id(self, hs_object, object_id):
        """Only available for sandbox"""
        time.sleep(.4)

        hs_object_client = self.get_hubspot_client(hs_object, environment="sandbox")

        if hs_object == "companies":
            api_response = hs_object_client.basic_api.archive(company_id=object_id)
        elif hs_object == "deals":
            api_response = hs_object_client.basic_api.archive(deal_id=object_id)
        elif hs_object == "contacts":
            api_response = hs_object_client.basic_api.archive(contact_id=object_id)
        elif hs_object == "line_items":
            api_response = hs_object_client.basic_api.archive(line_item_id=object_id)
        elif hs_object == "products":
            api_response = hs_object_client.basic_api.archive(product_id=object_id)
        elif hs_object == "tickets":
            api_response = hs_object_client.basic_api.archive(ticket_id=object_id)
        elif hs_object == "quotes":
            api_response = hs_object_client.basic_api.archive(quote_id=object_id)

        else:
            print(
                f"""No Delete Object by ID method incorporated for objects of type {hs_object}.
    Only the following objects are currently supported: companies, deals, contacts, line_items, products, tickets, quotes
                    """
            )
            return []

        return True

    def get_object_records(
        self, hs_object, limit, properties, associations=[], environment="prod"
    ):

        hs_object_client = self.get_hubspot_client(hs_object, environment=environment)

        if limit > 100:
            page_limit = 100
        else:
            page_limit = limit

        all_records = []
        i = 0
        pages = True

        while pages:
            if i == 0:
                api_response = hs_object_client.basic_api.get_page(
                    limit=page_limit,
                    archived=False,
                    properties=properties,
                    associations=associations,
                )
                if api_response.to_dict()["results"]:
                    all_records.extend(api_response.to_dict()["results"])
                    i += 1
                else:
                    pages = False

                if len(all_records) >= limit:
                    pages = False
            else:
                time.sleep(0.5)
                try:
                    api_response = prod_client.crm.companies.basic_api.get_page(
                        limit=page_limit,
                        archived=False,
                        properties=properties,
                        associations=associations,
                        after=api_response.paging.next.after,
                    )
                    if api_response.to_dict()["results"]:
                        all_records.extend(api_response.to_dict()["results"])
                        i += 1
                    else:
                        pages = False

                    if len(all_records) >= limit:
                        pages = False
                except:
                    pages = False
            if i % 10 == 0:
                print(i * 100, "objects downloaded")

        return all_records[:limit]

    def batch_create_records(self, list_of_object_properties):
        """Only available for sandbox"""

        hs_object_client = self.get_hubspot_client(hs_object, environment="sandbox")

        results = []

        record_chunk_list = chunks(list_of_object_properties, 10)

        for chunk in record_chunk_list:
            time.sleep(.4)
            batch_input_simple_public_object_input = BatchInputSimplePublicObjectInput(
                inputs=chunk
            )
            try:
                api_response = hs_object_client.batch_api.create(
                    batch_input_simple_public_object_input=batch_input_simple_public_object_input
                )
                #             pprint(api_response)
                results.extend(api_response.to_dict()["results"])
            except ApiException as e:
                print("Exception when calling batch_api->create: %s\n" % e)

        return results

    def create_sandbox_record_from_prod_record(self, hs_object, properties, prod_id):
        
        time.sleep(.4)
        hs_object_client = self.get_hubspot_client(hs_object, environment="sandbox")
        portal_id = self.sandbox_portal_id

        simple_public_object_input = SimplePublicObjectInput(properties=properties)

        if "hs_object_id" in properties:
            properties.pop("hs_object_id")
        if "lastmodifieddate" in properties:
            properties.pop("lastmodifieddate")
        if "hs_lastmodifieddate" in properties:
            properties.pop("hs_lastmodifieddate")
        if "createdate" in properties:
            properties.pop("createdate")

        try:
            api_response = hs_object_client.basic_api.create(
                simple_public_object_input=simple_public_object_input
            )
            result = api_response.to_dict()

            sandbox_id = result["id"]

            result["prod_id"] = prod_id

            conn = sqlite3.connect("hubspot_migration_mappings.sqlite")
            try:
                cur = conn.cursor()
                cur.execute(
                    f"INSERT INTO object_mappings_{portal_id} VALUES ({sandbox_id}, {prod_id}, '{hs_object}')"
                )
                conn.commit()
                conn.close()
            except:
                conn.close()
                raise

        except Exception as ex:
            print(ex)
            print(f"Skipping the creation of {hs_object} with Prod ID {prod_id}")
            result = {}
            pass

        return result

    def get_associated_records(self, hs_object, properties):

        environment = "prod"

        hs_object_client = self.get_hubspot_client(hs_object, environment=environment)
        portal_id = self.sandbox_portal_id

        object_results = []

        conn = sqlite3.connect("hubspot_migration_mappings.sqlite")
        ids_to_get_df = pd.read_sql_query(
            f"""SELECT DISTINCT 
                                                prod_to_id as id
                                            FROM prod_associations_{portal_id} 
                                            WHERE to_object = '{hs_object}'
                                            """,
            conn,
        )

        if ids_to_get_df.empty == False:
            for oid in ids_to_get_df["id"].tolist():
                result = self.get_record_by_id(environment, hs_object, oid)
                object_results.append(result)
        else:

            ids_to_get_df = pd.read_sql_query(
                f"""SELECT DISTINCT 
                                                prod_to_id as id
                                            FROM prod_associations_{portal_id} 
                                            WHERE to_object = '{' '.join(hs_object.split('_'))}'
                                            """,
                conn,
            )
            if ids_to_get_df.empty == False:
                object_results = []

                for oid in ids_to_get_df["id"].tolist():
                    result = get_record_by_id(environment, hs_object, oid)
                    object_results.append(result)

            else:
                print(f"No records of type {hs_object} found")
                object_results = []

        return object_results

    def setup_sqlite(self):
        portal_id = self.sandbox_portal_id
        conn = sqlite3.connect("hubspot_migration_mappings.sqlite")
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS object_mappings_{portal_id}
                    (sandbox_id BIGINT PRIMARY KEY NOT NULL, 
                     prod_id BIGINT, 
                     hs_object VARCHAR(256)
                     )"""
        )

        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS prod_associations_{portal_id}
                    (prod_from_id BIGINT, 
                     prod_to_id BIGINT,
                     from_object VARCHAR(256),
                     to_object VARCHAR(256),
                     hs_association_string VARCHAR(256)
                     )"""
        )

        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS sandbox_associations_{portal_id}
                    (sandbox_from_id BIGINT, 
                     sandbox_to_id BIGINT,
                     from_object VARCHAR(256),
                     to_object VARCHAR(256),
                     hs_association_string VARCHAR(256)
                     )"""
        )

        conn.commit()
        conn.close()

    def clear_sqlite(self):
        portal_id = self.sandbox_portal_id
        conn = sqlite3.connect("hubspot_migration_mappings.sqlite")
        cur = conn.cursor()

        cur.execute(f"DROP TABLE IF EXISTS object_mappings_{portal_id}")
        cur.execute(f"DROP TABLE IF EXISTS prod_associations_{portal_id}")
        cur.execute(f"DROP TABLE IF EXISTS sandbox_associations_{portal_id}")

        conn.commit()
        conn.close()
        self.setup_sqlite()

    def insert_mappings(self, df):
        """
        df must have three columns:
        sandbox_id: id of the object in sandbox
        prod_id: id of the corresponding object in prod
        hs_object: string of the object type that was created
        """
        portal_id = self.sandbox_portal_id

        try:
            assert list(df.columns) == ["sandbox_id", "prod_id", "hs_object"]
        except Exception as ex:
            print(
                "Dataframe Columns are not correctly named for insertion into object_mappings table. Should be ['sandbox_id','prod_id','hs_object']"
            )
            raise

        conn = sqlite3.connect("hubspot_migration_mappings.sqlite")
        cur = conn.cursor()
        df.to_sql(
            f"object_mappings_{portal_id}", con=conn, if_exists="append", index=False
        )
        conn.commit()
        conn.close()

        print(len(df), f"records uploaded to object_mappings_{portal_id} table")

    def insert_prod_associations(self, df):
        """
        df must have five columns:
        prod_from_id: integer id of the left-hand object on the association in prod
        prod_to_id: integer id of the right-hand object on the association in prod
        from_object: object type of the left-hand object on the association in prod
        to_object: object type of the right-hand object on the association in prod
        hs_association_string: string that Hubspot expects when creating an association
        """
        portal_id = self.sandbox_portal_id

        try:
            assert list(df.columns) == [
                "prod_from_id",
                "prod_to_id",
                "from_object",
                "to_object",
                "hs_association_string",
            ]
        except Exception as ex:
            print(
                "Dataframe Columns are not correctly named for insertion into object_mappings table. Should be ['prod_from_id','prod_to_id','from_object','to_object','hs_association_string']"
            )
            raise

        conn = sqlite3.connect("hubspot_migration_mappings.sqlite")
        cur = conn.cursor()
        df.to_sql(
            f"prod_associations_{portal_id}", con=conn, if_exists="append", index=False
        )
        conn.commit()
        conn.close()

        print(len(df), f"records uploaded to prod_associations_{portal_id} table")

    def insert_sandbox_associations(self, df):
        """
        df must have five columns:
        sandbox_from_id: integer id of the left-hand object on the association in prod
        sandbox_to_id: integer id of the right-hand object on the association in prod
        from_object: object type of the left-hand object on the association in prod
        to_object: object type of the right-hand object on the association in prod
        hs_association_string: string that Hubspot expects when creating an association
        """
        portal_id = self.sandbox_portal_id

        try:
            assert list(df.columns) == [
                "sandbox_from_id",
                "sandbox_to_id",
                "from_object",
                "to_object",
                "hs_association_string",
            ]
        except Exception as ex:
            print(
                "Dataframe Columns are not correctly named for insertion into object_mappings table. Should be ['sandbox_from_id','sandbox_to_id','from_object','to_object','hs_association_string']"
            )
            raise

        conn = sqlite3.connect("hubspot_migration_mappings.sqlite")
        cur = conn.cursor()
        df.to_sql(
            f"sandbox_associations_{portal_id}",
            con=conn,
            if_exists="append",
            index=False,
        )
        conn.commit()
        conn.close()

        print(len(df), "records uploaded to sandbox associations table")

    def get_object_properties_list(self, object_records):
        object_properties_df = pd.DataFrame([o["properties"] for o in object_records])
        if "hs_object_id" in list(object_properties_df.columns):
            object_properties_df = object_properties_df.drop(["hs_object_id"], axis=1)
        if "lastmodifieddate" in list(object_properties_df.columns):
            object_properties_df = object_properties_df.drop(
                ["lastmodifieddate"], axis=1
            )
        if "createdate" in list(object_properties_df.columns):
            object_properties_df = object_properties_df.drop(["createdate"], axis=1)

        object_properties_list = json.loads(
            object_properties_df.to_json(orient="records")
        )
        object_properties_list = [{"properties": r} for r in object_properties_list]

        return object_properties_list

    def get_object_properties(self, hs_object):
        return self.object_config[hs_object]["properties"]

    def get_properties(self, hs_object):
        hs_client = hubspot.Client.create(api_key=self.prod_api_key)
        return pd.DataFrame(
            hs_client.crm.properties.core_api.get_all(
                object_type=hs_object, archived=False
            ).to_dict()["results"]
        )

    def get_prod_associations(self, object_records):
        print("Getting Prod Associations")
        all_associations = []
        for obj in object_records:
            try:
                for k in obj["associations"].keys():
                    for result in obj["associations"][k]["results"]:
                        all_associations.append(
                            {
                                "prod_from_id": obj["id"],
                                "prod_to_id": result["id"],
                                "from_object": result["type"]
                                .split("_to_")[0]
                                .replace("y", "ie")
                                + "s",
                                "to_object": result["type"]
                                .split("_to_")[1]
                                .replace("y", "ie")
                                + "s",
                                "hs_association_string": result["type"],
                            }
                        )
            except:
                continue

        prod_associations_df = pd.DataFrame(all_associations)
        return prod_associations_df

    def find_product_mapping(self, product_name):
        hs_object = "products"
        sandbox_client = self.get_hubspot_client(hs_object, environment="sandbox")

        public_object_search_request = PublicObjectSearchRequest(
            filter_groups=[
                {
                    "filters": [
                        {
                            "value": product_name,
                            "propertyName": "name",
                            "operator": "EQ",
                        }
                    ]
                }
            ],
            limit=1,
        )
        try:
            time.sleep(.4)
            api_response = sandbox_client.search_api.do_search(
                public_object_search_request=public_object_search_request
            )
            result = api_response.to_dict()["results"][0]
        except Exception as ex:
            print(ex)
            print(
                f"No Product Exists in Sandbox for Production Product: {product_name}"
            )
            result = {}

        return result

    def create_product_mapping(self):
        hs_object = "products"

        prod_client = self.get_hubspot_client(hs_object, environment="prod")

        sandbox_client = self.get_hubspot_client(hs_object, environment="sandbox")

        try:
            api_response = prod_client.basic_api.get_page(limit=100, archived=False)
            results_json = api_response.to_dict()["results"]
        except ApiException as e:
            print("Exception when calling basic_api->get_page: %s\n" % e)

        sandbox_responses = []

        for j in results_json:
            result = self.find_product_mapping(j["properties"]["name"])
            if result:
                result["prod_id"] = j["id"]
                sandbox_responses.append(result)
            else:
                prod_id = j["id"]
                properties = j["properties"]
                properties.pop("hs_object_id")
                simple_public_object_input = SimplePublicObjectInput(
                    properties=properties
                )
                try:
                    time.sleep(.25)
                    api_response = sandbox_client.basic_api.create(
                        simple_public_object_input=simple_public_object_input
                    )
                    result = api_response.to_dict()
                    result["prod_id"] = prod_id
                    sandbox_responses.append(result)
                except ApiException as e:
                    print("Exception when calling basic_api->create: %s\n" % e)

        sandbox_mappings_df = pd.DataFrame(sandbox_responses)

        sandbox_mappings_df["hs_object"] = hs_object

        sandbox_mappings_df = sandbox_mappings_df[["id", "prod_id", "hs_object"]]

        sandbox_mappings_df.columns = ["sandbox_id", "prod_id", "hs_object"]

        self.insert_mappings(sandbox_mappings_df)
        return sandbox_mappings_df

    def clean_up(self, remove_products=False):
        portal_id = self.sandbox_portal_id
        conn = sqlite3.connect("hubspot_migration_mappings.sqlite")
        cur = conn.cursor()
        if remove_products:
            records_to_delete_df = pd.read_sql_query(
                f"""SELECT *
                                                    FROM object_mappings_{portal_id} 
                                                    """,
                conn,
            )
        else:
            records_to_delete_df = pd.read_sql_query(
                f"""SELECT *
                                                    FROM object_mappings_{portal_id} 
                                                    WHERE hs_object != 'products'
                                                    """,
                conn,
            )
        hs_objects = records_to_delete_df["hs_object"].unique().tolist()

        deleted_records = []

        for hs_obj in hs_objects:
            hs_object_client = self.get_hubspot_client(hs_obj, environment="sandbox")
            records_df = records_to_delete_df[
                records_to_delete_df["hs_object"] == hs_obj
            ]
            for index, row in records_df.iterrows():
                sandbox_id = row["sandbox_id"]
                response = self.delete_record_by_id(hs_obj, sandbox_id)
                deleted_records.append(response)
                try:
                    cur.execute(
                        f"DELETE FROM object_mappings_{portal_id} WHERE sandbox_id = {sandbox_id}"
                    )
                    conn.commit()
                except:
                    conn.commit()
                    conn.close()
                    raise

        self.delete_all_associations()

        conn.close()
        self.clear_sqlite()
        print(len(deleted_records), "records deleted from Sandbox")

    def create_all_associations(self):
        portal_id = self.sandbox_portal_id
        conn = sqlite3.connect("hubspot_migration_mappings.sqlite")

        final_associations = []

        distinct_association_types_df = pd.read_sql_query(
            f"""SELECT DISTINCT 
                                                                hs_association_string, 
                                                                from_object, 
                                                                to_object 
                                                                FROM prod_associations_{portal_id}""",
            conn,
        )

        for ix, association_row in distinct_association_types_df.iterrows():
            print(
                f"Inserting associations of type {association_row['hs_association_string']}"
            )
            associations_df = pd.read_sql_query(
                f"""SELECT b.sandbox_id as sandbox_from_id,
                                                     c.sandbox_id as sandbox_to_id,
                                                     from_object,
                                                     to_object,
                                                     hs_association_string
                                                    FROM prod_associations_{portal_id} as a
                                                    INNER JOIN object_mappings_{portal_id} as b ON a.prod_from_id = b.prod_id
                                                    INNER JOIN object_mappings_{portal_id} as c ON a.prod_to_id = c.prod_id
                                                    WHERE hs_association_string = '{association_row['hs_association_string']}'
                                """,
                conn,
            )

            input_sandbox_associations = []

            for index, row in associations_df.iterrows():
                new_rec = {
                    "from": {"id": row["sandbox_from_id"]},
                    "to": {"id": row["sandbox_to_id"]},
                    "type": row["hs_association_string"],
                }
                input_sandbox_associations.append(new_rec)

            sandbox_client = hubspot.Client.create(api_key=self.sandbox_api_key)

            batch_input_public_association = BatchInputPublicAssociation(
                inputs=input_sandbox_associations
            )
            try:
                time.sleep(.4)
                api_response = sandbox_client.crm.associations.batch_api.create(
                    from_object_type=association_row["from_object"],
                    to_object_type=association_row["to_object"],
                    batch_input_public_association=batch_input_public_association,
                )
                sandbox_associations = api_response.to_dict()["results"]
            except ApiException as e:
                print("Exception when calling batch_api->create: %s\n" % e)

            self.insert_sandbox_associations(associations_df)

        conn.commit()
        conn.close()
        return True

    def delete_all_associations(self):
        portal_id = self.sandbox_portal_id
        conn = sqlite3.connect("hubspot_migration_mappings.sqlite")
        cur = conn.cursor()

        distinct_association_types_df = pd.read_sql_query(
            f"""SELECT DISTINCT 
                                                                hs_association_string, 
                                                                from_object, 
                                                                to_object 
                                                                FROM sandbox_associations_{portal_id}""",
            conn,
        )

        for ix, association_row in distinct_association_types_df.iterrows():
            print(
                f"Deleting associations of type {association_row['hs_association_string']}"
            )
            associations_df = pd.read_sql_query(
                f"""SELECT * FROM sandbox_associations_{portal_id}
                                                WHERE hs_association_string = '{association_row['hs_association_string']}'
                                                """,
                conn,
            )

            input_sandbox_associations = []
            for index, row in associations_df.iterrows():
                new_rec = {
                    "from": {"id": row["sandbox_from_id"]},
                    "to": {"id": row["sandbox_to_id"]},
                    "type": row["hs_association_string"],
                }
                input_sandbox_associations.append(new_rec)

            sandbox_client = hubspot.Client.create(api_key=self.sandbox_api_key)
            batch_input_public_association = BatchInputPublicAssociation(
                inputs=input_sandbox_associations
            )
            try:
                time.sleep(.4)
                api_response = sandbox_client.crm.associations.batch_api.archive(
                    from_object_type=association_row["from_object"],
                    to_object_type=association_row["to_object"],
                    batch_input_public_association=batch_input_public_association,
                )
            except ApiException as e:
                print("Exception when calling batch_api->archive: %s\n" % e)

            cur.execute(
                f"""DELETE FROM sandbox_associations_{portal_id} 
            WHERE hs_association_string = '{association_row['hs_association_string']}'"""
            )
            print(f"{len(associations_df)} Associations deleted")
            conn.commit()

        conn.commit()
        conn.close()
        return True

    def replace_with_fake_data(self, property_json, hs_object):
        person = Person("en")
        address = Address()
        #         datetime = Datetime()
        #         finance = Finance()
        #         internet = Internet()
        #         food = Food()
        #         number = Numeric()

        available_properties = property_json.keys()
        #     to_replace = [k for k in available_properties if k in replaceable_properties]

        for p in available_properties:
            if p == "name" and hs_object == "contacts":
                property_json[p] = person.name()
            elif p in ["firstname", "first_name"]:
                property_json[p] = person.first_name()
            elif p in ["lastname", "last_name"]:
                property_json[p] = person.last_name()
            elif p == "email":
                property_json[p] = person.email()
            elif p == "address":
                property_json[p] = (
                    str(address.street_number())
                    + " "
                    + str(address.street_name())
                    + " "
                    + str(address.street_suffix())
                )
            elif p == "city":
                property_json[p] = address.city()
            elif p in ["zip", "post_code", "postal_code", "zip_code"]:
                property_json[p] = address.zip_code()
            elif p in ["phone", "phonenumber", "phone_number"]:
                property_json[p] = person.telephone()

        #             elif p == 'name' and hs_object == 'companies':
        #                 property_json[p] = finance.company()
        #             elif p == 'dealname' and hs_object == 'deals':
        #                 property_json[p] = food.dish()
        #             elif p == 'price':
        #                 property_json[p] = finance.price()

        #             elif p == 'state':
        #                 property_json[p] = address.state()
        #         elif p == 'website':
        #             property_json[p] = internet.url() ## Causes issues for companies in Hubspot, so leaving out
        #             elif p in ['occupation','job','job_title','job_role']:
        #                 property_json[p] = person.occupation()
        #             elif p in ['company']:
        #                 property_json[p] = finance.company()
        #             elif p in ['quantity','count','number']:
        #                 property_json[p] = number.integer_number(start=1)

        return property_json

    @show_time
    def migrate_object(
        self,
        hs_object,
        properties=[],
        include_associations=False,
        limit=100,
        fake_data=False,
    ):

        """
        hs_object: Which HS Object you are migrating, can be any of ['companies','deals','contacts','line_items','products']
        properties: If you want to not use the object_config, you can identify the list of properties you want migrated here
        include_associations: if you want all associated records in the tree migrated (i.e. all companies and deals associated with the contacts migrated), then select True
        limit: Maximum number of records to migrate, make this None if you intend to migrate all items
        fake_data: If you want personally identifiable information like name, address, email, phone to be replaced with fake data, select True
        """
        assert hs_object in object_config.keys()

        if not properties:
            try:
                #                 print(f"No properties provided. Attempting to utilize default properties for {hs_object}")
                properties = object_config[hs_object]["properties"]
            except Exception as ex:
                print("Unable to use default properties for object provided")
                raise

        print("Confirming that Sandbox API Key is for a Hubspot Sandbox Instance")
        try:
            assert is_sandbox(self.sandbox_api_key)
        except Exception as ex:
            print(
                "API Key provided for Sandbox is not actually a Sandbox Hubspot Instance"
            )
            raise

        print("Confirming that Prod API Key is for a Hubspot Production Instance")
        try:
            assert is_production(self.prod_api_key)
        except Exception as ex:
            print(
                "API Key provided for Prod is not actually a Production Hubspot Instance"
            )
            raise

        print("Getting Object Properties")
        object_properties_df = self.get_properties(hs_object)
        sandbox_object_client = self.get_hubspot_client(
            hs_object, environment="sandbox"
        )
        prod_object_client = self.get_hubspot_client(hs_object, environment="prod")

        print("Confirming that Properties Provided Match Object Properties")
        try:
            assert all([p in object_properties_df["name"].tolist() for p in properties])
        except Exception as ex:
            print(
                f"List of properties includes entries that are not api names on object {hs_object}"
            )
            print(f"Missing properties will be ignored when migrating")
            print("Sleeping for five seconds to give you an opportunity to cancel")
            time.sleep(5)

        print(f"Getting {hs_object} from Production")

        associations = [k for k in object_config.keys() if k != hs_object]

        object_records = self.get_object_records(
            hs_object, limit, properties, associations, environment="prod"
        )

        #     object_properties_list = get_object_properties_list(object_records)

        prod_records_df = json_normalize(object_records, sep="_")

        print(f"Creating {hs_object} in Sandbox")
        records_created = []
        #     records_created = batch_create_records(sandbox_object_client,
        #                                              object_properties_list)
        self.setup_sqlite()

        for rec in object_records:
            properties = rec["properties"]
            prod_id = rec["id"]

            if fake_data:
                properties = self.replace_with_fake_data(properties, hs_object)

            record_created = self.create_sandbox_record_from_prod_record(
                hs_object, properties, prod_id
            )
            records_created.append(record_created)

        sandbox_records_df = json_normalize(records_created, sep="_")

        associations_df = self.get_prod_associations(object_records)
        self.insert_prod_associations(associations_df)

        if include_associations:
            for hs_obj in object_config:
                if hs_obj == "products":
                    continue
                if hs_obj == "line_items":
                    product_mappings_df = self.create_product_mapping()
                    product_mapping_dict = {
                        str(pid): str(sid)
                        for pid, sid in zip(
                            product_mappings_df["prod_id"].tolist(),
                            product_mappings_df["sandbox_id"].tolist(),
                        )
                    }
                if hs_obj != hs_object:
                    sandbox_object_client = self.get_hubspot_client(
                        hs_obj, environment="sandbox"
                    )
                    prod_object_client = self.get_hubspot_client(
                        hs_obj, environment="prod"
                    )
                    print(f"Getting {hs_obj} from Production")

                    properties = self.get_object_properties(hs_obj)
                    object_records = self.get_associated_records(hs_obj, properties)

                    if object_records:
                        print(f"Creating {hs_obj} in Sandbox")
                        records_created = []

                        for rec in object_records:
                            properties = rec["properties"]

                            if fake_data:
                                properties = self.replace_with_fake_data(
                                    properties, hs_obj
                                )

                            prod_id = rec["id"]
                            if hs_obj == "line_items":
                                properties["hs_product_id"] = product_mapping_dict[
                                    properties["hs_product_id"]
                                ]
                            record_created = (
                                self.create_sandbox_record_from_prod_record(
                                    hs_obj, properties, prod_id
                                )
                            )
                            records_created.append(record_created)

                        sandbox_records_df = json_normalize(records_created, sep="_")

                        prod_records_df = json_normalize(object_records, sep="_")
                        sandbox_records_df = json_normalize(records_created, sep="_")

                        associations_df = self.get_prod_associations(object_records)
                        self.insert_prod_associations(associations_df)

            self.create_all_associations()

        if include_associations:
            print(
                f"Successfully migrated {limit} {hs_object} and their associated objects"
            )
        else:
            print(f"Successfully migrated {limit} {hs_object}")
