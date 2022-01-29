"""
Update the properties on each object that you want included in the migration. Delete objects that you do not want migrated.
"""

object_config = {
    "contacts": {
        "properties": [
            "address",
            "city",
            "company",
            "country",
            "email",
            "firstname",
            "jobtitle",
            "lastname",
            "lifecyclestage",
            "phone",
            "state",
            "website",
            "zip",
        ]
    },
    "companies": {
        "properties": [
            "name",
            "address",
            "address2",
            "domain",
            "phone",
            "type",
            "website",
            "zip",
            "state",
            "country",
        ]
    },
    "deals": {
        "properties": [
            "amount",
            "closedate",
            "dealstage",
            "dealname",
            "dealtype",
            "deal_status",
            "pipeline",
            "status",
        ]
    },
    "products": {"properties": ["name", "price", "description"]},
    "line_items": {
        "properties": [
            "amount",
            "description",
            "discount",
            "name",
            "price",
            "quantity",
            "recurringbillingfrequency",
            "tax",
            "hs_product_id",
        ]
    },
}
