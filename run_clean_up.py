from hubspot_prod_to_sandbox import HubspotSandboxMigrator

hubspot_sandbox_api_key = 'your_sandbox_api_key'
hubspot_prod_api_key = 'your_prod_api_key'

migrator = HubspotSandboxMigrator(hubspot_prod_api_key,hubspot_sandbox_api_key)

# migrator.migrate_object(hs_object='contacts',
#                         limit=2,
#                         include_associations=True,
#                         fake_data=True)

migrator.clean_up()