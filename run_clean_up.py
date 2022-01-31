import argparse

from hubspot_prod_to_sandbox import HubspotSandboxMigrator

parser = argparse.ArgumentParser(
    description="Script for cleaning up previous hubspot prod to sandbox migration using this sandbox in this environment."
)

parser.add_argument(
    "-p",
    "--production",
    required=True,
    action="store",
    dest="hubspot_production_api_key",
    help="Your Hubspot Production API key",
)

parser.add_argument(
    "-s",
    "--sandbox",
    required=True,
    action="store",
    dest="hubspot_sandbox_api_key",
    help="Your Hubspot Sandbox API Key",
)

args = parser.parse_args()

migrator = HubspotSandboxMigrator(
    args.hubspot_production_api_key, args.hubspot_sandbox_api_key
)

migrator.clean_up()
