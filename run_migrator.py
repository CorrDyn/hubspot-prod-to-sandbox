from hubspot_prod_to_sandbox import HubspotSandboxMigrator
import argparse

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
        
parser = argparse.ArgumentParser(description='Script for migrating data from Hubspot Prod to Sandbox.')

parser.add_argument('-p',
                    '--production', 
                    required=True,
                    action="store", 
                    dest='hubspot_prod_api_key',
                    help="Your Hubspot Production API key")

parser.add_argument('-s',
                    '--sandbox', 
                    required=True,
                    action="store", 
                    dest='hubspot_sandbox_api_key',
                    help="Your Hubspot Sandbox API Key")

parser.add_argument('-o',
                    '--object', 
                    required=True,
                    action="store", 
                    dest='hs_object',
                    help="The primary object you want to migrate using this run of the migrator")

parser.add_argument('-l',
                    '--limit', 
                    type=int,
                    required=True,
                    action="store", 
                    dest='limit',
                    help="The number of objects you want to migrate using this run of the migrator")

parser.add_argument('-a',
                    '--associations', 
                    type=str2bool,
                    required=False,
                    action="store", 
                    dest='include_associations',
                    help="Whether you want to migrate associated records with the objects that are migrated")

parser.add_argument('-f',
                    '--fake-data', 
                    type=str2bool,
                    required=False,
                    action="store", 
                    dest='fake_data',
                    help="Whether you want to utilize fake data in place of personally identifiable information")

args = parser.parse_args()

migrator = HubspotSandboxMigrator(args.hubspot_prod_api_key,args.hubspot_sandbox_api_key)

if args.include_associations is not None:
    include_associations = args.include_associations
else:
    include_associations = False
    
if args.fake_data is not None:
    fake_data = args.fake_data
else:
    fake_data = False

migrator.migrate_object(hs_object=args.hs_object,
                        limit=args.limit,
                        include_associations=include_associations,
                        fake_data=fake_data)