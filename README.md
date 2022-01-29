# hubspot-prod-to-sandbox

### Migrate Production or Production-like Data from a Hubspot production instance to a Hubspot sandbox instance, using Python

## What You Will Need

1. A Hubspot Sandbox Instance, with pipelines and object definitions recently synced.
   - You can learn how to set up a Hubspot Sandbox Instance here: [Hubspot Sandbox Instructions](https://knowledge.hubspot.com/account/set-up-a-hubspot-sandbox-account) 
2. An API Key from a Production Hubspot Instance, called the `hubspot_prod_api_key` in code examples
3. An API Key from a Sandbox Hubspot Instance, called the `hubspot_sandbox_api_key` in code examples
4. The ability to run Python locally or remotely

## How to Set Up

```bash
git clone https://github.com/CorrDyn/hubspot-prod-to-sandbox.git
cd hubspot-prod-to-sandbox
python --version # confirm 3.8+

# create a new virtual environment.
python -m venv venv
# activate virtual environment.
# Note that the commands in the Set Up and Usage sections WILL NOT WORK if the virtual environment 
# is not active
# so you will need to run this command in any new shell session.
source ./venv/bin/activate
# install poetry to manage dependencies
pip install poetry

# install dependencies
poetry install
```

## How to Use

### 1. Setup your object configuration file
Go to `conf/object_config.py` and include the objects you want to migrate, along with the properties you want migrated from production to sandbox. This config file will drive the entire process.

### 2. Migrate your objects with three lines of code
Start with an Anchor Object and migrate all associated objects. 

Generally speaking, testing a process in Hubspot centers on one particular object, such as Contacts, Companies, or Deals. Select the Anchor Object to be the one that is most central to your sandbox testing.

```python
### import the module
from hubspot_prod_to_sandbox import HubspotSandboxMigrator

### instantiate the migrator with your prod API key and sandbox API Key
migrator = HubspotSandboxMigrator(hubspot_prod_api_key,hubspot_sandbox_api_key)

migrator.migrate_object(hs_object='contacts', ### What do you want to be your Anchor Object?
                        limit=2, ### How many objects do you want migrated
                        include_associations=True, ### If you want all associated objects in your object_config.py file to migrate
                        fake_data=True ### If you want personally identifiable information to be overwritten with fake data
                        )
```

### 3. When you're done testing in your Hubspot Sandbox, clean up your migrated records
```python
migrator.clean_up()
```

## Things to Keep in Mind

- This code will only read from a Hubspot Production instance and write to a Sandbox instance. There are tests built into the code to prevent you from writing to Production. With that said, you can edit the code to do other things with the Hubspot API. Happy coding.
- This code utilizes [SQLite](https://www.sqlite.org/index.html) to store information about what objects have been migrated and their corresponding associations between each other and between Prod and Sandbox. This means that a `.sqlite` file containing these mappings and associations will be stored in your repo after you run the code above. No PII is contained in this file.
- This code has been designed so that people who interact with multiple Prod and Sandbox environments (like agency support teams) can work in the same GitHub project and keep the mappings and associations separated, such that data is not mixed between Prod and Sandbox of different companies or clients. The most important thing is to keep your Prod and Sandbox API keys straight. If you do that, everything else should take care of itself.

## Do you like this project?
If you like this, shoot me an email at ross.katz@corrdyn.com to let me know how you're using it. 

Or [connect with me on LinkedIn](https://www.linkedin.com/in/b-ross-katz/) and tell me about it.

Or just star the repo and let me know how it can get better by filing an issue.

I hope this is useful to you! I know it would have saved me a lot of time...

### To Do List
- Account for custom objects
- Test tickets and quotes to make sure they operate as expected




                 



