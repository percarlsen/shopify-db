# Shopify-DB: minimalistic database for your shopify store

Shopify-db fetches essential information on customers, products, inventory and orders from your shopify store via shopify's APIs and stores it in a postgres database. Useful for backup, ad-hoc queries, analytics and integration purposes (e.g. [integration with the Norwegian accounting software Tripletex ](##tripletex-integration)).


## TODO
* Add support for credentials in the postgres database.
* Add progress bars.
* Comment code and functions.
* Connect to tripletex API.
* Instagram analytics.

## Getting started
1. __Set up a private app to access the shopify APIs:__ Enable private app development, create a private app and copy the app's credentials (API key and password). `shopify-db` uses the credentials to connect to your store, and will never be shared or used for other purposes. [This guide](https://help.shopify.com/en/manual/apps/private-apps) walks through the process. Make sure that the app has read access to customers, discounts, orders, gift cards, inventory, locations, products, shipping, shop locale and payments. Name the app as you want.

2. __Install requirements:__ `postgresql >= 12.6`, `python >= 3.9` and the requirements from the `Pipfile`. It is recommended that you use a virtual environment such as pipenv.

3. __Set up the database:__ make sure the project root is the current directory and run ```bash dbsetup.sh```. You can check out the database with `psql -d shopify`.

4. __Get your data from shopify:__ run the python script `shopifydb.py` with the subcommand `shopify-update`. See the [following section](#database-update) for more details.

## Database update
The subcommand `shopify-update` in main script, `shopifydb.py`, fetches data from your shopify store and adds and updates data in your postgres database. Run the following command to fetch all the information:
```bash
python shopifydb.py -v <store name> <api key> <api password> shopify-update
```

This may take a while depending on the size of your customer base and amount of orders. To update recent orders when once you have fetched all data, you can specify a start date limit to reduce the number of api calls and runtime significantly:
```bash
python shopifydb.py -v <store name> <api key> <api password> shopify-update -f 2021-06-01
```

For additional info, see `shopify-update -h`.

## Tripletex integration
Use the `tripletex-generate` subcommand to generate a .csv file with orders ready to be imported in the accounting software [Tripletex](https://www.tripletex.no/). The file will pass multiple sanity checks and give warnings in the terminal if there are any irregularities that should be addressed.
```bash
python shopifydb.py -v <store name> <api key> <api password> tripletex-generate tripletex-invoice/delme.csv <from date> <to date> <invoice start number> -g <gateway mappings>
```

, e.g.
```bash
python shopifydb.py -v <store name> <api key> <api password> tripletex-generate invoice.csv 2021-05-01 2021-06-30 144 -g vipps:Vipps stripe:Stripe
```

If you make any manual changes to the invoice file, you may use the subcommand `tripletex-verify` to run the same irregularity tests on the edited file:
```bash
python shopifydb.py -v <store name> <api key> <api password> tripletex-verify invoice.csv 144 vipps:Vipps stripe:Stripe
```

The invoice file is ready to be directly uploaded in Tripletex under 'Faktura > Fakturaimport'. Make sure to check the 'VAT included'-checkbox.

For more details and help, run `tripletex-generate -h`.
