import requests
import re
import pandas as pd
import numpy as np
import logging
from time import sleep

# Variables to controll API request retries
_RETRY_LIMIT = 10      # max. number of retried api calls before aborting
_RETRY_WAIT_TIME = 4   # seconds to wait before retry
_RETRY_INCREASE = 1.5  # Retry wait time factor

ORDER_FIELDS = [
    'id',
    'line_items',
    'name',
    'billing_address',
    'total_price',
    'closed_at',
    'created_at',
    'processed_at',
    'currency',
    'current_total_discounts',
    'current_subtotal_price',
    'fulfillment_status',
    'financial_status',
    'customer',
    'landing_site',
    'name',
    'shipping_lines',
    'taxes_included',
    'total_line_items_price',
    'total_discounts',
    'total_tax',
    'discount_applications',
]

CUSTOMER_FIELDS = [
    'id',
    'accepts_marketing',
    'created_at',
    'default_address',
    'email',
    'first_name',
    'last_name',
    'last_order_id',
    'last_order_name',
    'name',
    'note',
    'phone',
    'total_spent',
    'verified_email',
    'updated_at',
]

PRODUCT_FIELDS = [
    'id',
    'created_at',
    'product_type',
    'published_at',
    'status',
    'title',
    'updated_at',
    'variants',
    'vendor',
]

TRANSACTION_FIELDS = [
    'id',
    'location_id',
    'order_id',
    'amount',
    'authorization',
    'created_at',
    'currency',
    'error_code',
    'gateway',
    'kind',
    'message',
    'processed_at',
    'receipt',
    'status',
    'source_name',
]

REFUND_FIELDS = [
    'id',
    'note',
    'refund_line_items',
    'transactions',
    'created_at',
    'processed_at',
]


def pagination_links(rest_header):
    links = dict(next=None, previous=None)
    if 'Link' not in rest_header:
        return links
    for i in rest_header['Link'].split(', '):
        rel = re.search('(?<=rel=\").*?(?=\")', i)
        if rel is not None:
            links[rel.group(0)] = re.search(
                '(?<=page_info=).*?(?=>;)', i).group(0)
    return links


def fetch_all(
        api_key,
        api_pass,
        url,
        reference,
        fields,
        limit,
        created_at_min=None,
        created_at_max=None,
        filter_on_status=True,
        page=None):
    links = dict(next=None, previous=None)
    retries = 0
    current_wait_time = _RETRY_WAIT_TIME
    while retries < _RETRY_LIMIT:
        retries += 1
        # make the GET request. Cannot have any parameters if page is specified
        r = requests.get(
                url,
                auth=(api_key, api_pass),
                params={
                    'limit': limit,
                    'status': 'any' if filter_on_status and page is None else None,
                    'created_at_min': created_at_min if page is None else None,
                    'created_at_max': created_at_max if page is None else None,
                    'page_info': page,
                    'fields': ','.join(fields),
                }
            )

        if r.status_code == 200:
            break
        if r.status_code != 200 and retries < _RETRY_LIMIT-1:
            logging.info(
                f'Unsuccessful request to {url} (api limit reached?). '
                f'Retrying in {int(current_wait_time)} seconds'
            )
            sleep(current_wait_time)
            current_wait_time *= _RETRY_INCREASE
        elif r.status_code != 200:
            raise Exception(
                f'{retries} unsuccessful requests from {url}. '
                f'Error code {r.status_code}, reason: {r.reason}'
            )
    links = pagination_links(r.headers)
    # logging.info(f'Request status: {r.status_code}, {r.reason}')
    # logging.info(f'Request results: {len(r.json()[reference])}')

    return (links['next'], r.json()[reference])


def fetch_single(
    api_key, api_pass, fields, order_id, endpoint
) -> dict:
    url = (
        f'https://smeig.myshopify.com/admin/api/2021-04/'
        f'orders/{order_id}/{endpoint}.json')
    retries = 0
    current_wait_time = _RETRY_WAIT_TIME
    while retries < _RETRY_LIMIT:
        retries += 1
        r = requests.get(
            url,
            auth=(api_key, api_pass),
            params={
                'fields': ','.join(fields),
            },
        )
        if r.status_code == 200:
            break
        if r.status_code != 200 and retries < _RETRY_LIMIT-1:
            logging.info(
                f'Unsuccessful request to {url} (api limit reached?). '
                f'Retrying in {int(current_wait_time)} seconds'
            )
            sleep(current_wait_time)
            current_wait_time *= _RETRY_INCREASE
        elif r.status_code != 200:
            raise Exception(
                f'{retries} unsuccessful requests from {url}. '
                f'Error code {r.status_code}, reason: {r.reason}'
            )
    return r.json()


def update_customers(
    db, conn, url, api_key, api_pass,
    endpoint='customers.json', created_at_min=None,
    created_at_max=None, limit=250
) -> None:
    url = f'{url}{endpoint}'
    page = None
    while True:
        next_page, customers = fetch_all(
            api_key, api_pass, url, 'customers', CUSTOMER_FIELDS, limit,
            created_at_min=created_at_min,
            created_at_max=created_at_max,
            page=page
        )
        if not len(customers):
            break
        df = pd.json_normalize(customers, sep='_')
        df.rename(
            columns={
                'default_address_address1': 'address',
                'default_address_country': 'country',
                'default_address_zip': 'zip',
                'default_address_city': 'city',
                'default_address_name': 'name',
            },
            inplace=True
        )

        df['phone'] = df['phone'].fillna(df['default_address_phone'])
        df = df[[
            'id', 'email', 'name', 'first_name', 'last_name', 'phone',
            'address', 'city', 'country', 'zip', 'note', 'created_at',
            'updated_at', 'total_spent', 'verified_email', 'accepts_marketing'
            ]]
        # Replace np.nan with None to keep Postgres happy (no np.nan in Bigint)
        df.replace({np.nan: None}, inplace=True)
        db.update_customers(conn, df.to_dict(orient='records'))
        if next_page is None:
            break
        else:
            page = next_page


def update_orders(
    db, conn, url, api_key, api_pass, endpoint='orders.json', created_at_min=None, created_at_max=None, limit=250
) -> None:
    url = f'{url}{endpoint}'
    page = None
    cnt = dict(orders=0, line_items=0, shipping_lines=0)
    while True:
        next_page, orders = fetch_all(
            api_key, api_pass, url, 'orders', ORDER_FIELDS, limit,
            created_at_min=created_at_min,
            created_at_max=created_at_max,
            page=page
        )
        if not len(orders):
            break
        # extract order info
        order_df = pd.json_normalize(orders, sep='_')
        order_df = order_df[[
            'id', 'customer_id', 'name', 'created_at', 'processed_at',
            'closed_at', 'fulfillment_status', 'financial_status',
            'total_price', 'total_line_items_price', 'total_discounts',
            'total_tax', 'taxes_included', 'currency'
        ]]
        order_df.rename(
            columns={
                'total_discounts': 'total_discounts_amount',
                'total_tax': 'total_tax_amount'
            },
            inplace=True
        )
        # Replace np.nan with None to keep Postgres happy (no np.nan in Bigint)
        order_df.replace({np.nan: None}, inplace=True)
        db.update_orders(conn, order_df.to_dict(orient='records'))
        cnt["orders"] += order_df.id.nunique()

        # extract product lines / line items
        line_items = pd.json_normalize(
            [dict(
                li,
                **{
                    'order_id': order['id'],
                    'quantity': li['quantity'],
                    'tax_amount': li['tax_lines'][0]['price'] if li['tax_lines'] else 0,
                    'tax_rate': li['tax_lines'][0]['rate'] if li['tax_lines'] else 0,
                    'tax_title': li['tax_lines'][0]['title'] if li['tax_lines'] else None,
                    'currency': li['price_set']['presentment_money']['currency_code'],
                    'discount_amount': li['discount_allocations'][0]['amount'] if len(li['discount_allocations']) else 0
                }) for order in orders for li in order['line_items']],
            sep='_')
        line_items.rename(
            columns={
                'price': 'unit_price',
                'discount_amount': 'total_discount_amount'
            },
            inplace=True
        )
        line_items['total_price'] = (
            line_items['unit_price'].astype(float)
            * line_items['quantity'].astype(int)
        )
        line_items = line_items[[
            'id', 'order_id', 'product_id', 'title', 'sku', 'unit_price',
            'total_price', 'total_discount_amount',  'quantity', 'vendor',
            'variant_title', 'tax_amount', 'tax_rate', 'tax_title',
            'taxable', 'currency']]
        db.update_line_item_products(
            conn, line_items.replace({np.nan: None}).to_dict(orient='records')
        )
        cnt["line_items"] += line_items.id.nunique()

        # extract shipping details
        shipping = pd.DataFrame(
            [dict(
                sl,
                **{
                    'order_id': order['id'],
                    'address': order['billing_address']['address1'] if 'billing_address' in order else None,
                    'city': order['billing_address']['city'] if 'billing_address' in order else None,
                    'zip': order['billing_address']['zip'] if 'billing_address' in order else None,
                    'country': order['billing_address']['country'] if 'billing_address' in order else None,
                    'latitude': order['billing_address']['latitude'] if 'billing_address' in order else None,
                    'longitude': order['billing_address']['longitude'] if 'billing_address' in order else None,
                    'currency': sl['price_set']['presentment_money']['currency_code'],
                    'tax_title': sl['tax_lines'][0]['title'] if len(sl['tax_lines']) else None,
                    'tax_rate': sl['tax_lines'][0]['rate'] if len(sl['tax_lines']) else None,
                    'tax_amount': sl['tax_lines'][0]['price'] if len(sl['tax_lines']) else None,
                }) for order in orders for sl in order['shipping_lines']]
            )
        shipping = shipping[[
            'id', 'order_id', 'code', 'price', 'discounted_price', 'currency',
            'title', 'source', 'phone', 'address', 'city', 'zip', 'country',
            'latitude', 'longitude'
        ]]
        # Replace np.nan with None to keep Postgres happy (no np.nan in Bigint)
        shipping.replace({np.nan: None}, inplace=True)
        db.update_shipping(conn, shipping.to_dict(orient='records'))
        cnt["shipping_lines"] += shipping.id.nunique()

        # repeat if there are more pages, else quit
        if next_page is None:
            break
        else:
            page = next_page
    logging.info(
        f'Updated {cnt["orders"]} orders, {cnt["line_items"]} product lines '
        f'and {cnt["shipping_lines"]} shipping lines')


def update_transactions(
    db, conn, api_key, api_pass, order_ids
) -> None:
    if len(order_ids) > 100:
        logging.warning(
            f'Fetching transactions for {len(order_ids)} orders may '
            f' take a few minutes.'
        )
    transactions_cnt = 0
    transactions = []
    for ind, i in enumerate(order_ids):
        transaction = fetch_single(
            api_key, api_pass, TRANSACTION_FIELDS, i, 'transactions')
        if 'transactions' not in transaction:
            continue
        for trans in transaction['transactions']:
            transactions.append(
                dict(
                    id=trans['id'],
                    order_id=i,
                    status=trans['status'],
                    amount=trans['amount'],
                    currency=trans['currency'],
                    error_code=trans['error_code'],
                    gateway=trans['gateway'],
                    kind=trans['kind'],
                    created_at=trans['created_at'],
                    processed_at=trans['processed_at'],
                )
            )
        if ind and (ind % 10 == 0) or ind == len(order_ids)-1:
            # update db every 100th iteration just in case
            db.update_transactions(conn, transactions)
            transactions_cnt += len(transactions)
            transactions = []

    logging.info(f'Updated {transactions_cnt} transactions')


def update_refunds(db, conn, api_key, api_pass, order_ids) -> None:
    if len(order_ids) > 100:
        logging.warning(
            f'Fetching refunds for {len(order_ids)} orders may '
            f' take a few minutes.'
        )
    refunds_cnt = 0
    refunds_line_items_cnt = 0
    db_refunds = []
    db_refund_line_items = []
    for ind, i in enumerate(order_ids):
        refunds = fetch_single(
            api_key, api_pass, REFUND_FIELDS, i, 'refunds')

        for refund in refunds['refunds']:
            db_refunds.append(
                dict(
                    id=refund['id'],
                    order_id=i,
                    transaction_id=refund['transactions'][0]['id'],
                    note=refund['note'],
                    refund_product_cnt=len(refund['refund_line_items']) or 0,
                    created_at=refund['created_at'],
                    processed_at=refund['processed_at']
                )
            )
            for rli in refund['refund_line_items']:
                db_refund_line_items.append(
                    dict(
                        id=rli['id'],
                        refund_id=refund['id'],
                        line_item_product_id=rli['line_item']['id'],
                        quantity=rli['quantity'],
                        currency=rli['subtotal_set']['shop_money']['currency_code'],
                        refund_amount=rli['subtotal'],
                    )
                )
        if ind and (ind % 10 == 0) or ind == len(order_ids)-1:
            # update db every 100th iteration just in case
            db.update_refunds(conn, db_refunds)
            db.update_refund_line_items(conn, db_refund_line_items)
            refunds_cnt += len(db_refunds)
            refunds_line_items_cnt += len(db_refund_line_items)
            db_refunds = []
            db_refund_line_items = []
    logging.info(
        f'Updated {refunds_cnt} refunds with {refunds_line_items_cnt} '
        f'refunded line items'
    )


def update_products_and_variants(
    db, conn, url, api_key, api_pass,
    endpoint='products.json', created_at_min=None,
    created_at_max=None, limit=250
) -> None:
    url = f'{url}{endpoint}'
    page = None
    cnt = dict(products=0, variants=0)
    while True:
        next_page, products = fetch_all(
            api_key, api_pass, url, 'products', PRODUCT_FIELDS, limit,
            filter_on_status=False,
            created_at_min=created_at_min,
            created_at_max=created_at_max,
            page=page
        )
        if not len(products):
            break
        products_df = pd.DataFrame(products)
        cnt['products'] += products_df.id.nunique()
        products_df = products_df[[
            'id', 'title', 'status', 'product_type', 'created_at',
            'updated_at', 'vendor'
        ]].to_dict(orient='records')
        db.update_products(conn, products_df)

        product_variants = pd.DataFrame(
            [pv for product in products for pv in product['variants']])
        product_variants = product_variants[[
            'id', 'product_id', 'price', 'title', 'sku', 'option1',
            'option2', 'option3', 'created_at', 'updated_at'
        ]]
        # Replace np.nan with None to keep Postgres happy (no np.nan in Bigint)
        product_variants.replace({np.nan: None}, inplace=True)
        cnt['variants'] += product_variants.id.nunique()
        db.update_product_variants(
            conn, product_variants.to_dict(orient='records'))
        if next_page is None:
            break
        else:
            page = next_page
    logging.info(f'Updated {cnt["products"]} products and {cnt["variants"]} variants')
