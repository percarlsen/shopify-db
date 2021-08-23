import pandas as pd
import numpy as np
import datetime
import logging

INVOICE_REQUIRED_FIELDS = [
    'CUSTOMER NO',
    'ORDER NO',
    'PAID AMOUNT',
    'ORDER LINE - COUNT',
    'ORDER LINE - UNIT PRICE',
    'ORDER LINE - VAT CODE',
    'PAYMENT TYPE',
    'INVOICE DATE',
    'DELIVERY DATE',
    'ORDER DATE',
    'DUE DATE',
    'INVOICE NO',
]

INVOICE_OPTIONAL_FIELDS = [
    'CUSTOMER NAME',
    'ORDER LINE - PROD NAME',
    'ORDER LINE - DISCOUNT',
    'ORDER LINE - DESCRIPTION',
    'ORDER LINE - PROD NO',
]


def _none_values(df) -> bool:
    for i in INVOICE_REQUIRED_FIELDS:
        missing = df.loc[df[i].isna()]['ORDER NO'].unique()
        if len(missing):
            logging.warning(
                f'Required column {i} is missing for '
                f'orders {", ".join(missing)}'
            )
    return len(missing) == 0


def _description_or_sku(df) -> bool:
    errors = (
        df.loc[
            df[['ORDER LINE - PROD NO', 'ORDER LINE - DESCRIPTION']]
            .isna().all(axis=1)
        ]['ORDER NO'].unique()
    )
    if len(errors):
        logging.warning(
            f'The following {len(errors)} orders miss either '
            f'\'ORDER LINE - PROD NO\' or \'ORDER LINE - DESCRIPTION\': '
            f'{", ".join(errors)}'
        )
    return len(errors) == 0


def _order_no(df) -> bool:
    orders = [
        int(i) for i in df.loc[df['PAID AMOUNT'] >= 0]
        ['ORDER NO'].str[1:].unique()]
    missing_orders = [
        '#' + str(i) for i in range(min(orders)+1, max(orders))
        if i not in orders
    ]
    if len(missing_orders):
        logging.warning(
            f'The following {len(missing_orders)} orders are missing: '
            f'{", ".join(missing_orders)}'
        )
    return len(missing_orders) == 0


def _invoice_no(df) -> bool:
    inv = [
        int(i) for i in df['INVOICE NO'].unique()]
    missing_inv = [
        str(i) for i in range(min(inv)+1, max(inv)) if i not in inv]
    if len(missing_inv):
        logging.warning(
            f'The following {len(missing_inv)} invoice numbers '
            f'are missing: {", ".join(missing_inv)}'
        )
    return len(missing_inv) == 0


def _price(df) -> bool:
    df['price_after_discount'] = (
        df['ORDER LINE - COUNT'] * df['ORDER LINE - UNIT PRICE']
        * (100 - df['ORDER LINE - DISCOUNT']) / 100
    )
    grouped = df.groupby(['ORDER NO']).agg(
        paid_amount=pd.NamedAgg('PAID AMOUNT', 'first'),
        lineitems_total=pd.NamedAgg('price_after_discount', 'sum')
    )
    grouped['diff'] = abs(grouped.paid_amount - grouped.lineitems_total)
    # Get orders with 1% or higher deviation between paid amount and the
    # total price of all lineitems
    price_mismatch = grouped.loc[
        grouped['diff'] > abs(grouped.paid_amount) * 0.01].reset_index()
    for _, i in price_mismatch.iterrows():
        logging.warning(
            f'Order {i["ORDER NO"]} has a deviation between the total '
            f'amount paid and the sum of all lineitems of {i["diff"]}'
        )
    return len(price_mismatch) == 0


def _refunds(df) -> bool:
    refunds = set(list(df.loc[df['PAID AMOUNT'] <= 0]['ORDER NO']))
    if len(refunds):
        logging.info(
            f'The following {len(refunds)} orders have been '
            f'refunded and have \'-1\' appended to the order name to ensure '
            f'unique order numbers in Tripletex: {", ".join(refunds)}'
        )
    return True


def _unknown_gateway(df, gateway) -> bool:
    if gateway is None:
        return True
    flagged_gw = (
        df.loc[~df['PAYMENT TYPE'].isin(gateway)]
        .groupby(['ORDER NO', 'PAYMENT TYPE'])
        .first()
        .reset_index()
    )

    for _, i in flagged_gw.iterrows():
        logging.warning(
            f'Order {i["ORDER NO"]} has an unknown payment '
            f'gateway: \'{i["PAYMENT TYPE"]}\''
        )
    return len(flagged_gw) == 0


def get_invoices(
    db,
    conn,
    from_date: datetime.date.fromisoformat,
    to_date: datetime.date.fromisoformat,
    invoice_start_id: int,
) -> pd.DataFrame:
    return db.get_tripletex_invoice(
        conn, from_date, to_date, invoice_start_id
    )


def replace_invoice_gateway(
    df,
    gateway
) -> pd.DataFrame:
    df['PAYMENT TYPE'].replace(gateway, inplace=True)
    return df


def verify_invoices(
    df,
    gateway
) -> bool:
    df = df.replace('', np.nan)
    df.fillna(value=np.nan, inplace=True)

    # Display order counts
    n_refund = df.loc[df["PAID AMOUNT"] < 0]["ORDER NO"].nunique()
    n_ordinary = df.loc[df["PAID AMOUNT"] >= 0]["ORDER NO"].nunique()

    logging.info(
        f'There are {n_ordinary} ordinary orders and '
        f'{n_refund} refund-only orders'
    )

    tests_passed = [
        _order_no(df),
        _invoice_no(df),
        _none_values(df),
        _description_or_sku(df),
        _price(df),
        _unknown_gateway(df, gateway),
        _refunds(df)
    ]
    invoice_success = False not in tests_passed
    if invoice_success:
        logging.info(
            'Invoices were generated without any detected irregularities')
    else:
        logging.warning(
            'Invoices were generated but contain one or more warnings that '
            'should be checked manually'
        )

    return invoice_success
