from __future__ import annotations

from tqdm import tqdm
import logging
import argparse
import sys
import pandas as pd
import datetime

from shopify import (
    update_customers, update_orders, update_transactions, update_refunds,
    update_products_and_variants)
from db import Db
from tripletex import (
    get_invoices, verify_invoices, replace_invoice_gateway,
    INVOICE_REQUIRED_FIELDS, INVOICE_OPTIONAL_FIELDS)
from utils import create_shipping_heatmap
from typing import TypeVar, MutableMapping, Mapping, Callable


FT = TypeVar('FT')


class Subcommand():
    name: str
    func: FT

    def __init__(self, name: str, func: FT):
        self.name = name
        self.func = func

    @classmethod
    def get_decorator(
        cls,
        mapping: MutableMapping[str, Subcommand[Callable]],
    ) -> Callable[[Callable], Callable]:
        def subcommand_func(subcommand_name):
            def decorator(f):
                mapping[subcommand_name] = cls(subcommand_name, f)
                return f

            return decorator
        return subcommand_func

    def call_func(
        self,
        **kwargs: Mapping,
    ) -> None:
        self.func(**kwargs)


_SUBCOMMANDS: MutableMapping[str, Subcommand] = dict()
subcommand = Subcommand.get_decorator(_SUBCOMMANDS)


@subcommand('shopify-update')
def shopify_update(
    db,
    conn,
    api_url: str,
    key: str,
    password: str,
    created_at_min,
    created_at_max,
    *args,
    limit: int = 250,
    **kwargs,
) -> None:
    with tqdm(total=6) as pbar:
        update_customers(
            db, conn, api_url, key, password, created_at_min=created_at_min,
            created_at_max=created_at_max, limit=250)
        pbar.update(1)
        update_products_and_variants(
            db, conn, api_url, key, password, created_at_min=created_at_min,
            created_at_max=created_at_max, limit=250)
        pbar.update(1)
        update_orders(
            db, conn, api_url, key, password, created_at_min=created_at_min,
            created_at_max=created_at_max, limit=250)
        pbar.update(1)
        orders = db.get_orders(
            conn=conn,
            created_at_min=created_at_min,
            created_at_max=created_at_max
        )
        pbar.update(1)
        order_ids = list(orders['id'])
        update_transactions(db, conn, key, password, order_ids)
        pbar.update(1)
        refunded_order_ids = list(
            orders[orders.financial_status.str.contains('refund')]['id'])
        update_refunds(db, conn, key, password, refunded_order_ids)
        pbar.update(1)


@subcommand('heatmap')
def heatmap(
    db,
    conn,
    *args,
    **kwargs,
) -> None:
    hm = create_shipping_heatmap(db, conn)
    hm.save('orders-heatmap.html')


@subcommand('tripletex-verify')
def tripletex_verify(
    fn,
    gateway,
    *args,
    **kwargs
) -> None:
    invoices = pd.read_csv(fn, sep=';')
    logging.info('Verifying invoices')
    verify_invoices(invoices, gateway=[i[1] for i in gateway])


@subcommand('tripletex-generate')
def tripletex_generate(
    db,
    conn,
    store: str,
    from_date,
    to_date,
    invoice_start_id,
    gateway,
    fn,
    *args,
    **kwargs,
) -> None:
    logging.info('Generating invoices')
    invoices = get_invoices(
        db, conn, from_date, to_date, invoice_start_id
    )
    logging.info('Verifying invoices')
    if gateway is not None:
        # Replace gateway names if mapping provided
        invoices = replace_invoice_gateway(invoices, dict(gateway))
    # Sanity check invoices and flag inconsitensies
    verify_invoices(invoices, gateway=[i[1] for i in gateway])
    # Keep only columns accepted by Tripletex and save to file
    invoices[INVOICE_REQUIRED_FIELDS + INVOICE_OPTIONAL_FIELDS].to_csv(
        fn, index=False, sep=';')
    logging.info(
        f'Tripletex invoices for {store} from {from_date} to {to_date} has '
        f'been written to file {fn}. To upload in Tripletex, navigate to '
        f'\'Faktura\' > \'Fakturaimport\', tick the box to include VAT and '
        f'upload the invoices'
    )


def arghandler(argv):
    def pair(gateways):
        # Mapper from,to payment names
        return gateways.split(':')

    parser = argparse.ArgumentParser(
        description=(
            'Minimalistic database for shopify orders and customers in '
            'addition to a few utility commands'
        ),
        prog=argv[0],
    )

    parser.add_argument(
        'store', type=str, help='your store\'s name',
    )
    parser.add_argument(
        'key', type=str, help='your app\'s api key',
    )
    parser.add_argument(
        'password', type=str, help='your app\'s api password',
    )
    parser.add_argument(
        '-api', '--apiversion', type=str, default='2021-04',
        help='shopify api version',
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true'
    )

    subparsers = parser.add_subparsers(dest='subcommand', required=True)

    sparser = dict()
    for name, sc in _SUBCOMMANDS.items():
        sparser[name] = subparsers.add_parser(name)
        sparser[name].set_defaults(sc=sc)

    sparser['tripletex-generate'].add_argument(
        'fn', type=str, help='invoice file name'
    )
    sparser['tripletex-generate'].add_argument(
        'from_date',
        type=datetime.date.fromisoformat,
        help='date of the first invoice to include (format yyyy-mm-dd)'
    )
    sparser['tripletex-generate'].add_argument(
        'to_date',
        type=datetime.date.fromisoformat,
        help='date of the last invoice to include (format yyyy-mm-dd)'
    )
    sparser['tripletex-generate'].add_argument(
        'invoice_start_id', type=int,
        help=(
            'id of the first invoice to be generated. To find the correct '
            'number, check the id of your latest invoice in Tripletex and '
            'provide a value that is 1 greater'
        )
    )
    sparser['tripletex-generate'].add_argument(
        '-g', '--gateway', type=pair, nargs='+',
        help=(
            'case sensitive pairs (<old name>:<new name>) of names to rename '
            'gateways. Any other gateway will be flagged with a warning. '
            'E.g. \'-g stripe:Stripe vipps:Vipps\''
        )
    )

    sparser['tripletex-verify'].add_argument(
        'fn', type=str, help='invoice file name'
    )
    sparser['tripletex-verify'].add_argument(
        '-g', '--gateway', type=pair, nargs='+',
        help=(
            'case sensitive pairs (<old name>:<new name>) of names to rename '
            'gateways. Any other gateway will be flagged with a warning. '
            'E.g. \'-g stripe:Stripe vipps:Vipps\''
        )
    )

    sparser['shopify-update'].add_argument(
        '-f', '--from-date',
        type=datetime.date.fromisoformat,
        dest='created_at_min',
        help='date of the first shopify data to retrieve (format yyyy-mm-dd)'
    )
    sparser['shopify-update'].add_argument(
        '-t', '--to-date',
        type=datetime.date.fromisoformat,
        dest='created_at_max',
        help='date of the last shopify data to retrieve (format yyyy-mm-dd)'
    )

    args = parser.parse_args()
    return args


def main(argv):
    # Parse user arguments
    args = arghandler(argv)
    # Set appropriate verbose level
    if args.verbose:
        log_level = 'logging.INFO'
    else:
        log_level = 'logging.WARNING'
    logging.basicConfig(
        level=eval(log_level),
        format=f'{args.store.upper()} [%(levelname)s] %(message)s'
    )

    # Establish local db connection
    db = Db()
    # help(db)  # print db help
    conn = db.connect()
    api_url = f'https://{args.store}.myshopify.com/admin/api/{args.apiversion}/'
    args.sc.call_func(**vars(args), api_url=api_url, db=db, conn=conn)


if __name__ == '__main__':
    exit(main(sys.argv))
