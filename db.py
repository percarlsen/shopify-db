import attr
import psycopg2
from psycopg2.extras import execute_values
import logging
import pandas as pd
import datetime


@attr.s()
class Db(object):
    host: str = '127.0.0.1'
    user: str = ''
    password: str = ''
    port: str = '5432'
    dbname: str = 'shopify'
    """
    Class to represent the shopify postgres database

    Attributes
    ----------
    host: str, default '127.0.0.1'
        Database host address
    user: str, default 'per'
        User name used to authenticate
    password: str, default ''
        Password used to authenticate
    port: str, default '54321'
        Connection port number

    Methods
    -------
    connect()
        Connect to postgres database
    """

    def connect(self):  # -> psycopg2.connector:
        """ Connect to postgres database
        Returns
        ----------
        psycopg2.connector database connector instance
        """

        conn = None
        logging.info('Connecting to postgres')
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except Exception as e:
            logging.error('Connection failed')
            raise Exception(e)

        return conn

    def disconnect(self, conn):
        conn.close()

    def get_shipping(self, conn) -> pd.DataFrame:
        try:
            query = 'SELECT * FROM shipping;'
            return pd.read_sql(sql=query, con=conn)
        except Exception as e:
            raise Exception(f"Could not retrieve shipping from db. {e}")

    def get_orders(
        self, conn, created_at_min=None, created_at_max=None
    ) -> pd.DataFrame:
        try:
            if created_at_min and created_at_max:
                sql = (
                    'SELECT * FROM orders '
                    'WHERE DATE(created_at) BETWEEN %s::date AND %s::date;'
                )
                return pd.read_sql(
                    sql=sql, params=(created_at_min, created_at_max), con=conn)
            elif created_at_min:
                sql = (
                    'SELECT * FROM orders WHERE DATE(created_at) >= %s::date'
                )
                return pd.read_sql(
                    sql=sql, params=(created_at_min, ), con=conn)
            elif created_at_max:
                sql = (
                    'SELECT * FROM orders WHERE DATE(created_at) <= %s::date;'
                )
                return pd.read_sql(
                    sql=sql, params=(created_at_max, ), con=conn)
            else:
                sql = 'SELECT * FROM orders;'
                return pd.read_sql(sql=sql, con=conn)
        except Exception as e:
            raise Exception(f"Could not retrieve orders from db. {e}")

    def update_orders(self, conn, orders) -> None:
        cur = conn.cursor()
        columns = orders[0].keys()
        values = [[i for i in order.values()] for order in orders]
        query = f'''
            INSERT INTO orders ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE
              SET
                  customer_id = excluded.customer_id,
                  name = excluded.name,
                  fulfillment_status = excluded.fulfillment_status,
                  total_price = excluded.total_price,
                  total_line_items_price = excluded.total_line_items_price,
                  total_discounts_amount = excluded.total_discounts_amount,
                  total_tax_amount = excluded.total_tax_amount,
                  taxes_included = excluded.taxes_included,
                  created_at = excluded.created_at,
                  processed_at = excluded.processed_at,
                  closed_at = excluded.closed_at
            ;
        '''
        execute_values(cur, query, values)
        conn.commit()

    def update_discounts(self, conn, discounts) -> None:
        cur = conn.cursor()
        columns = discounts[0].keys()
        values = [[i for i in discount.values()] for discount in discounts]
        query = f'''
            INSERT INTO discount ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (order_id, discount_code) DO UPDATE
              SET
                  order_id = excluded.order_id,
                  discount_type = excluded.discount_type,
                  discount_value = excluded.discount_value,
                  discount_value_type = excluded.discount_value_type
            ;
        '''
        execute_values(cur, query, values)
        conn.commit()

    def update_transactions(self, conn, transactions) -> None:
        if len(transactions):
            cur = conn.cursor()
            columns = transactions[0].keys()
            values = [[i for i in trans.values()] for trans in transactions]
            query = f'''
                INSERT INTO transactions ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (id) DO UPDATE
                  SET
                      order_id = excluded.order_id,
                      status = excluded.status,
                      currency = excluded.currency,
                      error_code = excluded.error_code,
                      gateway = excluded.gateway,
                      kind = excluded.kind,
                      created_at = excluded.created_at,
                      processed_at = excluded.processed_at
                ;
            '''
            execute_values(cur, query, values)
            conn.commit()

    def update_refunds(self, conn, refunds) -> None:
        if len(refunds):
            cur = conn.cursor()
            columns = refunds[0].keys()
            values = [[i for i in r.values()] for r in refunds]
            query = f'''
                INSERT INTO refunds ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (id) DO UPDATE
                    SET
                        order_id = excluded.order_id,
                        transaction_id = excluded.transaction_id,
                        note = excluded.note,
                        refund_product_cnt = excluded.refund_product_cnt,
                        created_at = excluded.created_at,
                        processed_at = excluded.processed_at
                ;
            '''
            execute_values(cur, query, values)
            conn.commit()

    def update_refund_line_items(self, conn, refund_line_items) -> None:
        if len(refund_line_items):
            cur = conn.cursor()
            columns = refund_line_items[0].keys()
            values = [[i for i in r.values()] for r in refund_line_items]
            query = f'''
                INSERT INTO line_item_product_refunds ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (id) DO UPDATE
                    SET
                        refund_id = excluded.refund_id,
                        line_item_product_id = excluded.line_item_product_id,
                        refund_amount = excluded.refund_amount,
                        quantity = excluded.quantity,
                        currency = excluded.currency
                ;
            '''
            execute_values(cur, query, values)
            conn.commit()

    def update_customers(self, conn, customers) -> None:
        cur = conn.cursor()
        columns = customers[0].keys()
        values = [[i for i in c.values()] for c in customers]
        query = f'''
            INSERT INTO customers ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE
              SET
                  email = excluded.email,
                  name = excluded.name,
                  first_name = excluded.first_name,
                  last_name = excluded.last_name,
                  phone = excluded.phone,
                  address = excluded.address,
                  city = excluded.city,
                  zip = excluded.zip,
                  country = excluded.country,
                  total_spent = excluded.total_spent,
                  verified_email = excluded.verified_email,
                  accepts_marketing = excluded.accepts_marketing,
                  created_at = excluded.created_at,
                  updated_at = excluded.updated_at
            ;
        '''
        execute_values(cur, query, values)
        conn.commit()

    def update_line_item_products(self, conn, line_item_products) -> None:
        cur = conn.cursor()
        columns = line_item_products[0].keys()
        values = [[i for i in li.values()] for li in line_item_products]
        query = f'''
            INSERT INTO line_item_products ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE
              SET
                  order_id = excluded.order_id,
                  product_id = excluded.product_id,
                  title = excluded.title,
                  sku = excluded.sku,
                  unit_price = excluded.unit_price,
                  total_price = excluded.total_price,
                  total_discount_amount = excluded.total_discount_amount,
                  vendor = excluded.vendor,
                  variant_title = excluded.variant_title,
                  tax_amount = excluded.tax_amount,
                  tax_rate = excluded.tax_rate,
                  taxable = excluded.taxable,
                  currency = excluded.currency,
                  quantity = excluded.quantity
            ;
        '''
        execute_values(cur, query, values)
        conn.commit()

    def update_shipping(self, conn, shipping) -> None:
        cur = conn.cursor()
        columns = shipping[0].keys()
        values = [[i for i in s.values()] for s in shipping]
        query = f'''
            INSERT INTO shipping ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE
              SET
                  order_id = excluded.order_id,
                  code = excluded.code,
                  price = excluded.price,
                  discounted_price = excluded.discounted_price,
                  currency = excluded.currency,
                  title = excluded.title,
                  source = excluded.source,
                  phone = excluded.phone,
                  address = excluded.address,
                  city = excluded.city,
                  zip = excluded.zip,
                  country = excluded.country,
                  latitude = excluded.latitude,
                  longitude = excluded.longitude
            ;
        '''
        execute_values(cur, query, values)
        conn.commit()

    def update_products(self, conn, products) -> None:
        cur = conn.cursor()
        columns = products[0].keys()
        values = [[i for i in product.values()] for product in products]
        query = f'''
            INSERT INTO products ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE
              SET
                  title = excluded.title,
                  status = excluded.status,
                  product_type = excluded.product_type,
                  created_at = excluded.created_at,
                  updated_at = excluded.updated_at,
                  vendor = excluded.vendor
            ;
        '''
        execute_values(cur, query, values)
        conn.commit()

    def update_product_variants(self, conn, product_variants) -> None:
        cur = conn.cursor()
        columns = product_variants[0].keys()
        values = [[i for i in pv.values()] for pv in product_variants]
        query = f'''
            INSERT INTO product_variants ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE
              SET
                  product_id = excluded.product_id,
                  price = excluded.price,
                  title = excluded.title,
                  sku = excluded.sku,
                  option1 = excluded.option1,
                  option2 = excluded.option2,
                  option3 = excluded.option3,
                  created_at = excluded.created_at,
                  updated_at = excluded.updated_at
            ;
        '''
        execute_values(cur, query, values)
        conn.commit()

    def get_tripletex_invoice(
        self,
        conn,
        from_date: datetime.date.fromisoformat,
        to_date: datetime.date.fromisoformat,
        invoice_start_id: int,
    ) -> pd.DataFrame:
        """ Get ordes in the appropriate format for Tripletex

        Parameters
        ----------
        conn: psycopg2.connector
            Database connector
        from_date:
        to_date:
        invoice_start_id:

        Returns
        -------
        """
        try:
            sql = (
                '''
                    WITH invoice AS (
                        SELECT *
                        FROM tripletex_invoice
                        WHERE "ORDER DATE" BETWEEN %s AND %s
                    )
                    SELECT
                        ti.transaction_id,
                        ti.order_id,
                        ti."CUSTOMER NO",
                        ti."CUSTOMER NAME",
                        ti."ORDER NO",
                        ti."PAID AMOUNT",
                        ti."PAYMENT TYPE",
                        ti."ORDER LINE - COUNT",
                        ti."ORDER LINE - PROD NAME",
                        ti."ORDER LINE - UNIT PRICE",
                        ti."ORDER LINE - DISCOUNT",
                        ti."ORDER LINE - VAT CODE",
                        ti."ORDER LINE - DESCRIPTION",
                        ti."ORDER LINE - PROD NO",
                        ti."INVOICE DATE",
                        ti."DELIVERY DATE",
                        ti."ORDER DATE",
                        ti."DUE DATE",
                        %s + ind."INVOICE NO"-1 AS "INVOICE NO"
                    FROM tripletex_invoice ti
                    RIGHT JOIN (
                        SELECT
                            "ORDER NO",
                            payment_tag,
                            ROW_NUMBER() OVER() AS "INVOICE NO"
                        FROM invoice
                        GROUP BY "ORDER NO", payment_tag
                    ) ind
                    ON ti."ORDER NO" = ind."ORDER NO"
                    AND ti.payment_tag = ind.payment_tag
                    ORDER BY "INVOICE NO", "CUSTOMER NAME"
                    ;
                '''
            )
            return (
                pd.read_sql(
                    sql=sql,
                    con=conn,
                    params=(from_date, to_date, invoice_start_id)
                )
            )
        except Exception as e:
            raise Exception(
                f'Could not retrieve invoices for tripletex from db. {e}')

    def create_product_table(self, conn) -> None:
        """ Create product table if it doesnt exist

        Parameters
        ----------
        conn: psycopg2.connector
            Database connector
        """
        cur = conn.cursor()
        cur.execute(
            '''
                CREATE TABLE IF NOT EXISTS product (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) UNIQUE NOT NULL,
                    brand VARCHAR(50),
                    price REAL,
                    currency VARCHAR(10),
                    img VARCHAR(1000)
                );
            '''
        )
        conn.commit()

    def get_all_products(self, conn) -> pd.DataFrame:
        """ Retrieve all products from database

        Parameters
        ----------
        conn: psycopg2.connector
            Database connector

        Returns
        -------
        Pandas DataFrame with all matching products
        """
        try:
            sql = (
                '''
                    SELECT * FROM product ORDER BY brand, price;
                '''
            )
            return pd.read_sql(sql=sql, con=conn)
        except Exception as e:
            raise Exception(f"Could not retrieve products from db. {e}")

    def get_product_by_name(self, conn, product) -> None:
        """ Retrieve product by product name

        Parameters
        ----------
        conn: psycopg2.connector
            Database connector
        product: str
            Full or partial product name

        Returns
        -------
        Pandas DataFrame with all matching products
        """
        try:
            # FIXME prone to injection. Does read_sql support query variables?
            sql = (
                f'''
                    SELECT * FROM product
                    WHERE name ILIKE '%{product}%'
                    ORDER BY brand, price
                    ;
                '''
            )
            return pd.read_sql(sql=sql, con=conn)
        except Exception as e:
            raise Exception(f"Could not retrieve {product} from db. {e}")

    def update_product(self, conn, product) -> None:
        """ Update or add new product to database

        Parameters
        ----------
        conn: psycopg2.connector
            Database connector
        product: dict
            Product dictionary which must contain the following fields: name,
            brand, price, currency, img
        """
        cur = conn.cursor()
        cur.execute(
            '''
                INSERT INTO product (name, brand, price, currency, img)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE
                  SET brand = excluded.brand,
                      price = excluded.price,
                      currency = excluded.currency,
                      img = excluded.img
                ;
            ''',
            (product['name'], product['brand'], product['price'],
             product['currency'], product['img'])
        )
        conn.commit()
