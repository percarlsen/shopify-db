-- create necessary tables
CREATE TABLE IF NOT EXISTS customers (
    id BIGINT,
    email VARCHAR(50),
    name VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    phone VARCHAR(20),
    address VARCHAR(150),
    city VARCHAR(150),
    zip VARCHAR(20),
    country VARCHAR(150),
    total_spent DECIMAL,
    verified_email BOOLEAN,
    note VARCHAR(150),
    accepts_marketing BOOLEAN,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS orders (
    id BIGINT NOT NULL,
    customer_id BIGINT,
    name VARCHAR(10) NOT NULL,
    fulfillment_status VARCHAR(20),
    financial_status VARCHAR(50),
    total_price DECIMAL NOT NULL,
    total_line_items_price DECIMAL,
    total_discounts_amount DECIMAL,
    total_tax_amount DECIMAL,
    taxes_included BOOLEAN,
    currency VARCHAR(10),
    created_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY(id),
    CONSTRAINT fk_customer
        FOREIGN KEY(customer_id)
        REFERENCES customers(id)
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS discounts (
    id SERIAL PRIMARY KEY,
    order_id BIGINT,
    discount_code VARCHAR(50),
    discount_type VARCHAR(20),
    discount_value DECIMAL,
    discount_value_type VARCHAR(20),
    CONSTRAINT fk_order
        FOREIGN KEY(order_id)
            REFERENCES orders(id)
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS products (
    id BIGINT NOT NULL,
    title VARCHAR(100),
    status VARCHAR(20),
    product_type VARCHAR(50),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    vendor VARCHAR(50),
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS product_variants (
    id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    price DECIMAL,
    title VARCHAR(100),
    sku VARCHAR(20),
    option1 VARCHAR(20),
    option2 VARCHAR(20),
    option3 VARCHAR(20),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY(id),
    CONSTRAINT fk_product
        FOREIGN KEY(product_id)
            REFERENCES products(id)
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS line_item_products (
    id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    product_id BIGINT,
    title VARCHAR(150) NOT NULL,
    sku VARCHAR(20),
    unit_price DECIMAL NOT NULL,
    total_price DECIMAL NOT NULL,
    total_discount_amount DECIMAL,
    quantity INT NOT NULL,
    vendor VARCHAR(50),
    variant_title VARCHAR(50),
    tax_amount DECIMAL,
    tax_rate DECIMAL,
    tax_title VARCHAR(20),
    taxable BOOLEAN,
    currency VARCHAR(10),
    PRIMARY KEY(id),
    CONSTRAINT fk_order
        FOREIGN KEY(order_id)
            REFERENCES orders(id)
            ON DELETE CASCADE,
    CONSTRAINT fk_product
        FOREIGN KEY(product_id)
            REFERENCES products(id)
            ON DELETE SET DEFAULT
);

CREATE TABLE IF NOT EXISTS transactions (
    id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    status VARCHAR(20),
    amount DECIMAL,
    currency VARCHAR(10),
    error_code VARCHAR(20),
    gateway VARCHAR(20),
    kind VARCHAR(20),
    created_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY(id),
    CONSTRAINT order_id
        FOREIGN KEY(order_id)
            REFERENCES orders(id)
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS shipping (
    id BIGINT NOT NULL,
    order_id BIGINT,
    code VARCHAR(150),
    price DECIMAL NOT NULL,
    discounted_price DECIMAL,
    currency VARCHAR(10),
    title VARCHAR(150),
    source VARCHAR(150),
    phone VARCHAR(20),
    address VARCHAR(150),
    city VARCHAR(150),
    zip VARCHAR(20),
    country VARCHAR(150),
    latitude DECIMAL,
    longitude DECIMAL,
    PRIMARY KEY(id),
    CONSTRAINT fk_order
        FOREIGN KEY(order_id)
            REFERENCES orders(id)
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS refunds (
    id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    transaction_id BIGINT NOT NULL,
    note VARCHAR(200),
    refunded_product_cnt INT,
    created_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY(id),
    CONSTRAINT fk_order
        FOREIGN KEY(order_id)
            REFERENCES orders(id)
            ON DELETE CASCADE,
    CONSTRAINT fk_transaction
        FOREIGN KEY(transaction_id)
            REFERENCES transactions(id)
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS line_item_product_refunds (
    id BIGINT NOT NULL,
    refund_id BIGINT NOT NULL,
    line_item_product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    currency VARCHAR(10),
    refund_amount DECIMAL,
    PRIMARY KEY(id),
    CONSTRAINT fk_refund
        FOREIGN KEY(refund_id)
            REFERENCES refunds(id)
            ON DELETE CASCADE,
    CONSTRAINT fk_line_item
        FOREIGN KEY(line_item_product_id)
            REFERENCES line_item_products(id)
            ON DELETE CASCADE
);

CREATE VIEW tripletex_invoice AS (
    WITH success_transaction_payments AS ( -- rank successful transactions pr order after the most 'significant' transaction
        SELECT
            t.*,
            ROW_NUMBER() OVER(
                PARTITION BY t.order_id
                ORDER BY (
                    CASE t.kind
                        WHEN 'sale' THEN 1
                        WHEN 'capture' THEN 2
                        WHEN 'authorization' THEN 3
                        ELSE 10
                    END)
            ) AS transaction_rank
        FROM transactions t
        WHERE
            t.status = 'success'
            AND t.kind IN ('sale', 'capture', 'authorization')
            AND t.gateway != 'gift_card'
        ORDER BY (
            t.order_id
        )
    ),
    gift_card_lines AS (
        SELECT
            t.id AS transaction_id,
            o.id AS order_id,
            'payment' AS payment_tag,
            CAST(RIGHT(CAST(c.id AS CHAR(12)), 9) AS INT) AS "CUSTOMER NO", -- Tripletex only allows 9 digits as ID. Very unlikely that we will get any duplicates here
            c.name AS "CUSTOMER NAME",
            o.name AS "ORDER NO",
            stp.amount AS "PAID AMOUNT",
            1 AS "ORDER LINE - COUNT", -- only 1x gift card pr transaction
            'Gift card' AS "ORDER LINE - PROD NAME",
            -t.amount AS "ORDER LINE - UNIT PRICE",
            0 AS "ORDER LINE - DISCOUNT",
            3 AS "ORDER LINE - VAT CODE", -- VAT is always 25% (code 3)
            NULL::TEXT AS "ORDER LINE - DESCRIPTION", -- No description needed
            'GIFTCARD' AS "ORDER LINE - PROD NO",
            stp.gateway AS "PAYMENT TYPE",
            DATE(t.created_at) AS "INVOICE DATE",
            DATE(t.processed_at) AS "DELIVERY DATE",
            DATE(t.created_at) AS "ORDER DATE",
            DATE(t.processed_at) AS "DUE DATE",
            1 AS rank, -- dummy, needed to match shipping_lines cols
            4 AS priority
        FROM transactions t
        LEFT JOIN orders o
            ON o.id = t.order_id
        LEFT JOIN customers c
            ON c.id = o.customer_id
        LEFT JOIN success_transaction_payments stp
            ON stp.order_id = t.order_id
        WHERE t.gateway = 'gift_card' AND stp.transaction_rank = 1
    ),
    product_lines AS (
        SELECT
            t.id as transaction_id,
            o.id as order_id,
            'payment' AS payment_tag,
            CAST(RIGHT(CAST(c.id AS CHAR(12)), 9) AS INT) AS "CUSTOMER NO",
            c.name AS "CUSTOMER NAME",
            o.name AS "ORDER NO",
            t.amount AS "PAID AMOUNT",
            lip.quantity AS "ORDER LINE - COUNT",
            CASE
                WHEN NULLIF(lip.title, '') IS NOT NULL AND NULLIF(lip.variant_title, '') IS NOT NULL THEN CONCAT(lip.title, ' - ', lip.variant_title)
                WHEN lip.title IS NOT NULL THEN lip.title
                ELSE NULL
            END "ORDER LINE - PROD NAME",
            lip.unit_price AS "ORDER LINE - UNIT PRICE",
            100 * (1 - ((lip.total_price - lip.total_discount_amount) / NULLIF(lip.total_price, 0))) AS "ORDER LINE - DISCOUNT",
            3 AS "ORDER LINE - VAT CODE", -- VAT is always 25% (code 3)
            NULL::TEXT AS "ORDER LINE - DESCRIPTION", -- No description needed
            lip.sku::TEXT AS "ORDER LINE - PROD NO",
            t.gateway AS "PAYMENT TYPE",
            DATE(t.created_at) AS "INVOICE DATE",
            DATE(t.processed_at) AS "DELIVERY DATE",
            DATE(t.created_at) AS "ORDER DATE",
            DATE(t.processed_at) AS "DUE DATE",
            1 AS rank, -- dummy, needed to match shipping_lines cols
            1 AS priority

        FROM success_transaction_payments t
        LEFT JOIN orders o
            ON o.id = t.order_id
        LEFT JOIN customers c
            ON c.id = o.customer_id
        LEFT JOIN discounts d
            ON d.order_id = o.id
        LEFT JOIN line_item_products lip
            ON lip.order_id = o.id
        WHERE
            t.transaction_rank = 1 -- only want the most significant transaction. Avoid duplicates cause by other transaction events
    ),
    refund_lines AS (
        SELECT
            t.id as transaction_id,
            o.id as order_id,
            'refund' AS payment_tag,
            CAST(RIGHT(CAST(c.id AS CHAR(12)), 9) AS INT) AS "CUSTOMER NO",
            c.name AS "CUSTOMER NAME",
            CONCAT(o.name, '-1') AS "ORDER NO",
            -COALESCE(lipr.refund_amount, t.amount) AS "PAID AMOUNT",
            -COALESCE(lipr.quantity, 1) AS "ORDER LINE - COUNT",
            CASE
                WHEN lip.title IS NOT NULL THEN CONCAT(lip.title, ' - ', lip.variant_title)
                ELSE NULL --'Refund'
            END "ORDER LINE - PROD NAME",
            COALESCE(ROUND(lipr.refund_amount/lipr.quantity, 2), t.amount) AS "ORDER LINE - UNIT PRICE",
            0 AS "ORDER LINE - DISCOUNT", -- never discount on refunds
            3 AS "ORDER LINE - VAT CODE", -- VAT is always 25% (code 3)
            COALESCE(NULLIF(r.note, ''), 'Refund with unspecified reason') AS "ORDER LINE - DESCRIPTION",
            lip.sku::text AS "ORDER LINE - PROD NO",
            t.gateway AS "PAYMENT TYPE",
            DATE(r.created_at) AS "INVOICE DATE",
            DATE(r.processed_at) AS "DELIVERY DATE",
            DATE(r.created_at) AS "ORDER DATE",
            DATE(r.processed_at) AS "DUE DATE",
            1 AS rank, -- needed to match shipping_lines cols
            2 AS priority

        FROM transactions t
        INNER JOIN refunds r
            ON r.transaction_id = t.id
        LEFT JOIN line_item_product_refunds lipr
            ON lipr.refund_id = r.id
        LEFT JOIN orders o
            ON o.id = t.order_id
        LEFT JOIN customers c
            ON c.id = o.customer_id
        LEFT JOIN discounts d
            ON d.order_id = o.id
        LEFT JOIN line_item_products lip
            ON lip.order_id = r.order_id AND lip.id = lipr.line_item_product_id
        WHERE
            t.status = 'success' AND t.kind = 'refund'
    ),
    shipping_lines AS (
        SELECT
            pl.transaction_id,
            pl.order_id,
            'payment' AS payment_tag,
            pl."CUSTOMER NO",
            pl."CUSTOMER NAME",
            pl."ORDER NO",
            pl."PAID AMOUNT",
            1 AS "ORDER LINE - COUNT", -- Always only 1x shipping
            null::text AS "ORDER LINE - PROD NAME",
            s.price AS "ORDER LINE - UNIT PRICE",
            COALESCE(100 * (1 - (s.discounted_price / NULLIF(s.price, 0))), 0) AS "ORDER LINE - DISCOUNT",
            3 AS "ORDER LINE - VAT CODE", -- Shipping VAT code is always 3
            s.title AS "ORDER LINE - DESCRIPTION",
            'SHIPPING' AS "ORDER LINE - PROD NO",
            pl."PAYMENT TYPE",
            pl."INVOICE DATE",
            pl."DELIVERY DATE",
            pl."ORDER DATE",
            pl."DUE DATE",
            ROW_NUMBER() OVER(PARTITION BY pl.order_id ORDER BY pl."INVOICE DATE") AS rank,
            3 AS priority
        FROM product_lines as pl
        INNER JOIN shipping s
            ON s.order_id = pl.order_id
        WHERE rank = 1
    )
    SELECT
        transaction_id,
        order_id,
        payment_tag,
        "CUSTOMER NO",
        "CUSTOMER NAME",
        "ORDER NO",
        round("PAID AMOUNT"::numeric, 2) AS "PAID AMOUNT",
        "ORDER LINE - COUNT",
        "ORDER LINE - PROD NAME",
        round("ORDER LINE - UNIT PRICE"::numeric, 2) AS "ORDER LINE - UNIT PRICE",
        round("ORDER LINE - DISCOUNT"::numeric, 2) AS "ORDER LINE - DISCOUNT",
        "ORDER LINE - VAT CODE",
        "ORDER LINE - DESCRIPTION",
        "ORDER LINE - PROD NO",
        "PAYMENT TYPE",
        "INVOICE DATE",
        "DELIVERY DATE",
        "ORDER DATE",
        "DUE DATE"
    FROM (
        SELECT *
        FROM product_lines
        UNION
        SELECT *
        FROM refund_lines
        UNION
        SELECT *
        FROM shipping_lines
        UNION
        SELECT *
        FROM gift_card_lines
    ) AS invoice_lines
    WHERE rank = 1
    ORDER BY
        "ORDER DATE" DESC, order_id, "CUSTOMER NAME", priority
);

CREATE VIEW tripletex_customer_map AS (
    SELECT
        id AS shopify_id,
        CAST(RIGHT(CAST(id AS CHAR(12)), 9) AS INT) AS tripletex_id,
        name AS name,
        phone AS phone,
        email AS email
    FROM customers
);
