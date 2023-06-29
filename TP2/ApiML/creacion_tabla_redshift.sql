create table if not exists products_filtered(
    id varchar(50) not null,
    title varchar(256) not null,
    condition varchar(50) not null,
    currency_id varchar(50) not null,
    price float8 not null,
    original_price float8,
    installments_quantity integer not null,
    installments_rate float8 not null,
    tags varchar(500),
    attributes varchar(500),
    seller_id varchar(50) not null,
    seller_nickname varchar(100) not null,
    address_state_id varchar(50) not null,
    address_state_name varchar(100) not null,
    seller_reputation varchar(50),
    shipping_pick_up boolean not null,
    shipping_free boolean not null,
    shipping_logistic_type varchar(50) not null,
    search_date date not null,
    search_terms varchar(256) not null,
    product_category varchar(50) not null,
    product_name varchar(100) not null)
    distkey(search_terms)
    sortkey(search_terms, search_date)
;

create table if not exists price_analysis(
    search_date date not null,
    search_terms varchar(256) not null,
    count integer not null,
    mean float8 not null,
    median float8 not null,
    min float8 noy null)
    distkey(search_terms)
    sortkey(search_terms, search_date)
;