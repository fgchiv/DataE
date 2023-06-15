create table ypf(
open_price decimal(6,3) not null,
high_price decimal(6,3) not null,
low_price decimal(6,3) not null,
close_price decimal(6,3) not null,
adj_close_price decimal(6,3) not null,
volume integer not null,
dividend_amount decimal(4,3) not null,
split_coef decimal(3,3) not null,
fecha date not null,
primary key(fecha))
distkey(fecha)
sortkey(fecha);