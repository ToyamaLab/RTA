DROP TABLE stocks;
CREATE TABLE stocks (code varchar, brand varchar, market varchar, opening_price decimal, high_price decimal, low_price decimal, ending_price decimal, turnover int, trading_value bigint);
\copy stocks from '~/workspace/stocks_utf8.csv' WITH CSV;