DROP TABLE IF EXISTS column_info;
DROP TABLE IF EXISTS dbinfo;

CREATE TABLE dbinfo (id serial PRIMARY KEY, dbms varchar(255) NOT NULL, host varchar(255) NOT NULL, user_name varchar(255) NOT NULL, password varchar(255) NOT NULL, db_name varchar(255) NOT NULL, table_name varchar(255) NOT NULL UNIQUE, description varchar(255));
CREATE TABLE column_info (id serial PRIMARY KEY, table_id int REFERENCES dbinfo(id) ON DELETE CASCADE NOT NULL, number int, type varchar(255) NOT NULL, name varchar(255) NOT NULL, description varchar(255));

INSERT INTO dbinfo VALUES (1, 'postgresql', 'spacia.db.ics.keio.ac.jp', 'shu', 'shu', 'rta_databases', 'postal_code', 'Postal code data in Tokyo.');
INSERT INTO dbinfo VALUES (2, 'postgresql', 'spacia.db.ics.keio.ac.jp', 'shu', 'shu', 'rta_databases', 'stocks', 'Stock prices data on Dec. 20.');
INSERT INTO dbinfo VALUES (3, 'postgresql', 'spacia.db.ics.keio.ac.jp', 'shu', 'shu', 'rta_databases', 'stations', 'Stations data in Japan.');
INSERT INTO dbinfo VALUES (4, 'postgresql', 'spacia.db.ics.keio.ac.jp', 'shu', 'shu', 'rta_databases', 'lines', 'Lines data in Japan.');
INSERT INTO dbinfo VALUES (5, 'postgresql', 'spacia.db.ics.keio.ac.jp', 'shu', 'shu', 'rta_databases', 'station_joins', 'Station joins data in Japan.');
INSERT INTO dbinfo VALUES (6, 'postgresql', 'spacia.db.ics.keio.ac.jp', 'shu', 'shu', 'rta_databases', 'prefectures', 'Prefectures in Japan.');

INSERT INTO column_info VALUES (1, 1, 1, 'int', 'prefecture_code', 'Japanese local goverment code（JIS X0401、X0402）');
INSERT INTO column_info VALUES (2, 1, 2, 'int', 'old_code', 'Old postal code（5 number）');
INSERT INTO column_info VALUES (3, 1, 3, 'int', 'code', 'Postal code（7 number）');
INSERT INTO column_info VALUES (4, 1, 4, 'string', 'prefecture_name_kana', 'Prefecture name（katakana）');
INSERT INTO column_info VALUES (5, 1, 5, 'string', 'city_name_kana', 'City name（katakana）');
INSERT INTO column_info VALUES (6, 1, 6, 'string', 'address_kana', 'Address（katakana）');
INSERT INTO column_info VALUES (7, 1, 7, 'string', 'prefecture_name', 'Prefecture name');
INSERT INTO column_info VALUES (8, 1, 8, 'string', 'city_name', 'City name');
INSERT INTO column_info VALUES (9, 1, 9, 'string', 'address', 'Address');

INSERT INTO column_info VALUES(10, 2, 1, 'string', 'code', 'Security code');
INSERT INTO column_info VALUES(11, 2, 2, 'string', 'brand', 'Company name');
INSERT INTO column_info VALUES(12, 2, 3, 'string', 'market', 'Market name');
INSERT INTO column_info VALUES(13, 2, 4, 'decimal', 'opening_price', 'Opening price');
INSERT INTO column_info VALUES(14, 2, 5, 'decimal', 'high_price', 'High Price');
INSERT INTO column_info VALUES(15, 2, 6, 'decimal', 'low_price', 'Low price');
INSERT INTO column_info VALUES(16, 2, 7, 'decimal', 'ending_price', 'Ending price');
INSERT INTO column_info VALUES(17, 2, 8, 'int', 'turnover', 'Turnover');
INSERT INTO column_info VALUES(18, 2, 9, 'bigint', 'trading_value', 'Trading value');

INSERT INTO column_info VALUES(19, 3, 1, 'bigint', 'trading_value', 'Trading value');
