CREATE TABLE user_stocks (
  user_id int NOT NULL,
  code varchar NOT NULL,
  number int NOT NULL
);

INSERT INTO user_stocks (user_id, code, number)
VALUES
	(6,'7203-T',100),
	(6,'6753-T',2000),
	(14,'4661-T',300),
	(2,'2432-T',200),
	(10,'3938-T',300),
	(3,'3287-T',10),
	(8,'1434-T',100),
	(11,'1828-T',50),
	(8,'2345-T',100),
	(7,'2499-T',20),
	(15,'2668-T',40),
	(13,'6042-T',60),
	(8,'6237-T',20),
	(12,'6772-T',100),
	(15,'6982-T',30),
	(1,'7544-T',200),
	(7,'7203-T',100),
	(9,'9708-T',2000),
	(1,'4661-T',300),
	(12,'2432-T',200),
	(11,'3938-T',300),
	(13,'3287-T',10),
	(10,'1434-T',100),
	(2,'1828-T',50),
	(3,'2345-T',100),
	(9,'2499-T',20),
	(2,'2668-T',40),
	(3,'6042-T',60),
	(1,'6237-T',20),
	(4,'6772-T',100),
	(4,'6982-T',30),
	(5,'7544-T',200),
	(14,'9873-T',2000),
	(6,'9537-T',50),
	(4,'8912-T',1000),
	(11,'7965-T',500);

CREATE TABLE users (
  id serial,
  name varchar NOT NULL,
  postal_code int DEFAULT NULL,
  PRIMARY KEY (id)
);

INSERT INTO users (id, name, postal_code)
VALUES
	(1,'村上',1160003),
	(2,'佐藤',1340083),
	(3,'田中',1810001),
	(4,'山田',1030013),
	(5,'中山',1070062),
	(6,'小坂',1920151),
	(7,'中井',2070015),
	(8,'新山',1030028),
	(9,'田嶋',1820004),
	(10,'金丸',1210836),
	(11,'大島',1600014),
	(12,'遠山',1840013),
	(13,'日比',1040054),
	(14,'小林',1980041),
	(15,'斎藤',1860001);
