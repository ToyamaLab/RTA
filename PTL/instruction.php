<? include 'header.php'; ?>

<div class="container">
  <div class="row">
    <div class="col-lg-8 col-lg-offset-2">

      <h1 class="page-header">Instructions for RTA</h1>

      <h2 class="page-header">Installation &amp; Settings</h2>


      <p>Access This link <a href="./data/RTA.zip">(Download)</a> and get RTA.zip, and execute the following commands.</p>


<pre>
$ cd (Downloaded Directory)
$ unzip RTA.zip
$ cd RTA
$ bin/rta_setup
</pre>

      <p>
        Then, a configuration file <strong>.rta</strong> is created in your home directory and alias <strong>rta</strong> is written in your .bashrc.<br>
        If the alias isn't written, edit .bashrc manually as follows.
      </p>

<pre>
$ vi ~/.bashrc

~
alias rta='(Downloaded Directory)/RTA/bin/rta'

$ source ~/.bashrc
</pre>

      <p>Edit file <strong>.rta</strong> as below to configure RTA.</p>

<pre>
$ vi ~/.rta

driver=mysql        // (mysql | postgresql)
host=localhost      
user=user           
password=password
db=dbname           // Local database name to use as local
tmp_db=tmp          // Tmp database name to save result table
</pre>

      <p>After this, command rta will work. Input your RTA query. RTA query must end with ; like normal SQL.</p>

<pre>
$ rta
rta> (Input RTA query)
</pre>

      <p>Example)</p>

<pre>
$ rta
rta> SELECT u.id, u.name, u.postal_code, p.prefecture_name, p.city_name, p.address
  -> FROM users u, #postal_code p
  -> WHERE u.postal_code = p.code;
</pre>

      <p>If you want to quit RTA, enter \q.</p>

<pre>
rta> \q
$
</pre>

      <h2 class="page-header">Usage</h2>
      <p>If you want to try RTA instantly, you can download and insert these sample tables and execute following queries.</p>
      <p><a href="./data/sample_mysql.sql" download="rta_sample.sql">MySQL Version</a> / <a href="./data/sample_psql.sql" download="rta_sample.sql">PostgreSQL Version</a></p>
      <dl>
        <dt>SQL 1</dt>
        <dd>
<pre>
SELECT u.id, u.name, u.postal_code, p.prefecture_name, p.city_name, p.address
FROM users u, #postal_code p
WHERE u.postal_code = p.code;
</pre>
        </dd>
      </dl>
      <dt>SQL 2</dt>
        <dd>
<pre>
SELECT u.id, u.name, SUM (us.number * s.ending_price)
FROM users u, user_stocks us, #stocks s
WHERE u.id = us.user_id AND us.code = s.code GROUP BY u.id, u.name;
</pre>
        </dd>
      </dl>
      <p>And other many queries!</p>

      <h2 class="page-header">Table Registration</h2>
      <p>If you want to register your own table to PTL, access <a href="./register.php">Resiter Page</a> and input your table info. To register miltiple tables at once input tables separated by commas like "table1,table2,...". Please don't register writable DB user, but readonly user.</p>
      <img src="./img/table_registration.png" alt="Table Registration" class="img-responsive">
      <p>If inputed table is available, input addituinal info（Table escription, Each column Descriptions） to the next page. You normaly don't need to edit "Access Name".
      <img src="./img/column_registration.png" alt="Column Registration" class="img-responsive">

    </div>
  </div>
</div>

<? include 'footer.php'; ?>
