#!/usr/bin/env bash

read -p "Type MySQL root password: " -s password
echo

read -p "Type MySQL user password: " -s upassword
echo

function mysql_do() {
  mysql -uroot -p$password -e "$1" 2> >(grep -v "Using a password on") $2
}

for db in market_en market_ru; do

  mysql_do "CREATE DATABASE IF NOT EXISTS $db CHARACTER SET utf8"
  mysql_do "GRANT ALL PRIVILEGES ON market_ru.* TO market_app@localhost"

  mysql_do "$(cat <<-EOF
    CREATE TABLE IF NOT EXISTS users (
      user_id INT NOT NULL AUTO_INCREMENT,
      name VARCHAR(255),
      PRIMARY KEY (user_id)
    );
EOF
  )" $db

  mysql_do "$(cat <<-EOF
    CREATE TABLE IF NOT EXISTS sales (
      order_id INT NOT NULL AUTO_INCREMENT,
      user_id INT,
      order_amount FLOAT,
      PRIMARY KEY (order_id)
    );
EOF
  )" $db

done

mysql_do "SET PASSWORD FOR market_app@localhost = '$upassword'"
mysql_do "FLUSH PRIVILEGES"

# market_en
# insert into users(name) values ('Bob'), ('Alice'), ('Goblin')
# insert into sales(user_id, order_amount) values (1, 1000.2), (1, 234.67), (3, 130.12)

# market_ru
# insert into users(name) values ('Боб'), ('Алиса'), ('Гоблин');
# insert into sales(user_id, order_amount) values (1, 10), (1, 20), (2, 10.5), (3, 100.12), (3, 200.123)
