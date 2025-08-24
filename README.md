# 環境構築の手順

1.クローンコマンド

```:ターミナル
git clone tamahome@tamahome.git.backlog.com:/SAP/jobcan-obic.git
```

# 昔の申請

・発注申請?

## 2.config/database.yml の develop と development_jtm_database を下記に修正

```yml
development:
  adapter: mysql2
  encoding: utf8mb4
  charset: utf8mb4
  collation: utf8mb4_general_ci
  reconnect: false
  database: jobcan_development
  username: root
  password: jobcan
  host: db
  pool: 5
  timeout: 10000

development_jtm_database:
  adapter: oracle_enhanced
  database: //10.201.0.5:1521/mukouka
  username: obictest
  password: obictesta
  # database: //10.201.0.22:1521/jtm
  # username: tamaobic
  # password: tamaobica
  # database: //10.201.0.22:1521/jtm
  # username: tamaobic
  # password: tamaobica
  # database: //10.201.0.15:1521/jtm
  # username: tamasap
  # password: tamasapa
  pool: 5
  timeout: 20000
```

コマンド

1.ビルド

```::ターミナル
docker-compose build
```

2.アップ

```::ターミナル
docker-compose up -d
```

3.db コンテナ内の入り方

```::ターミナル
docker-compose exec db bash
```

4.mysql 設定

```::ターミナル
mysql -u root -p

GRANT ALL PRIVILEGES ON _._ TO 'root'@'%' IDENTIFIED BY 'jobcan' WITH GRANT OPTION;

FLUSH PRIVILEGES;

SHOW DATABASES;

USE jobcan_development;
```

5.web コンテナ内の入り方

```::ターミナル
docker-compose exec api bash

# gemのエラーが出て再度bundle installしたい時
docker-compose run --no-deps --entrypoint bash web

bundle install
```

6.マイグレーション

```::ターミナル
rake db:migrate
```
