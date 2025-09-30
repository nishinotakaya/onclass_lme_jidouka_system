# 環境構築＆運用 README（jobcan-obic）

> **メモ**: このドキュメントは“全部コードブロック”で出力しています。  
> 内側の ``` や ~~~ の囲みはそのまま記述として読めます（レンダリングはしません）。

---

## 目次

- [0. 昔の申請（メモ）](#0-昔の申請メモ)
- [1. リポジトリ取得](#1-リポジトリ取得)
- [2. ローカル開発環境（Docker）](#2-ローカル開発環境docker)
  - [2.1 `config/database.yml` 設定](#21-configdatabaseyml-設定)
  - [2.2 ビルド & 起動](#22-ビルド--起動)
  - [2.3 DB コンテナ操作 & MySQL 設定](#23-db-コンテナ操作--mysql-設定)
  - [2.4 Web/API コンテナ操作](#24-webapi-コンテナ操作)
  - [2.5 マイグレーション](#25-マイグレーション)
  - [2.6 （Playwright/Chromium を使う場合のローカル準備）](#26-playwrightchromium-を使う場合のローカル準備)
- [3. Heroku（本番）](#3-heroku本番)
  - [3.1 リモート設定 & デプロイ](#31-リモート設定--デプロイ)
  - [3.2 推奨ビルドパック（Selenium/Chrome 運用想定）](#32-推奨ビルドパックseleniumchrome-運用想定)
  - [3.3 よく使うログの見方](#33-よく使うログの見方)
  - [3.4 よく使う運用コマンド](#34-よく使う運用コマンド)
- [4. トラブルシュート](#4-トラブルシュート)
  - [4.1 失敗ログの抜粋保存](#41-失敗ログの抜粋保存)
  - [4.2 Playwright を外して Selenium のみで運用する場合](#42-playwright-を外して-selenium-のみで運用する場合)
  - [4.3 メモリ（R14）/再起動（R15）対策のヒント](#43-メモリr14再起動r15対策のヒント)
- [付録：よく使う環境変数例](#付録よく使う環境変数例)

---

## 0. 昔の申請（メモ）

- 発注申請?（要確認・分類）

---

## 1. リポジトリ取得

**クローンコマンド**

```bash
git clone tamahome@tamahome.git.backlog.com:/SAP/jobcan-obic.git
cd jobcan-obic
```

> 既存プロジェクトを使う場合は適宜パスを読み替え。

---

## 2. ローカル開発環境（Docker）

### 2.2 ビルド & 起動

```bash
# Docker v2 以降の表記
docker compose build
docker compose up -d

# 旧バージョン互換（必要なら）
docker-compose build
docker-compose up -d
```

### 2.4 Web/API コンテナ操作

```bash
# Web / API コンテナに入る（compose の service 名に合わせて選択）
docker compose exec api bash
# or
docker compose exec web bash
# 旧: docker-compose exec api bash / docker-compose exec web bash

# Rails console
bundle exec rails c

# gem のエラーが出て再度 bundle install したい時
docker compose run --no-deps --entrypoint bash web
bundle install
# 旧: docker-compose run --no-deps --entrypoint bash web
```

### 2.5 マイグレーション

```bash
# プロジェクトに合わせていずれか
rake db:migrate
# または
bundle exec rails db:migrate
```

### 2.6 （Playwright/Chromium を使う場合のローカル準備）

> Selenium のみで動かすなら **不要**。Playwright の同梱 Chromium をローカルで使うときだけ実行。

```bash
# コンテナに入る（service 名に合わせて）
docker compose exec api bash
# もしくは:
# docker compose exec app bash

# 念のため lock を最新化して node_modules を作成
rm -rf node_modules package-lock.json
npm install

# Playwright 管理の Chromium をインストール（コンテナ内で！）
npx playwright install chromium
```

---

## 3. Heroku（本番）

> 例として環境変数 `$APP` を使うと便利です（例: `onclass-lme-jidouka-app`）。

```bash
export APP=onclass-lme-jidouka-app
```

### 3.1 リモート設定 & デプロイ

```bash
heroku git:remote -a $APP
git push heroku HEAD:main
```

**キャッシュ再作成が必要なケース**

```bash
# ビルドキャッシュを疑う時の再ビルドトリガ
git commit --allow-empty -m "rebuild after cache purge"
git push heroku main
```

### 3.2 推奨ビルドパック（Selenium/Chrome 運用想定）

```bash
heroku buildpacks:clear -a $APP
heroku buildpacks:add -a $APP heroku-community/apt
heroku buildpacks:add -a $APP https://github.com/heroku/heroku-buildpack-google-chrome
heroku buildpacks:add -a $APP https://github.com/heroku/heroku-buildpack-chromedriver
heroku buildpacks:add -a $APP heroku/ruby
# すでに設定済みならスキップ可
```

### 3.3 よく使うログの見方

**全体を追う**

```bash
heroku logs -a $APP --tail
```

**直近 N 行（例: 1000）**

```bash
heroku logs -a $APP -n 1000
```

**Web だけ**

```bash
heroku logs -a $APP --tail --dyno web.1
```

**Sidekiq Worker だけ**

```bash
heroku logs -a $APP --tail --dyno worker.1
```

**一時実行（run）のログ**

```bash
# 1) 全体を tail しておく（run の ID を拾いやすい）
heroku logs -a $APP --tail

# 2) 別タブで任意コマンドを実行
heroku run -a $APP -- bundle exec rails runner "Lme::LineInflowsWorker.new.perform"

# 3) run の ID が表示されたらピンポイントで追う
heroku logs -a $APP --tail --dyno run.XXXX
```

**タグやキーワードで絞り込み（grep）**

```bash
heroku logs -a $APP --tail | grep LmeLoginUserService
heroku logs -a $APP --tail | egrep "basic|XSRF|cookie|LineInflowsWorker"
```

**デプロイ直後の起動確認**

```bash
# アプリのログ
heroku logs -a $APP --tail --source app

# メモリ警告・再起動も見たいとき
heroku logs -a $APP --tail --source app --source heroku
```

**稼働中 dyno 確認**

```bash
heroku ps -a $APP
```

**ランタイムメトリクス（任意）**

```bash
heroku labs:enable log-runtime-metrics -a $APP
```

### 3.4 よく使う運用コマンド

```bash
# 再起動
heroku restart -a $APP

# Redis 接続先確認（例）
heroku config:get REDISCLOUD_TLS_URL -a $APP
```

---

## 4. トラブルシュート

### 4.1 失敗ログの抜粋保存

```bash
heroku logs -a $APP -n 1500 | tee /tmp/heroku.log
grep -n "LmeLoginUserService\|LineInflowsWorker" /tmp/heroku.log
```

### 4.2 Playwright を外して Selenium のみで運用する場合

- **Heroku**: Google Chrome / Chromedriver の buildpack を入れておけば Playwright のブラウザダウンロードは不要。
- **コード**: Playwright 呼び出し部分を外す or フィーチャフラグで無効化。
- **環境変数**: `GOOGLE_CHROME_BIN` / `WEBDRIVER_CHROME_DRIVER` は buildpack により自動設定（Cedar-14/Cedar-20/Cedar-22 の CFT では自動のことが多い）。
- **ジョブ並列**: メモリ節約のため `SIDEKIQ_CONCURRENCY=1` を推奨。

### 4.3 メモリ（R14）/再起動（R15）対策のヒント

- Headless Chrome 起動オプションに `--no-sandbox --disable-dev-shm-usage --disable-gpu` を付与。
- 同時起動ブラウザ/タブを抑制（Sidekiq の並列 1、処理の直列化）。
- 不要な Playwright のブラウザ DL を抑止（Selenium 運用であれば `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1` など）。
- 大きなページダンプや大量ログを控える。

---

## 付録：よく使う環境変数例

```bash
# Heroku で使う場合の例
export APP=onclass-lme-jidouka-app

# （Heroku の CFT Buildpack 運用時は自動で入ることが多い）
# echo で確認可能：
heroku run -a $APP -- bash -lc '
  echo "$GOOGLE_CHROME_BIN"; "$GOOGLE_CHROME_BIN" --version;
  echo "$WEBDRIVER_CHROME_DRIVER"; "$WEBDRIVER_CHROME_DRIVER" --version
'
```

---
