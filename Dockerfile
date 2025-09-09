FROM ruby:3.0.3

RUN apt-get update -qq && apt-get install -y \
    build-essential \
    default-mysql-client \
    redis-server \
    wget \
    gnupg \
    ca-certificates \
    curl \
    chromium \
    chromium-driver

# Node.js 18のインストール
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && node --version \
    && npm --version

# Playwrightのインストール
RUN npm install -g playwright@1.55.0 \
    && npx playwright install chromium \
    && npx playwright install-deps chromium

ENV APP_PATH=/myapp
WORKDIR $APP_PATH

# Bundler の保存先と PATH を固定
ENV BUNDLE_PATH=/usr/local/bundle
ENV GEM_HOME=/usr/local/bundle
ENV PATH="/usr/local/bundle/bin:${PATH}"

# 先に Gemfile を入れて bundle（キャッシュ用）
COPY Gemfile Gemfile.lock ./
RUN bundle install

# アプリ本体
COPY . .

# entrypoint
COPY entrypoint.sh /usr/bin/
RUN chmod +x /usr/bin/entrypoint.sh
ENTRYPOINT ["entrypoint.sh"]

EXPOSE 3000
CMD ["bundle", "exec", "rails", "server", "-b", "0.0.0.0", "-p", "3000"]
