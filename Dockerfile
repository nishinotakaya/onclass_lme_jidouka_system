FROM ruby:3.0.3

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    xdg-utils \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Node.js 18 のインストール
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && node --version \
    && npm --version

# Playwright のインストール
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

# 👇 xvfb-run を噛ませて Rails 起動
CMD ["xvfb-run", "-a", "bundle", "exec", "rails", "server", "-b", "0.0.0.0", "-p", "3000"]
