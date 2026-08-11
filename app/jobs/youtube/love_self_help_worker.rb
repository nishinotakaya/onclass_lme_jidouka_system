# app/jobs/youtube/love_self_help_worker.rb
#
# 恋愛系・自己啓発系 YouTube チャンネルのリサーチ用ワーカー。
# 取得・整形・シート書き込みのロジックは Youtube::CompetitorWorker と全く同じで、
# 「調べる対象チャンネル」と「出力先シート」だけを差し替えている。
class Youtube::LoveSelfHelpWorker < Youtube::CompetitorWorker
  include Sidekiq::Worker
  sidekiq_options queue: "youtube_competitors"

  # 2026-08-11 リサーチ。YouTube のチャンネル検索（恋愛/自己啓発系キーワード 45 本）で
  # 候補を洗い出し、各チャンネルの RSS フィードで「直近 4 ヶ月以内に投稿がある活動中チャンネル」
  # だけを残したもの。括弧内は調査時点の登録者数。
  CHANNELS = [
    # ---- 恋愛系（恋愛相談・恋愛心理・モテ/男磨き）----
    { name: "ジョージ -メンズコーチ-",                    url: "https://www.youtube.com/@coachjoji" },              # 46.5万
    { name: "リョータのイケメン製作所",                   url: "https://www.youtube.com/@ryota_ikemen" },           # 38.1万
    { name: "みなこの圧倒的モテ男TV",                     url: "https://www.youtube.com/@moteotv" },                # 35.1万
    { name: "モテ期プロデューサー荒野",                   url: "https://www.youtube.com/@moteki" },                 # 30.8万
    { name: "ゆとりモンスターズ【ゆとモン】",             url: "https://www.youtube.com/@ytmn" },                   # 26.9万
    { name: "【魅力の大学】by 恋愛屋ジュン",              url: "https://www.youtube.com/@attractionuniv" },         # 25.1万
    { name: "ゆりチャンネル",                             url: "https://www.youtube.com/@rennai-yuri" },            # 18.7万
    { name: "ノブ太のプロデュース。",                     url: "https://www.youtube.com/@nobuta_produce" },         # 14.2万
    { name: "マミ先生おとこの恋愛ガチレッスン",           url: "https://www.youtube.com/@mamisensei" },             # 9.6万
    { name: "チコの恋愛大学",                             url: "https://www.youtube.com/@Chiko_renaidaigdku" },     # 9.47万
    { name: "カムロの恋愛心理学",                         url: "https://www.youtube.com/@kamuro-love" },            # 8.57万
    { name: "プラス7%の恋愛法則",                         url: "https://www.youtube.com/@7-gm1mi" },                # 7.63万
    { name: "JURIのモテ男くん養成チャンネル",             url: "https://www.youtube.com/@jurich6936" },             # 7.1万
    { name: "だまされない女のつくり方 / 藤本シゲユキ",    url: "https://www.youtube.com/@damasarenai" },            # 6.32万
    { name: "小川健次@大人の恋愛講座",                    url: "https://www.youtube.com/@ogawakenji" },             # 5.84万
    { name: "恋愛講師が教える「恋愛のやり方」-光太-",     url: "https://www.youtube.com/@koutamcbrown" },           # 5.53万
    { name: "垢抜け大学┃ヒロキ",                          url: "https://www.youtube.com/@akanukeuniv" },            # 4.52万
    { name: "ゆうの恋愛相談室",                           url: "https://www.youtube.com/@Yuuu-rs" },                # 2.78万
    { name: "きよ【カップル恋愛相談室 男性心理】",        url: "https://www.youtube.com/@Kiyo.renai.souansitu" },   # 1.76万
    { name: "恋愛の神チャンネル",                         url: "https://www.youtube.com/@asd_elegant" },            # 1.6万
    { name: "やまもと兄弟 -大人の恋愛講座-",              url: "https://www.youtube.com/@ryukke" },                 # 1.42万
    { name: "fumiki恋愛塾",                               url: "https://www.youtube.com/@fumiki358" },              # 1.17万

    # ---- 婚活・結婚相談所・夫婦問題 ----
    { name: "来島美幸の婚活チャンネル",                   url: "https://www.youtube.com/@presia_01" },              # 16.6万
    { name: "ナレソメ予備校【公式】",                     url: "https://www.youtube.com/@naresome_yobiko" },        # 11.2万
    { name: "植草美幸の結婚相談所マリーミー",             url: "https://www.youtube.com/@uekusamiyuki" },           # 9.5万
    { name: "不倫解決カウンセラー 河村陽子",              url: "https://www.youtube.com/@furinkaikethu" },          # 5.81万
    { name: "アラサー男性の婚活戦略 by メンズリフト",     url: "https://www.youtube.com/@mens.marriage.strategy" }, # 5.08万
    { name: "マッチングアプリ専門家さき",                 url: "https://www.youtube.com/@matchappsaki" },           # 4.89万
    { name: "絶対結婚ちゃんねる",                         url: "https://www.youtube.com/@100kekkon" },              # 4.18万

    # ---- 自己啓発・ビジネス書解説・本要約 ----
    { name: "両学長 リベラルアーツ大学",                  url: "https://www.youtube.com/@ryogakucho" },             # 991万
    { name: "中田敦彦のYouTube大学",                      url: "https://www.youtube.com/@NKTofficial" },            # 548万
    { name: "メンタリスト DaiGo",                         url: "https://www.youtube.com/@mentalistdaigo" },         # 221万
    { name: "街録ch〜あなたの人生、教えて下さい〜",       url: "https://www.youtube.com/@gairokuch" },              # 190万
    { name: "本要約チャンネル【毎日18時更新】",           url: "https://www.youtube.com/@youyaku" },                # 180万
    { name: "フェルミ漫画大学",                           url: "https://www.youtube.com/@ferumi" },                 # 139万
    { name: "岡田斗司夫",                                 url: "https://www.youtube.com/@toshiookada0701" },        # 123万
    { name: "鴨頭嘉人",                                   url: "https://www.youtube.com/@kamohappy" },              # 105万
    { name: "学識サロン",                                 url: "https://www.youtube.com/@gakushikisaron" },         # 96.2万
    { name: "サラタメさん",                               url: "https://www.youtube.com/@salatame" },               # 82.5万
    { name: "大愚和尚の一問一答",                         url: "https://www.youtube.com/@osho_taigu" },             # 76.6万
    { name: "本要約チャンネル【毎日12時更新】",           url: "https://www.youtube.com/@abst" },                   # 66.1万
    { name: "サムの本解説ch",                             url: "https://www.youtube.com/@sam-book" },               # 58.1万
    { name: "西野亮廣",                                   url: "https://www.youtube.com/@akihironishino" },         # 52.8万
    { name: "アバタロー",                                 url: "https://www.youtube.com/@Aba_Book_Tuber" },         # 52.5万
    { name: "ユースフル / 実務変革のプロ",                url: "https://www.youtube.com/@youseful_skill" },         # 47.4万
    { name: "たろにぃ / 毎朝6時半 朝活生配信",            url: "https://www.youtube.com/@taronii" },                # 41.8万
    { name: "ハック大学",                                 url: "https://www.youtube.com/@hack-univ" },              # 28.6万
    { name: "Utsuさん",                                   url: "https://www.youtube.com/@3utsu" },                  # 23.9万
    { name: "研修トレーナー伊庭正康のスキルアップch",     url: "https://www.youtube.com/@m-iba" },                  # 23.1万
    { name: "内田博史【金持ちの習慣】",                   url: "https://www.youtube.com/@uchida-hiroshi" },         # 22.2万
    { name: "Ryuの自己管理室",                            url: "https://www.youtube.com/@ryu_potex" },              # 16.6万
    { name: "越川慎司のトップ5%仕事術",                   url: "https://www.youtube.com/@Koshiylou" },              # 12.9万
    { name: "青木仁志の『人生経営哲学』",                 url: "https://www.youtube.com/@satoshi-aoki" },           # 11.7万
    { name: "自己肯定感アニキ 津田紘彰",                  url: "https://www.youtube.com/@tsudahiroaki" },           # 8.64万
    { name: "Daikiさん。「自分磨き」と「自己啓発」",      url: "https://www.youtube.com/@Daiki-vv5mi" },            # 7.46万
    { name: "七瀬アリーサ【大人の勉強ch】",               url: "https://www.youtube.com/@ArisaNanase" },            # 5.69万

    # ---- メンタル・心理・潜在意識 ----
    { name: "精神科医がこころの病気を解説するCh",         url: "https://www.youtube.com/@masudatherapy" },          # 74万
    { name: "精神科医・樺沢紫苑の樺チャンネル",           url: "https://www.youtube.com/@kabasawa3" },              # 70.3万
    { name: "心理カウンセラー・ラッキー",                 url: "https://www.youtube.com/@lucky-panda" },            # 65.3万
    { name: "幸せと自己実現の心理学 / 野口嘉則",          url: "https://www.youtube.com/@y_noguchi" },              # 29.8万
    { name: "心理カウンセラーmasa",                       url: "https://www.youtube.com/@masa0358" },               # 28.9万
    { name: "ココヨワチャンネル",                         url: "https://www.youtube.com/@cocoyowa" },               # 24.9万
    { name: "生活に役立つメンタルヘルス",                 url: "https://www.youtube.com/@mentalhealthforlife" },    # 18.2万
    { name: "潜在意識OS【BAZZI】",                        url: "https://www.youtube.com/@bazzi8" },                 # 16.2万
    { name: "サイトウさんの毎日心理学チャンネル",         url: "https://www.youtube.com/@yusuke_counselor" },       # 14.1万
    { name: "精神科医さわの幸せの処方箋",                 url: "https://www.youtube.com/@CocoroDr_Sawa" },          # 13.5万
    { name: "思考の学校【潜在意識・引き寄せ】大石洋子",   url: "https://www.youtube.com/@yokoooishi" },             # 12.7万
    { name: "心理学研究所【ゆっくり解説】",               url: "https://www.youtube.com/@read-the-mind" },          # 9.33万
    { name: "精神科医Tomyの人生クリニック",               url: "https://www.youtube.com/@PdoctorTomy" },            # 7.14万
    { name: "潜在意識の学校",                             url: "https://www.youtube.com/@school-of-subconscious" }, # 5.55万
    { name: "アドラーくん",                               url: "https://www.youtube.com/@adlerkun" },               # 4.61万
    { name: "たかの / 心理学YouTuber",                    url: "https://www.youtube.com/@YusukeTakano" }            # 3.79万
  ].freeze

  private

  def default_competitors
    CHANNELS
  end

  def default_spreadsheet_id
    ENV["YOUTUBE_LOVE_SELF_HELP_SPREADSHEET_ID"] ||
      raise("YOUTUBE_LOVE_SELF_HELP_SPREADSHEET_ID が設定されていません")
  end

  def default_sheet_name
    ENV["YOUTUBE_LOVE_SELF_HELP_SHEET_NAME"] || "恋愛系_自己啓発系"
  end
end
