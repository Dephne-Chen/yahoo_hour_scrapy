import re
import time
import scrapy
from datetime import datetime, timezone, timedelta
from scrapy_selenium import SeleniumRequest
from scrapy import Selector

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException


class YahooHourSpider(scrapy.Spider):
    name = "yahoo_hour"
    allowed_domains = ["tw.news.yahoo.com"]
    start_urls = ["https://tw.news.yahoo.com/archive/"]

    def parse_iso_utc(self, s: str):
        if not s:
            raise ValueError("empty datetime")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)

    def start_requests(self):
        yield SeleniumRequest(
            url=self.start_urls[0],
            callback=self.parse,
            wait_time=1.0,
        )

    def parse(self, response):
        driver = response.meta.get("driver")
        driver.set_window_size(1920, 4000)  # 讓 headless 也能觸發懶載入

        # 狀態/常數
        CARD_SEL = "h3 a[href$='.html']"
        seen = set() # 去重：已經排過隊（yield）的文章 URL 就不要重複送
        wait = WebDriverWait(driver, 4)
        self.hit_older = False

        GLOBAL_DEADLINE = time.time() + 90     # 全域最長 90 秒
        SCROLL_CAP = 400                       # 另一層保險絲
        STALLS_LIMIT = 3                       # 連續等不到新內容就停
        stalls = 0

        # ── 小工具：讀卡片數/最後 href（對付虛擬化列表）──────────────
        def get_card_count(d):
            return d.execute_script(
                "return document.querySelectorAll(arguments[0]).length;", CARD_SEL
            )

        def get_last_href(d):
            return d.execute_script(
                "const els=document.querySelectorAll(arguments[0]);"
                "return els.length ? els[els.length-1].getAttribute('href') : null;",
                CARD_SEL,
            )

        # ── 收集目前可見卡片；同時從列表讀「X 小時前」做停條件 ───────
        HOURS_RE = re.compile(r"(約\s*)?(\d+)\s*小時前")  # 吃「約 1 小時前」等寫法

        def collect_visible():
            nonlocal seen
            sel = Selector(text=driver.page_source)
            added = 0

            for a in sel.css(CARD_SEL):
                href = a.attrib.get("href")
                title = (a.css("::text").get() or "").strip()
                if not href or not title:
                    continue

                # 找離這張卡最近的 meta 列，抽相對時間
                rel_txt = a.xpath(
                    "ancestor::*[self::article or self::li or self::div][1]"
                    "//div[contains(., '分鐘前') or contains(., '小時前')][1]//text()"
                ).getall()
                rel_txt = " ".join(t.strip() for t in rel_txt if t.strip())

                m = HOURS_RE.search(rel_txt)
                if m:
                    hours = int(m.group(2))
                    if hours >= 1:
                        self.hit_older = True  # 列表已跨「>= 1 小時前」；下一輪就停

                url = response.urljoin(href)
                if url in seen:
                    continue
                seen.add(url)
                added += 1

                yield scrapy.Request(
                    url=url,
                    callback=self.parse_article,
                    cb_kwargs={"title_from_list": title},
                    dont_filter=True,
                )

        # ── 初次收集 ────────────────────────────────────────────────
        for req in collect_visible():
            yield req

        # ── 主捲動：數量增加 或 最後 href 改變 即視為有新內容 ───────────
        scrolls = 0
        last_sig = (get_card_count(driver), get_last_href(driver))

        while True:
            if self.hit_older:
                break
            if time.time() > GLOBAL_DEADLINE:
                break
            if scrolls >= SCROLL_CAP:
                break

            before_cnt, before_last = last_sig

            # 把最後一張卡片捲到視窗底，較容易觸發載入
            try:
                last_el = driver.find_elements(By.CSS_SELECTOR, CARD_SEL)[-1]
                driver.execute_script("arguments[0].scrollIntoView({block:'end'});", last_el)
            except Exception:
                driver.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
            driver.execute_script("window.dispatchEvent(new Event('scroll'))")

            try:
                wait.until(lambda d: (get_card_count(d) > before_cnt) or (get_last_href(d) != before_last))
                stalls = 0
            except TimeoutException:
                # 有些頁面是按鈕載入
                try:
                    btn = driver.find_element(
                        By.XPATH, "//button[contains(., '更多') or contains(., '載入') or contains(., 'More')]"
                    )
                    driver.execute_script("arguments[0].click();", btn)
                    stalls = 0
                except Exception:
                    stalls += 1
                    self.logger.info(f"[scroll] no new cards (stall {stalls}/{STALLS_LIMIT})")
                    if stalls >= STALLS_LIMIT:
                        self.logger.info("[stop] stalled.")
                        break

            # 收集新出現的卡片
            new_any = False
            for req in collect_visible():
                new_any = True
                yield req

            # 若這批收集後已跨一小時，就在下次迴圈開頭停
            if self.hit_older:
                continue

            # 若真的沒變化，計一次停滯（上面 TimeoutException 已計一次）
            if not new_any:
                stalls += 1
                if stalls >= STALLS_LIMIT:
                    self.logger.info("[stop] plateau.")
                    break

            scrolls += 1
            last_sig = (get_card_count(driver), get_last_href(driver))

        # 保險再掃一次（最後一批）
        for req in collect_visible():
            yield req

    def parse_article(self, response, title_from_list):
        # 標題（內文為主，缺就用列表）
        title = (response.css("h1::text").get() or title_from_list).strip()

        # 時間
        published_at_raw = response.css("time::attr(datetime)").get()
        pub_dt_utc = self.parse_iso_utc(published_at_raw)
        current_time_utc = datetime.now(timezone.utc)
        time_difference = current_time_utc - pub_dt_utc
        one_hour_delta = timedelta(hours=1)

        # 沒抓到時間或不在一小時內就略過
        if abs(time_difference) < one_hour_delta:
            published_at = (response.css("time::text").get()).strip()
        else:
            return

        # 作者（這個 selector 可能會因版型不同而失效，可再調）
        texts = [t.strip() for t in response.css("span.text-batcave ::text").getall() if t.strip()]
        author = texts[-1] if texts else ""

        yield {
            "連結": response.url,
            "標題": title,
            "作者": author,
            "日期": published_at,
        }
