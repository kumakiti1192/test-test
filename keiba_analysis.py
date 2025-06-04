# coding: utf-8

"""競馬 Excel 解析スクリプト

このスクリプトは Excel ファイルから予想印を読み込み、ネット競馬からレース結果
およびオッズを取得して馬券シミュレーションを行います。

`予想データ` フォルダにある `YYYYMMDD_*.xlsx` という2つのファイルを対象に
集計を行い、購入金額、払戻金額、回収率などを計算します。
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import pickle
import random
import re
import signal
import subprocess
import time
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import undetected_chromedriver as uc

# -----------------------------------------------
# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
uc.logger.setLevel(logging.ERROR)

# -----------------------------------------------
# ユーティリティ

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/136.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Version/17.5 Mobile Safari/605.1.15",
]

def random_ua() -> str:
    return random.choice(UA_POOL)


def _kill_tasks(pattern: str = r"(chrome|chromedriver|msedge)") -> None:
    if os.name != "nt":
        return
    try:
        out = subprocess.check_output("tasklist").decode(errors="ignore")
        for line in out.splitlines():
            if re.search(pattern, line, re.I):
                os.kill(int(line.split()[1]), signal.SIGTERM)
    except Exception:
        pass

def kill_zombie_chrome() -> None:
    _kill_tasks()

NETWORK_IDLE_JS = """
return (function(sec){
  const now=performance.now();
  const recent=performance.getEntriesByType('resource')
               .filter(r=>(now-r.responseEnd)<(sec*1000));
  return recent.length===0;})(arguments[0]);
"""

_uc_driver_cached: Path | None = None

def prepare_chrome_driver(
    *,
    headless: bool = True,
    no_images: bool = False,
    version_main: int | None = 136,
    max_retry: int = 4,
):
    """Selenium 用 ChromeDriver 準備関数"""

    global _uc_driver_cached
    for att in range(1, max_retry + 1):
        try:
            _kill_tasks()
            opt = uc.ChromeOptions()
            opt.add_argument(f"--user-agent={random_ua()}")
            if headless:
                opt.add_argument("--headless=new")
            for flag in (
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--remote-debugging-port=0",
            ):
                opt.add_argument(flag)
            if no_images:
                opt.add_experimental_option(
                    "prefs", {"profile.managed_default_content_settings.images": 2}
                )

            kw: Dict[str, Any] = dict(
                options=opt,
                patcher_force_close=True,
                log_level=3,
                timeout=90,
                suppress_welcome=True,
            )
            if version_main is not None:
                kw["version_main"] = version_main + (att - 1)
            if _uc_driver_cached:
                kw["driver_executable_path"] = str(_uc_driver_cached)

            driver = uc.Chrome(**kw)
            if _uc_driver_cached is None:
                _uc_driver_cached = Path(driver.service.path).resolve()

            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"},
            )
            logging.info(f"[Driver] Chrome 起動成功 (retry={att})")
            return driver
        except Exception as e:  # pragma: no cover - driver errors are external
            logging.warning(f"[Driver Retry {att}] {e}")
            time.sleep(2 + random.random())
    raise RuntimeError("Chrome 起動失敗 : 全リトライ失敗")


def safe_driver_get(
    driver,
    url: str,
    wait_xpath: str | None = None,
    timeout: int = 45,
    quiet_sec: int = 2,
    max_retry: int = 3,
    use_network_idle: bool = True,
) -> bool:
    for att in range(1, max_retry + 1):
        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            if wait_xpath:
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, wait_xpath))
                )
            if use_network_idle:
                WebDriverWait(driver, timeout).until(
                    lambda d: d.execute_script(NETWORK_IDLE_JS, quiet_sec)
                )
            return True
        except (TimeoutException, Exception) as e:
            logging.warning(f"[GET-Retry {att}/{max_retry}] {url} → {e.__class__.__name__}")
            time.sleep(2 + random.random())
    return False


# -----------------------------------------------
# Excel 解析部

PRED_DIR = Path("予想データ")
BET_UNIT = 100
MARK_ORDER = ["◎", "○", "▲", "△", "✕", "6", "7", "8"]
PLACE_CODE = {
    "札幌": "01",
    "函館": "02",
    "福島": "03",
    "新潟": "04",
    "東京": "05",
    "中山": "06",
    "中京": "07",
    "京都": "08",
    "阪神": "09",
    "小倉": "10",
}
INV_PLACE_CODE = {v: k for k, v in PLACE_CODE.items()}


def save_pickle_safe(obj: Any, path: Path) -> None:
    tmp = path.with_suffix(".tmp")
    pickle.dump(obj, tmp.open("wb"))
    tmp.replace(path)


def load_pickle_safe(path: Path) -> Any:
    try:
        return pickle.load(open(path, "rb"))
    except Exception:
        path.unlink(missing_ok=True)
        return None


def extract_date_from_filename(fname: str) -> str | None:
    m = re.search(r"(\d{8})", fname)
    return m.group(1) if m else None


def z2h(s: str) -> str:
    return unicodedata.normalize("NFKC", s) if isinstance(s, str) else s


def load_predictions(xlsx_path: Path) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, header=None, dtype=str)
    df = df.iloc[:, [0, 1, 2, 39]]
    df.columns = ["場所", "R", "番", "印"]

    df["番"] = df["番"].map(lambda x: z2h(str(x)).strip())
    df["R"] = df["R"].map(lambda x: z2h(str(x)).strip())
    df = df[df["番"].str.match(r"^\d+$")].dropna(subset=["印"])

    df["番"] = df["番"].astype(int)
    df["R"] = df["R"].astype(int)
    return df


CACHE_RID = Path("cache/race_ids")
CACHE_RID.mkdir(parents=True, exist_ok=True)


def scrape_race_ids_one_day(
    date: str,
    force: bool = False,
    cache_dir: Path = Path("cache/race_ids"),
) -> List[str]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cfile = cache_dir / f"race_ids_{date}.pkl"
    if not force and cfile.exists():
        ids = load_pickle_safe(cfile)
        if ids:
            logging.info(f"[CACHE] {date} race_id {len(ids)} 件")
            return ids

    url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date}"
    logging.info(f"[GET] {url}")
    try:
        html = requests.get(url, headers={"User-Agent": random_ua()}, timeout=20).text
    except Exception as e:
        logging.warning(f"[{date}] requests 失敗 → {e}")
        html = ""

    pat = r"(?:race[_I]d|raceId)=(\d{12})"
    ids = re.findall(pat, html)
    if not ids:
        ids = re.findall(r"/race/result\.html\?race_id=(\d{12})", html)

    if not ids:
        logging.warning(f"[{date}] requests 0 件 ⇒ Selenium fallback")
        drv = prepare_chrome_driver(headless=True, no_images=True)
        ok = safe_driver_get(
            drv,
            url,
            wait_xpath='//a[contains(@href,"race_id=")]',
            use_network_idle=False,
        )
        if ok:
            html = drv.page_source
            ids = re.findall(pat, html) or re.findall(
                r"/race/result\.html\?race_id=(\d{12})", html
            )
        drv.quit()

    ids = sorted(set(ids))
    logging.info(f"[{date}] 抽出 {len(ids)} 件")
    if ids:
        save_pickle_safe(ids, cfile)
    return ids


def build_place_r_map(rid: str) -> Tuple[str, int]:
    place_cd = rid[8:10]
    rnum = int(rid[-2:])
    return INV_PLACE_CODE.get(place_cd, "??"), rnum


def get_race_ids_for_date(date: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for rid in scrape_race_ids_one_day(date):
        place, r = build_place_r_map(rid)
        mapping[f"{place}-{r}"] = rid
    return mapping


def parse_payout_table(soup: BeautifulSoup) -> Dict[str, Dict[Any, int]]:
    pay: Dict[str, Dict[Any, int]] = {
        k: {} for k in (
            "tansho",
            "fukusho",
            "umaren",
            "umatan",
            "wide",
            "sanrenpuku",
            "sanrentan",
        )
    }
    rows = soup.select(".PayTable_01 tr")
    for tr in rows:
        th = tr.find("th")
        bt = th.get_text(strip=True) if th else ""
        tds = tr.find_all("td")
        txts = [td.get_text(strip=True) for td in tds]
        if "単勝" in bt and len(txts) >= 2:
            num = int(txts[0])
            odd = int(txts[1].replace(",", ""))
            pay["tansho"][num] = odd
        elif "複勝" in bt and len(txts) >= 6:
            for i in range(0, 6, 2):
                n = int(txts[i])
                o = int(txts[i + 1].replace(",", ""))
                pay["fukusho"][n] = o
        elif "馬連" in bt and "-" in txts[0]:
            a, b = sorted(map(int, txts[0].split("-")))
            odd = int(txts[1].replace(",", ""))
            pay["umaren"][(a, b)] = odd
        elif "馬単" in bt and "→" in txts[0]:
            a, b = map(int, txts[0].split("→"))
            odd = int(txts[1].replace(",", ""))
            pay["umatan"][(a, b)] = odd
        elif "ワイド" in bt:
            for span in tr.select("span"):
                p = span.get_text(strip=True)
                if "-" in p:
                    a, b = sorted(map(int, p.split("-")))
                    odd = int(
                        span.find_next("span").get_text(strip=True).replace(",", "")
                    )
                    pay["wide"][(a, b)] = odd
        elif "三連複" in bt and "-" in txts[0]:
            nums = tuple(sorted(map(int, txts[0].split("-"))))
            odd = int(txts[1].replace(",", ""))
            pay["sanrenpuku"][nums] = odd
        elif "三連単" in bt and "→" in txts[0]:
            nums = tuple(map(int, txts[0].split("→")))
            odd = int(txts[1].replace(",", ""))
            pay["sanrentan"][nums] = odd
    return pay


CACHE_RES = Path("cache/results")
CACHE_RES.mkdir(parents=True, exist_ok=True)


def get_race_results_with_odds(rid: str, driver: Optional[uc.Chrome] = None) -> Dict[str, Any]:
    """Return result order and payouts for a race.

    HTML は ``cache/results`` に保存し、次回以降はキャッシュを利用する。
    ``requests`` で取得できなかった場合のみ Selenium にフォールバックする。
    """
    url = f"https://race.netkeiba.com/race/result.html?race_id={rid}"

    html_path = CACHE_RES / f"{rid}.html"
    soup = None

    if html_path.exists():
        try:
            soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "lxml")
            logging.info(f"[CACHE] {rid} html loaded")
        except Exception:
            html_path.unlink(missing_ok=True)

    if soup is None:
        try:
            html = requests.get(url, headers={"User-Agent": random_ua()}, timeout=20).text
            soup = BeautifulSoup(html, "lxml")
            if soup.select_one(".RaceTable01"):
                html_path.write_text(html, encoding="utf-8")
                logging.info(f"[REQ] {rid} html fetched")
            else:
                soup = None
        except Exception as e:
            logging.warning(f"[REQ {rid}] {e}")
            soup = None

    if soup is None:
        close_driver = False
        if driver is None:
            driver = prepare_chrome_driver(headless=True, no_images=True)
            close_driver = True
        ok = safe_driver_get(
            driver,
            url,
            wait_xpath='//*[@class="RaceTable01"]',
            use_network_idle=False,
            timeout=90,
        )
        if not ok:
            if close_driver:
                driver.quit()
            return {"order": []}
        soup = BeautifulSoup(driver.page_source, "lxml")
        html_path.write_text(driver.page_source, encoding="utf-8")
        if close_driver:
            driver.quit()

    order = [
        int(tr.find_all("td")[2].get_text(strip=True))
        for tr in soup.select(".RaceTable01 tr")[1:4]
    ]
    payout = parse_payout_table(soup)
    return {"order": order, **payout}


# ---------------------------------------------------------------------
# 馬券生成

def generate_bets(marks: Dict[str, int | None]) -> Dict[str, List[Tuple[str, Tuple]]]:
    uma0 = marks.get("◎")
    if uma0 is None:
        return {}

    uma1 = marks.get("○")
    uma2 = marks.get("▲")
    uma3 = marks.get("△")
    others = [v for k, v in marks.items() if k not in ["◎", "○", "▲", "△"] and v is not None]

    top8 = [u for u in [uma0, uma1, uma2, uma3] + others if u is not None][:8]

    bets: Dict[str, List[Tuple[str, Tuple]]] = defaultdict(list)

    bets["単勝◎"].append(("単勝", (uma0,)))
    bets["複勝◎"].append(("複勝", (uma0,)))

    for um in top8[1:]:
        bets["馬連◎-8"].append(("馬連", tuple(sorted((uma0, um)))))

    for um in top8[1:6]:
        bets["馬単◎-6"].append(("馬単", (uma0, um)))

    if uma3 is not None:
        for a, b in [(uma0, uma3), (uma1, uma3), (uma2, uma3)]:
            if a is not None and b is not None:
                bets["ワイド◎-△"].append(("ワイド", tuple(sorted((a, b)))))

    for c in itertools.combinations(top8, 3):
        if uma0 in c:
            bets["三連複◎-8"].append(("三連複", tuple(sorted(c))))

    for p in itertools.permutations(top8, 3):
        if p[0] == uma0:
            bets["三連単◎-8"].append(("三連単", p))

    if uma1 is not None and uma3 is not None:
        pairs = {(uma0, uma1), (uma0, uma3), (uma1, uma3)}
        if uma2 is not None:
            pairs |= {(uma0, uma2), (uma2, uma3)}
        for a, b in pairs:
            bets["馬連◎○-△フォ"].append(("馬連", tuple(sorted((a, b)))))

    box = [u for u in [uma0, uma1, uma2, uma3] if u is not None]
    for a, b in itertools.combinations(box, 2):
        bets["ワイド◎～△BOX"].append(("ワイド", tuple(sorted((a, b)))))

    if all(x is not None for x in (uma1, uma2, uma3)):
        for p in itertools.permutations([uma1, uma2, uma3], 2):
            bets["三連単◎○▲△-8フォ"].append(("三連単", (uma0, p[0], p[1])))

    if uma1 is not None:
        for c in itertools.combinations(top8, 3):
            if uma0 in c and uma1 in c:
                bets["三連複◎○-8フォ"].append(("三連複", tuple(sorted(c))))

    box6 = top8[:6]
    if len(box6) == 6:
        for c in itertools.combinations(box6, 3):
            if uma0 in c:
                bets["三連複◎～6BOX"].append(("三連複", tuple(sorted(c))))

    return bets

# ---------------------------------------------------------------------
# 払戻判定

def evaluate_bet(bet_type: str, nums: Tuple, result: Dict[str, Any]) -> int:
    order = result["order"]
    if not order:
        return 0
    if bet_type == "単勝":
        return (
            result["tansho"].get(nums[0], 0) * BET_UNIT // 100
            if nums[0] == order[0]
            else 0
        )
    if bet_type == "複勝":
        return (
            result["fukusho"].get(nums[0], 0) * BET_UNIT // 100
            if nums[0] in order[:3]
            else 0
        )
    if bet_type == "馬連":
        return result["umaren"].get(tuple(sorted(nums)), 0) * BET_UNIT // 100
    if bet_type == "馬単":
        return result["umatan"].get(nums, 0) * BET_UNIT // 100
    if bet_type == "ワイド":
        return result["wide"].get(tuple(sorted(nums)), 0) * BET_UNIT // 100
    if bet_type == "三連複":
        return result["sanrenpuku"].get(tuple(sorted(nums)), 0) * BET_UNIT // 100
    if bet_type == "三連単":
        return result["sanrentan"].get(nums, 0) * BET_UNIT // 100
    return 0


# ---------------------------------------------------------------------
# 一日分処理

def process_one_date(date: str, files: List[Path]) -> pd.DataFrame:
    preds = pd.concat([load_predictions(f) for f in files], ignore_index=True)
    race_map = get_race_ids_for_date(date)
    recs: List[Dict[str, Any]] = []

    driver = prepare_chrome_driver(headless=True, no_images=True)
    for (place, r), grp in preds.groupby(["場所", "R"]):
        rid = race_map.get(f"{place}-{int(r)}")
        if not rid:
            continue
        marks = {
            row["印"]: row["番"]
            for _, row in grp.iterrows()
            if row["印"] in MARK_ORDER
        }
        bets = generate_bets(marks)
        result = get_race_results_with_odds(rid, driver=driver)
        for bname, blist in bets.items():
            for btype, nums in blist:
                pay = evaluate_bet(btype, nums, result)
                recs.append(
                    dict(日付=date, 券種=bname, 的中=int(pay > 0), 購入=BET_UNIT, 払戻=pay)
                )
    driver.quit()
    logging.info(f"[RECS] {date}: {len(recs)} 行")
    return pd.DataFrame(recs)


# ---------------------------------------------------------------------
# 集計メイン

def main() -> None:
    date_to_files: Dict[str, List[Path]] = defaultdict(list)
    for f in PRED_DIR.glob("*.xlsx"):
        if f.name.startswith("~$"):
            continue
        d = extract_date_from_filename(f.name)
        if d:
            date_to_files[d].append(f)

    all_df: List[pd.DataFrame] = []
    for date, flist in date_to_files.items():
        if len(flist) != 2:
            logging.warning(f"[WARN] {date}: Excel が 2 枚ではありません → {flist}")
        all_df.append(process_one_date(date, flist))

    df_all = pd.concat(all_df, ignore_index=True) if all_df else pd.DataFrame()

    if df_all.empty:
        logging.error("⚠️ 有効データ 0 行です。race_id 取得や列位置を確認してください。")
        return

    summary = (
        df_all.groupby("券種", as_index=False)
        .agg(
            的中数=("的中", "sum"),
            購入金額=("購入", "sum"),
            払戻金額=("払戻", "sum"),
            総レース数=("的中", "count"),
        )
        .assign(
            的中率=lambda d: d["的中数"] / d["総レース数"] * 100,
            回収率=lambda d: d["払戻金額"] / d["購入金額"] * 100,
        )
        .round({"的中率": 2, "回収率": 2})
    )
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
