"""
Telegram-бот: следит за ETH-кошельками и присылает админу уведомление,
когда на кошелёк приходит нулевая (0 ETH) внутренняя транзакция.
По кнопке показывает балансы тех, кто прислал нулевые транзакции.

Источники данных (переменная SOURCE):
  blockscout — Blockscout API (по умолчанию): бесплатно, без ключа, видит нулевые транзакции;
  etherscan  — Etherscan API v2, метод txlistinternal (нужен ETHERSCAN_API_KEY);
  trace      — RPC-узел с поддержкой trace_filter (Alchemy, QuickNode и т.п.).
Балансы всегда берутся из Blockscout.
"""

import html
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------- настройки

def load_env(path: Path) -> None:
    """Простой загрузчик .env (KEY=VALUE), без лишних зависимостей."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env(BASE_DIR / ".env")

# bothost.ru сам подставляет токен в BOT_TOKEN (или API_TOKEN / TELEGRAM_BOT_TOKEN)
BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
             or os.getenv("API_TOKEN") or "").strip()
ADMIN_IDS = {int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_IDS", "")) if x.strip()}
SOURCE = os.getenv("SOURCE", "blockscout").strip().lower()
BLOCKSCOUT_URL = os.getenv("BLOCKSCOUT_URL", "https://eth.blockscout.com").rstrip("/")
BLOCKSCOUT_MAX_PAGES = 5
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
CHAIN_ID = os.getenv("CHAIN_ID", "1").strip()
RPC_URL = os.getenv("RPC_URL", "").strip()
CHECK_INTERVAL = max(15, int(os.getenv("CHECK_INTERVAL", "60")))
NOTIFY_ALL = os.getenv("NOTIFY_ALL", "false").strip().lower() in ("1", "true", "yes")
EXPLORER_URL = os.getenv("EXPLORER_URL", "https://etherscan.io").rstrip("/")
TRACE_MAX_BLOCKS = max(1, int(os.getenv("TRACE_MAX_BLOCKS", "10000")))
# Запасные бесплатные узлы Ethereum (поддерживают trace_filter): используются, когда Blockscout лежит
FALLBACK_RPCS = [u for u in re.split(r"[,\s]+", os.getenv(
    "FALLBACK_RPCS", "https://eth.drpc.org,https://eth.merkle.io")) if u]
# Бесплатные узлы для простых запросов (баланс, код, номер блока, транзакция).
# Нагрузка распределяется по кругу — так ни один узел не упирается в лимит.
BASIC_RPCS = [u for u in re.split(r"[,\s]+", os.getenv("BASIC_RPCS", ",".join([
    "https://rpc.mevblocker.io", "https://eth-mainnet.public.blastapi.io",
    "https://rpc.flashbots.net", "https://eth-pokt.nodies.app", "https://1rpc.io/eth",
    "https://mainnet.gateway.tenderly.co", "https://ethereum-rpc.publicnode.com"]))) if u]
# Через сколько минут молчащего Blockscout писать админу о переключении (короткие сбои — без сообщений)
FALLBACK_NOTIFY_MIN = max(0, int(os.getenv("FALLBACK_NOTIFY_MIN", "30")))
INITIAL_WALLETS = [w for w in re.split(r"[,\s]+", os.getenv("WALLETS", "")) if w]
# 0 = разбирать все нулевые транзакции из истории; число — только столько последних
SENDERS_LIMIT = max(0, int(os.getenv("SENDERS_LIMIT", "0")))
# Сколько страниц истории (по 50 записей) смотреть: 40 страниц = до 2000 записей
SENDERS_MAX_PAGES = max(1, int(os.getenv("SENDERS_MAX_PAGES", "40")))
# Кнопка «Создатели»: показывать только адреса, где ETH больше чем на столько долларов
MIN_CREATOR_USD = float(os.getenv("MIN_CREATOR_USD", "10"))

# Папка для данных. На bothost.ru переживает обновления только /app/data.
_default_data = Path("/app/data") if Path("/app").is_dir() else BASE_DIR
DATA_DIR = Path(os.getenv("DATA_DIR") or _default_data)
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"

ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
MAX_MESSAGES_PER_CHECK = 20
SEEN_LIMIT = 2000
BUTTONS_LIMIT = 500
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DATA_DIR / "bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("eth-zero-bot")
http = requests.Session()
# Обычный браузерный User-Agent: защита Cloudflare у Blockscout и узлов
# блокирует «ботовые» заголовки с серверов хостингов (ошибка 403).
http.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json",
})


def safe(err: Exception) -> str:
    """Текст ошибки без токенов и ключей (requests кладёт URL в сообщение)."""
    text = str(err)
    for secret in (BOT_TOKEN, ETHERSCAN_API_KEY, RPC_URL):
        if secret:
            text = text.replace(secret, "***")
    return text


# ---------------------------------------------------------------- состояние

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception as e:  # повреждённый файл — начинаем заново
            log.error("Не удалось прочитать state.json: %s", e)
    return {"wallets": {}}


_save_lock = threading.Lock()


def save_state() -> None:
    with _save_lock:  # сохранять могут несколько потоков сразу
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE_FILE)


state = load_state()
runtime = {"last_check": None, "last_error": None, "error_streak": 0, "error_notified": False}


# ---------------------------------------------------------------- Telegram

def tg(method: str, **params):
    params = {k: v for k, v in params.items() if v is not None}
    timeout = params.get("timeout", 0) + 20
    r = http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=params, timeout=timeout)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram {method}: {data.get('description')}")
    return data["result"]


def send(chat_id: int, text: str, reply_markup=None) -> None:
    try:
        tg("sendMessage", chat_id=chat_id, text=text, parse_mode="HTML",
           disable_web_page_preview=True, reply_markup=reply_markup)
    except Exception as e:
        log.error("Не удалось отправить сообщение в %s: %s", chat_id, safe(e))


def send_long(chat_id: int, text: str) -> None:
    """Telegram ограничивает сообщение 4096 символами — режем по строкам."""
    chunk = ""
    for line in text.split("\n"):
        if len(chunk) + len(line) + 1 > 3800:
            send(chat_id, chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk.strip():
        send(chat_id, chunk)


def notify_admins(text: str, reply_markup=None) -> None:
    for admin_id in ADMIN_IDS:
        send(admin_id, text, reply_markup)
        time.sleep(0.05)


MENU_CONTRACTS = "📜 Контракты"
MENU_CREATORS = "👤 Создатели контрактов"
MENU_TEST = "🔎 Тест"
MENU_LIST = "📋 Кошельки"
MENU_KEYBOARD = {
    "keyboard": [[{"text": MENU_CONTRACTS}, {"text": MENU_CREATORS}],
                 [{"text": MENU_TEST}, {"text": MENU_LIST}]],
    "resize_keyboard": True,
    "is_persistent": True,
}


# ---------------------------------------------------------------- источники данных

def etherscan_get(**params):
    params.update(chainid=CHAIN_ID, apikey=ETHERSCAN_API_KEY)
    r = http.get(ETHERSCAN_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


class BlockscoutDown(RuntimeError):
    """Blockscout недоступен (сбой на их стороне)."""


def blockscout_get(path: str, params=None, attempts: int = 4, timeout: int = 30):
    """GET к Blockscout с повторами: сервер иногда отвечает 500/502/503/429."""
    for attempt in range(attempts):
        try:
            r = http.get(f"{BLOCKSCOUT_URL}/api/v2/{path}", params=params, timeout=timeout)
            if r.status_code in (403, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524):
                raise BlockscoutDown(f"Blockscout недоступен (ошибка {r.status_code})")
            if r.status_code >= 400:
                raise BlockscoutDown(f"Blockscout вернул ошибку {r.status_code}")
            return r.json()
        except (requests.RequestException, BlockscoutDown) as e:
            if attempt == attempts - 1:
                if isinstance(e, BlockscoutDown):
                    raise
                raise BlockscoutDown(f"Blockscout не отвечает ({type(e).__name__})") from e
            time.sleep(3 * 2 ** attempt)  # 3, 6, 12 секунд


def rpc_urls() -> list:
    """Узлы с поддержкой trace_* (история внутренних транзакций)."""
    return ([RPC_URL] if RPC_URL else []) + [u for u in FALLBACK_RPCS if u != RPC_URL]


_rr = [0]


def rpc_urls_for(method: str) -> list:
    """Для trace_* — только trace-узлы; для простых запросов — все узлы по кругу."""
    if method.startswith("trace_"):
        urls = rpc_urls()
    else:
        basic = [u for u in BASIC_RPCS if u not in rpc_urls()]
        if basic:
            _rr[0] = (_rr[0] + 1) % len(basic)
            basic = basic[_rr[0]:] + basic[:_rr[0]]
        urls = ([RPC_URL] if RPC_URL else []) + basic + [u for u in FALLBACK_RPCS if u != RPC_URL]
    now = time.time()
    fresh = [u for u in urls if _rpc_cooldown.get(u, 0) <= now]
    # узлы «на перерыве» — в конец списка, но не выбрасываем совсем
    return fresh + [u for u in urls if u not in fresh]


class RpcRateLimited(RuntimeError):
    """Бесплатный узел ограничил число запросов."""


_rpc_lock = threading.Lock()
_rpc_last: dict = {}
_rpc_cooldown: dict = {}
RPC_MIN_INTERVAL = 0.25  # не чаще 4 запросов в секунду на один узел


def _rpc_once(url: str, method: str, params: list):
    with _rpc_lock:  # «светофор» для каждого узла отдельно
        wait = _rpc_last.get(url, 0.0) + RPC_MIN_INTERVAL - time.time()
        _rpc_last[url] = time.time() + max(0.0, wait)
    if wait > 0:
        time.sleep(wait)
    try:
        r = http.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                      timeout=40)
    except requests.RequestException:
        _rpc_cooldown[url] = time.time() + 60
        raise
    def limited(msg):
        _rpc_cooldown[url] = time.time() + 30  # узел отдыхает 30 с, работают остальные
        return RpcRateLimited(msg)

    if r.status_code in (403, 429):
        raise limited(f"RPC {method}: слишком много запросов")
    try:
        data = r.json()
    except ValueError:
        raise limited(f"RPC {method}: узел вернул не JSON (HTTP {r.status_code})")
    if not isinstance(data, dict):
        raise limited(f"RPC {method}: странный ответ узла")
    if "error" in data:
        err = data["error"]
        text = str(err).lower()
        if "rate" in text or "limit" in text or "too many" in text:
            raise limited(f"RPC {method}: {err}")
        raise RuntimeError(f"RPC {method}: {err}")
    if "result" not in data:
        raise limited(f"RPC {method}: пустой ответ узла")
    return data["result"]


def rpc(method: str, params: list):
    """JSON-RPC запрос: пробуем узлы по очереди; если все ограничили — ждём и повторяем."""
    last_error = None
    for attempt in range(5):
        busy = False
        for url in rpc_urls_for(method):
            try:
                return _rpc_once(url, method, params)
            except (RpcRateLimited, requests.RequestException) as e:
                busy = True
                last_error = e
            except Exception as e:
                # узел не умеет этот запрос (нет архива/метода) — сразу пробуем другой
                last_error = last_error if isinstance(last_error, RpcRateLimited) else e
        if not busy:
            break
        time.sleep(3 * (attempt + 1))  # 3, 6, 9, 12 секунд
    raise last_error or RuntimeError("RPC-узлы не заданы")


_latest_cache = {"block": 0, "time": 0.0}


def rpc_latest_block() -> int:
    # Номер блока меняется раз в 12 с — кэшируем на 10 с, чтобы не тратить запросы.
    if time.time() - _latest_cache["time"] > 10:
        try:
            # Узлы за балансировщиком иногда чуть отстают — берём на 3 блока раньше.
            block = int(rpc("eth_blockNumber", []), 16) - 3
            _latest_cache.update(block=max(block, _latest_cache["block"]), time=time.time())
        except Exception:
            if not _latest_cache["block"]:
                raise
            # узлы заняты — берём прошлое значение, за минуту ничего не потеряется
            log.warning("Номер блока не получен, использую последний известный")
    return _latest_cache["block"]


# ---- переключение Blockscout <-> запасной источник

def use_blockscout(kind: str = "txs") -> bool:
    """False, пока этот раздел Blockscout недавно падал: не тратим время на повторы.
    kind: "txs" — история транзакций, "addr" — балансы/создатели (падают независимо)."""
    if kind == "addr" and SOURCE == "trace":
        return False
    if kind == "txs" and SOURCE != "blockscout":
        return False
    return time.time() >= runtime.setdefault("bs_retry_at", {}).get(kind, 0)


def blockscout_failed(e: Exception, kind: str = "txs") -> None:
    # Повторно пробуем Blockscout через 3 минуты — сбои у него обычно короткие.
    runtime.setdefault("bs_retry_at", {})[kind] = time.time() + 180
    if kind != "txs":
        log.warning("Blockscout (балансы) недоступен: %s — беру данные с узлов Ethereum", e)
        return
    if not runtime.get("fallback"):
        runtime.update(fallback=True, fallback_since=time.time(), fallback_notified=False)
        log.warning("Blockscout недоступен (%s) — переключаюсь на запасной источник", e)
    # Короткие сбои админа не беспокоят: пишем, только если Blockscout молчит долго.
    elif (not runtime.get("fallback_notified")
          and time.time() - runtime.get("fallback_since", time.time()) >= FALLBACK_NOTIFY_MIN * 60):
        runtime["fallback_notified"] = True
        notify_admins(f"⚠️ Blockscout не работает уже больше {FALLBACK_NOTIFY_MIN} мин "
                      f"({html.escape(str(e))}). Слежу через запасные узлы Ethereum — "
                      f"уведомления продолжают приходить.")


def blockscout_ok() -> None:
    if runtime.get("fallback"):
        runtime["fallback"] = False
        log.info("Blockscout снова работает")
        if runtime.pop("fallback_notified", False):
            notify_admins("✅ Blockscout снова работает, вернулся на основной источник.")


def latest_block() -> int:
    if SOURCE == "trace":
        return rpc_latest_block()
    if SOURCE == "etherscan":
        data = etherscan_get(module="proxy", action="eth_blockNumber")
        result = data.get("result")
        if not isinstance(result, str) or not result.startswith("0x"):
            raise RuntimeError(f"Etherscan eth_blockNumber: {data}")
        return int(result, 16)
    if use_blockscout():
        try:
            return int(blockscout_get("blocks", {"type": "block"})["items"][0]["height"])
        except BlockscoutDown as e:
            blockscout_failed(e)
    return rpc_latest_block()


def item_key(tx_hash: str, frm: str, to: str, value: int, extra: str = "") -> str:
    """Один и тот же ключ у всех источников — чтобы при переключении не было дублей."""
    return f"{tx_hash}:{frm}:{to}:{value}:{extra}".lower()


def fetch_blockscout(address: str, start_block: int, max_pages: int = BLOCKSCOUT_MAX_PAGES):
    """Входящие внутренние транзакции через Blockscout (видит и нулевые)."""
    items = []
    params = {"filter": "to"}
    for _ in range(max_pages):
        data = blockscout_get(f"addresses/{address}/internal-transactions", params)
        reached_old = False
        for t in data.get("items") or []:
            block = int(t["block_number"])
            if block < start_block:
                reached_old = True
                break
            ts = None
            if t.get("timestamp"):
                ts = int(datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00")).timestamp())
            frm = (t.get("from") or {}).get("hash", "")
            to = (t.get("to") or {}).get("hash", "")
            value = int(t.get("value") or 0)
            items.append({
                "hash": t["transaction_hash"],
                "block": block,
                "from": frm,
                "to": to,
                "value": value,
                "time": ts,
                "type": t.get("type", ""),
                "error": t.get("success") is False,
                "key": item_key(t["transaction_hash"], frm, to, value),
            })
        next_params = data.get("next_page_params")
        if reached_old or not next_params:
            break
        params = {"filter": "to", **next_params}
        time.sleep(0.3)

    items.reverse()  # от старых к новым
    # Следующий запрос начинаем с последнего блока (включительно): дубли отсекаются по key.
    next_block = max((i["block"] for i in items), default=start_block)
    return items, next_block


def fetch_etherscan(address: str, start_block: int):
    """Внутренние транзакции кошелька начиная с start_block (включительно)."""
    data = etherscan_get(module="account", action="txlistinternal", address=address,
                         startblock=start_block, endblock=99999999,
                         page=1, offset=1000, sort="asc")
    if data.get("status") != "1":
        message = str(data.get("message", ""))
        result = data.get("result")
        if result == [] or "No transactions found" in message:
            return [], start_block
        raise RuntimeError(f"Etherscan: {message} — {result}")

    items = []
    for t in data["result"]:
        value = int(t.get("value") or 0)
        items.append({
            "hash": t["hash"],
            "block": int(t["blockNumber"]),
            "from": t.get("from", ""),
            "to": t.get("to", ""),
            "value": value,
            "time": int(t["timeStamp"]) if t.get("timeStamp") else None,
            "type": t.get("type", ""),
            "error": t.get("isError") == "1",
            "key": item_key(t["hash"], t.get("from", ""), t.get("to", ""), value),
        })
    next_block = max((i["block"] for i in items), default=start_block)
    return items, next_block


def parse_traces(traces) -> list:
    items = []
    for t in traces or []:
        trace_address = t.get("traceAddress") or []
        if not trace_address or t.get("type") != "call":
            continue  # верхний уровень — это обычная транзакция, не внутренняя
        action = t.get("action", {})
        frm, to = action.get("from", ""), action.get("to", "")
        value = int(action.get("value") or "0x0", 16)
        items.append({
            "hash": t["transactionHash"],
            "block": int(t["blockNumber"]),
            "from": frm,
            "to": to,
            "value": value,
            "time": None,
            "type": action.get("callType", "call"),
            "error": bool(t.get("error")),
            "key": item_key(t["transactionHash"], frm, to, value),
        })
    return items


def trace_range(address: str, lo: int, hi: int, depth: int = 0) -> list:
    """trace_filter за диапазон блоков; если узел отказал — делим диапазон пополам."""
    try:
        return rpc("trace_filter", [{"fromBlock": hex(lo), "toBlock": hex(hi), "toAddress": [address]}]) or []
    except Exception:
        if depth >= 8 or hi - lo < 2000:
            raise
        mid = (lo + hi) // 2
        return trace_range(address, lo, mid, depth + 1) + trace_range(address, mid + 1, hi, depth + 1)


def fetch_trace(address: str, start_block: int):
    """Все внутренние вызовы на адрес через trace_filter (включая нулевые)."""
    latest = rpc_latest_block()
    if start_block > latest:
        return [], start_block
    end_block = min(latest, start_block + TRACE_MAX_BLOCKS - 1)
    items = parse_traces(trace_range(address, start_block, end_block))
    return items, end_block + 1


def trace_history(address: str) -> list:
    """Вся история внутренних транзакций на адрес через trace_filter."""
    return sorted(parse_traces(trace_range(address, 0, rpc_latest_block())), key=lambda i: i["block"])


def fetch(address: str, start_block: int):
    if SOURCE == "trace":
        return fetch_trace(address, start_block)
    if SOURCE == "etherscan":
        return fetch_etherscan(address, start_block)
    if use_blockscout():
        try:
            result = fetch_blockscout(address, start_block)
            blockscout_ok()
            return result
        except BlockscoutDown as e:
            blockscout_failed(e)
    return fetch_trace(address, start_block)


def history(address: str) -> list:
    """Вся история входящих внутренних транзакций (для кнопок меню)."""
    if use_blockscout():
        try:
            items, _ = fetch_blockscout(address, 0, max_pages=SENDERS_MAX_PAGES)
            blockscout_ok()
            return items
        except BlockscoutDown as e:
            blockscout_failed(e)
    return trace_history(address)


# ---------------------------------------------------------------- балансы

_price_cache = {"usd": 0.0, "time": 0.0}


def eth_price() -> float:
    """Цена ETH в долларах (для запасного режима). Кэш на 5 минут."""
    if time.time() - _price_cache["time"] < 300 and _price_cache["usd"]:
        return _price_cache["usd"]
    sources = [
        ("https://api.coinbase.com/v2/prices/ETH-USD/spot", lambda d: d["data"]["amount"]),
        ("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
         lambda d: d["ethereum"]["usd"]),
        ("https://api.kraken.com/0/public/Ticker?pair=ETHUSD",
         lambda d: next(iter(d["result"].values()))["c"][0]),
    ]
    for url, extract in sources:
        try:
            price = float(extract(http.get(url, timeout=15).json()))
            if price > 0:
                _price_cache.update(usd=price, time=time.time())
                return price
        except Exception:
            continue
    return _price_cache["usd"]  # последняя известная цена (или 0)


def address_balance(addr: str, with_tokens: bool = True) -> dict:
    """ETH-баланс адреса, создатель (для контрактов) и крупнейшие токены.
    Основной источник — Blockscout; если он лежит — публичные узлы Ethereum (без токенов)."""
    if use_blockscout("addr"):
        try:
            info = blockscout_get(f"addresses/{addr}", attempts=3)
            wei = int(info.get("coin_balance") or 0)
            eth = wei / 1e18
            rate = float(info.get("exchange_rate") or 0)
            creator = (info.get("creator_address_hash") or "").lower()
            result = {
                "address": addr.lower(),
                "eth": eth,
                "usd": eth * rate,
                "is_contract": bool(info.get("is_contract")),
                "name": info.get("name") or "",
                "creator": creator if creator else ("" if not info.get("is_contract") else None),
                "tokens": [],
                "tokens_error": False,
            }
            if with_tokens:
                add_tokens(result)
            return result
        except BlockscoutDown as e:
            blockscout_failed(e, "addr")

    wei = int(rpc("eth_getBalance", [addr, "latest"]), 16)
    code = rpc("eth_getCode", [addr, "latest"]) or "0x"
    eth = wei / 1e18
    return {
        "address": addr.lower(),
        "eth": eth,
        "usd": eth * eth_price(),
        "is_contract": code not in ("0x", "0x0"),
        "name": "",
        "creator": None,  # None = неизвестен, узнаём через find_creator()
        "tokens": [],
        "tokens_error": False,
    }


def find_creator(contract: str, hint_block: int = 0) -> str:
    """Кто создал контракт (кошелёк, отправивший транзакцию создания).
    hint_block — блок, где контракт точно уже существовал (когда прислал 0 ETH).
    Ответ кэшируется: создатель контракта никогда не меняется."""
    contract = contract.lower()
    cache = state.setdefault("creators", {})
    if contract in cache:
        return cache[contract]

    def has_code(block: int) -> bool:
        return rpc("eth_getCode", [contract, hex(block)]) not in ("0x", "0x0", None)

    hi = hint_block or rpc_latest_block()
    if not has_code(hi):
        hi = rpc_latest_block()
        if not has_code(hi):
            return ""
    # Шагаем назад всё большими шагами, пока код не пропадёт: такие контракты обычно
    # созданы недавно, поэтому это быстрее, чем искать по всей истории сети.
    step, lo = 1000, 0
    while hi - step > 0:
        if has_code(hi - step):
            hi -= step
            step *= 8
        else:
            lo = hi - step
            break
    # Двоичный поиск первого блока с кодом в диапазоне (lo, hi].
    while lo < hi:
        mid = (lo + hi) // 2
        if has_code(mid):
            hi = mid
        else:
            lo = mid + 1

    tx_hash = ""
    for t in rpc("trace_block", [hex(lo)]) or []:
        if t.get("type") == "create" and ((t.get("result") or {}).get("address") or "").lower() == contract:
            tx_hash = t.get("transactionHash", "")
            break
    creator = ""
    if tx_hash:
        creator = ((rpc("eth_getTransactionByHash", [tx_hash]) or {}).get("from") or "").lower()
    if creator:
        cache[contract] = creator
        save_state()
    return creator


def creator_of(b: dict, hint_block: int = 0) -> str:
    """Создатель для отчётов: из Blockscout, а если там неизвестен — через узлы Ethereum."""
    if not b["is_contract"]:
        return b["address"]  # прислал обычный кошелёк — он и есть адрес
    if b.get("creator"):
        return b["creator"]
    try:
        return find_creator(b["address"], hint_block)
    except Exception as e:
        log.warning("Не удалось найти создателя %s: %s", b["address"], safe(e))
        return None  # None = сбой (можно повторить), "" = создателя нет


def add_tokens(result: dict) -> None:
    """Дозагружает до 5 крупнейших токенов с известной ценой."""
    addr = result["address"]
    if not use_blockscout("addr"):
        return  # токены умеет показывать только Blockscout
    try:  # у огромных адресов список токенов может не загрузиться — это не страшно
        data = blockscout_get(f"addresses/{addr}/tokens", {"type": "ERC-20"}, attempts=1, timeout=20)
        tokens = []
        for t in data.get("items") or []:
            token = t.get("token") or {}
            token_rate = float(token.get("exchange_rate") or 0)
            if not token_rate or token.get("reputation") not in (None, "ok"):
                continue  # без цены или помечен как скам — пропускаем
            amount = int(t.get("value") or 0) / 10 ** int(token.get("decimals") or 18)
            usd = amount * token_rate
            if usd >= 1:
                tokens.append({"symbol": token.get("symbol") or "?", "amount": amount, "usd": usd})
        tokens.sort(key=lambda x: x["usd"], reverse=True)
        result["tokens"] = tokens[:5]
    except Exception:
        result["tokens_error"] = True
    return result


def format_balance(b: dict, title: str) -> str:
    kind = "контракт" if b["is_contract"] else "кошелёк"
    name = f" «{html.escape(b['name'])}»" if b["name"] else ""
    lines = [
        f"{title} ({kind}{name}):",
        f"<code>{b['address']}</code>",
        f"   ETH: <b>{b['eth']:.6f}</b> (~${b['usd']:,.2f})",
    ]
    for t in b["tokens"]:
        lines.append(f"   {html.escape(t['symbol'])}: {t['amount']:,.4f} (~${t['usd']:,.2f})")
    if b["tokens_error"]:
        lines.append("   токены: не удалось загрузить")
    lines.append(f"   <a href=\"{EXPLORER_URL}/address/{b['address']}\">открыть в Etherscan</a>")
    return "\n".join(lines)


def balances_parallel(addresses) -> dict:
    """Балансы (без токенов) для многих адресов сразу; неудачные повторяет по одному."""
    def get(a):
        try:
            return address_balance(a, with_tokens=False)
        except Exception as e:
            return e

    addresses = list(dict.fromkeys(addresses))
    with ThreadPoolExecutor(max_workers=3) as pool:
        result = dict(zip(addresses, pool.map(get, addresses)))
    for a in [a for a, b in result.items() if isinstance(b, Exception)]:
        time.sleep(1)  # Blockscout под нагрузкой иногда отвечает ошибкой
        result[a] = get(a)
    return result


def with_tokens(balances: list) -> list:
    """Токены грузим только для тех адресов, что попадут в отчёт (это медленно)."""
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(add_tokens, balances))
    return balances


def collect_senders(addr: str, progress=None) -> dict:
    """Все отправители нулевых транзакций на кошелёк и их балансы.
    Результат кэшируется на 10 минут, чтобы вторая кнопка отвечала быстро."""
    cache = runtime.setdefault("senders_cache", {})
    cached = cache.get(addr)
    if cached and time.time() - cached["time"] < 600:
        return cached

    items = history(addr)
    zero = [i for i in items if i["to"].lower() == addr and i["value"] == 0]
    zero.sort(key=lambda i: i["block"], reverse=True)
    recent = zero[:SENDERS_LIMIT] if SENDERS_LIMIT else zero

    counts = {}
    first_block = {}  # когда отправитель впервые прислал 0 ETH — к этому блоку контракт уже существовал
    for i in recent:
        sender = i["from"].lower()
        counts[sender] = counts.get(sender, 0) + 1
        first_block[sender] = min(first_block.get(sender, i["block"]), i["block"])
    if progress and recent:
        progress(f"Нашёл нулевых транзакций: {len(recent)}, отправителей: {len(counts)}. "
                 f"Собираю балансы…")

    balances = balances_parallel(counts)
    result = {"time": time.time(), "tx_count": len(recent), "counts": counts,
              "first_block": first_block, "balances": balances,
              "fallback": bool(runtime.get("fallback"))}
    cache[addr] = result
    return result


def fallback_note(data: dict) -> str:
    if not data.get("fallback"):
        return ""
    return ("\n⚠️ Blockscout сейчас недоступен — данные с публичных узлов Ethereum, "
            "токены не показаны (только ETH).")


def contracts_report(addr: str, progress=None) -> str:
    """Кнопка «Контракты»: контракты, приславшие 0 ETH, у которых есть ETH."""
    data = collect_senders(addr, progress)
    if not data["tx_count"]:
        return f"На <code>{addr}</code> нулевых транзакций не найдено."

    contracts = {a: b for a, b in data["balances"].items()
                 if isinstance(b, Exception) or b["is_contract"]}
    good = sorted((b for b in contracts.values() if not isinstance(b, Exception) and b["eth"] > 0),
                  key=lambda b: b["eth"], reverse=True)
    failed = [a for a, b in contracts.items() if isinstance(b, Exception)]

    blocks = [f"📜 <b>Контракты, приславшие 0 ETH</b> на <code>{addr}</code>\n"
              f"Нулевых транзакций: {data['tx_count']}, контрактов: {len(contracts)}\n"
              f"С балансом ETH: <b>{len(good)}</b> (скрыто с 0 ETH: {len(contracts) - len(good) - len(failed)})"
              + fallback_note(data)]
    for b in with_tokens(good):
        blocks.append(format_balance(b, f"Прислал 0 ETH раз: {data['counts'][b['address']]}"))
    if not good:
        blocks.append("Ни у одного контракта нет ETH на балансе.")
    for a in failed:
        blocks.append(f"<code>{a}</code>: не удалось узнать баланс (сервис не ответил)")
    total = sum(b["eth"] for b in good)
    blocks.append(f"<b>Итого:</b> {total:.6f} ETH (~${sum(b['usd'] for b in good):,.2f})")
    return "\n\n".join(blocks)


def creators_report(addr: str, progress=None) -> str:
    """Кнопка «Создатели»: кошельки, создавшие эти контракты, с балансом больше MIN_CREATOR_USD."""
    data = collect_senders(addr, progress)
    if not data["tx_count"]:
        return f"На <code>{addr}</code> нулевых транзакций не найдено."

    # создатель -> какие контракты он создал и сколько нулевых транзакций они прислали
    created = {}
    unknown = 0       # создатель так и не определился
    lookup_failed = 0  # не определился из-за ограничений/сбоев — можно повторить позже
    known = {a: b for a, b in data["balances"].items() if not isinstance(b, Exception)}
    cache = state.get("creators", {})
    need_lookup = [b for b in known.values()
                   if b["is_contract"] and not b.get("creator") and b["address"] not in cache]
    if need_lookup and progress:
        progress(f"Ищу создателей {len(need_lookup)} контрактов через узлы Ethereum — "
                 f"около {max(1, len(need_lookup) * 4 // 60)} мин…")
    with ThreadPoolExecutor(max_workers=2) as pool:
        owner_of = dict(zip(known, pool.map(
            lambda b: creator_of(b, data.get("first_block", {}).get(b["address"], 0)), known.values())))
    for a, b in data["balances"].items():
        if isinstance(b, Exception):
            unknown += 1
            continue
        owner = owner_of.get(a, "")
        if owner is None:
            lookup_failed += 1
            continue
        if not owner:
            unknown += 1
            continue
        entry = created.setdefault(owner, {"contracts": [], "tx": 0})
        if b["is_contract"]:
            entry["contracts"].append(a)
        entry["tx"] += data["counts"][a]

    owners = balances_parallel(created)
    good = sorted((b for b in owners.values() if not isinstance(b, Exception) and b["usd"] > MIN_CREATOR_USD),
                  key=lambda b: b["usd"], reverse=True)
    failed = [a for a, b in owners.items() if isinstance(b, Exception)]

    blocks = [f"👤 <b>Создатели контрактов</b>, приславших 0 ETH на <code>{addr}</code>\n"
              f"Нулевых транзакций: {data['tx_count']}, создателей: {len(created)}\n"
              f"С балансом больше ${MIN_CREATOR_USD:g}: <b>{len(good)}</b> "
              f"(скрыто: {len(created) - len(good) - len(failed)})" + fallback_note(data)]
    for b in with_tokens(good):
        entry = created[b["address"]]
        lines = [format_balance(b, f"Создатель (его контракты прислали 0 ETH раз: {entry['tx']})")]
        if entry["contracts"]:
            shown = ", ".join(f"<a href=\"{EXPLORER_URL}/address/{c}\">{short(c)}</a>"
                              for c in entry["contracts"][:5])
            more = f" и ещё {len(entry['contracts']) - 5}" if len(entry["contracts"]) > 5 else ""
            lines.append(f"   создал контракты: {shown}{more}")
        blocks.append("\n".join(lines))
    if not good:
        blocks.append(f"Ни у одного создателя нет больше ${MIN_CREATOR_USD:g} в ETH.")
    for a in failed:
        blocks.append(f"<code>{a}</code>: не удалось узнать баланс (сервис не ответил)")
    if lookup_failed:
        blocks.append(f"⏳ Не успел найти создателей у {lookup_failed} контрактов — бесплатные узлы "
                      f"Ethereum ограничили число запросов. Нажмите кнопку ещё раз через 2–3 минуты: "
                      f"уже найденных создателей бот запомнил, будет быстрее.")
        runtime.get("senders_cache", {}).pop(addr, None)  # при повторе — заново
    if unknown:
        blocks.append(f"Не удалось определить создателя у {unknown} контрактов.")
    blocks.append(f"<b>Итого:</b> {sum(b['eth'] for b in good):.6f} ETH "
                  f"(~${sum(b['usd'] for b in good):,.2f})")
    return "\n\n".join(blocks)


def contract_button_report(sender: str) -> str:
    """Кнопка под уведомлением: контракт-отправитель этой транзакции."""
    b = address_balance(sender)
    if b["eth"] <= 0:
        return f"📜 Контракт <code>{b['address']}</code>: 0 ETH на балансе."
    return format_balance(b, "📜 Отправитель 0 ETH")


def creator_button_report(sender: str, block: int = 0) -> str:
    """Кнопка под уведомлением: кто создал контракт-отправитель."""
    s = address_balance(sender, with_tokens=False)
    owner = creator_of(s, block)
    if owner is None:
        return ("⏳ Бесплатные узлы Ethereum сейчас ограничили число запросов. "
                "Нажмите кнопку ещё раз через минуту.")
    if not owner:
        return f"👤 Не удалось определить создателя контракта <code>{s['address']}</code>."
    b = address_balance(owner)
    if b["usd"] <= MIN_CREATOR_USD:
        return (f"👤 Создатель <code>{owner}</code>: в ETH меньше ${MIN_CREATOR_USD:g} "
                f"({b['eth']:.6f} ETH, ~${b['usd']:,.2f}).")
    return format_balance(b, f"👤 Создатель контракта {short(s['address'])}")


# ---------------------------------------------------------------- логика

def short(addr: str) -> str:
    return f"{addr[:8]}…{addr[-6:]}" if len(addr) > 16 else addr


def format_item(addr: str, wallet: dict, item: dict) -> str:
    label = wallet.get("label")
    title = "⚠️ <b>Нулевая транзакция</b>" if item["value"] == 0 else \
        f"💸 <b>Внутренняя транзакция: {item['value'] / 1e18:.6f} ETH</b>"
    lines = [
        title,
        f"Кошелёк: <code>{addr}</code>" + (f" ({html.escape(label)})" if label else ""),
        f"От: <a href=\"{EXPLORER_URL}/address/{item['from']}\">{short(item['from'])}</a>",
        f"Блок: <a href=\"{EXPLORER_URL}/block/{item['block']}\">{item['block']}</a>",
    ]
    if item.get("time"):
        ts = datetime.fromtimestamp(item["time"], tz=timezone.utc).strftime("%d.%m.%Y %H:%M:%S UTC")
        lines.append(f"Время: {ts}")
    if item.get("error"):
        lines.append("Статус: ❌ ошибка выполнения")
    lines.append(f"Транзакция: <a href=\"{EXPLORER_URL}/tx/{item['hash']}\">{short(item['hash'])}</a>")
    return "\n".join(lines)


def balance_button(item: dict) -> dict:
    """Кнопка «баланс отправителя». В callback_data влезает только 64 байта,
    поэтому храним адрес и хеш в state под коротким номером."""
    seq = state.get("button_seq", 0) + 1
    state["button_seq"] = seq
    buttons = state.setdefault("buttons", {})
    buttons[str(seq)] = {"from": item["from"], "hash": item["hash"], "block": item["block"]}
    for old in list(buttons)[:-BUTTONS_LIMIT]:
        buttons.pop(old, None)
    return {"inline_keyboard": [[
        {"text": "📜 Контракт", "callback_data": f"con:{seq}"},
        {"text": "👤 Создатель", "callback_data": f"cre:{seq}"},
    ]]}


def check_wallet(addr: str) -> int:
    wallet = state["wallets"][addr]
    items, next_block = fetch(addr, wallet["next_block"])

    seen = wallet.setdefault("seen", [])
    seen_set = set(seen)
    new = []
    for item in items:
        if item["key"] in seen_set:
            continue
        seen.append(item["key"])
        seen_set.add(item["key"])
        if item["to"].lower() != addr:
            continue
        if NOTIFY_ALL or item["value"] == 0:
            new.append(item)

    del seen[:-SEEN_LIMIT]
    wallet["next_block"] = next_block

    new.sort(key=lambda i: i["block"])
    if len(new) > MAX_MESSAGES_PER_CHECK:
        notify_admins(f"⚠️ На <code>{addr}</code> пришло {len(new)} новых нулевых транзакций. "
                      f"Показываю последние {MAX_MESSAGES_PER_CHECK}.")
        new = new[-MAX_MESSAGES_PER_CHECK:]
    for item in new:
        notify_admins(format_item(addr, wallet, item), balance_button(item))
    save_state()
    return len(new)


def check_all() -> None:
    errors = []
    for addr in list(state["wallets"]):
        try:
            found = check_wallet(addr)
            if found:
                log.info("%s: найдено новых транзакций: %d", addr, found)
        except Exception as e:
            errors.append(f"{short(addr)}: {safe(e)}")
            log.error("Ошибка проверки %s: %s", addr, safe(e))
        time.sleep(0.3)  # бережём лимиты API

    runtime["last_check"] = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    if errors:
        runtime["last_error"] = "; ".join(errors)
        runtime["error_streak"] += 1
        # Пропущенные проверки не теряют транзакции (бот догонит их позже),
        # поэтому пишем админу, только если сбой длится долго — 15 проверок подряд.
        if runtime["error_streak"] >= 15 and not runtime["error_notified"]:
            notify_admins("❗ Бот не может проверить кошельки уже 15 раз подряд "
                          "(транзакции не потеряются — догоню, когда сервисы оживут):\n"
                          f"<code>{html.escape(runtime['last_error'][:1500])}</code>")
            runtime["error_notified"] = True
    else:
        if runtime["error_notified"]:
            notify_admins("✅ Проверка кошельков снова работает.")
        runtime.update(last_error=None, error_streak=0, error_notified=False)


def add_wallet(addr: str, label: str = "") -> str:
    if not ADDRESS_RE.match(addr):
        return "Неверный адрес. Нужен формат 0x + 40 символов."
    addr = addr.lower()
    if addr in state["wallets"]:
        if label:
            state["wallets"][addr]["label"] = label
            save_state()
            return f"Метка для <code>{addr}</code> обновлена."
        return f"<code>{addr}</code> уже отслеживается."
    # Старт с текущего блока: старая история не присылается, только новые транзакции.
    state["wallets"][addr] = {"next_block": latest_block() + 1, "label": label, "seen": []}
    save_state()
    return f"✅ Добавлен <code>{addr}</code>. Сообщу о новых нулевых транзакциях."


def diagnose(address: str) -> str:
    """Что видят источники по кошельку за последнее время — для команды /test."""
    addr = address.lower()
    lines = [f"🔎 <b>Проверка</b> <code>{addr}</code>"]

    def summarize(name, items):
        incoming = [i for i in items if i["to"].lower() == addr]
        zero = [i for i in incoming if i["value"] == 0]
        line = f"<b>{name}</b>: входящих внутренних {len(incoming)}, из них нулевых {len(zero)}"
        if zero:
            last = max(zero, key=lambda i: i["block"])
            line += (f"\n   последняя нулевая: блок {last['block']}, "
                     f"<a href=\"{EXPLORER_URL}/tx/{last['hash']}\">{short(last['hash'])}</a>")
        return line

    try:
        items, _ = fetch_blockscout(addr, 0, max_pages=1)
        lines.append(summarize("Blockscout (последние 50)", items))
    except Exception as e:
        lines.append(f"<b>Blockscout</b>: ошибка <code>{html.escape(safe(e))[:300]}</code>")

    try:
        latest = rpc_latest_block()
        items = parse_traces(trace_range(addr, latest - 10000, latest))
        lines.append(summarize("Запасной источник — узлы Ethereum (последние ~33 часа)", items))
    except Exception as e:
        lines.append(f"<b>Запасной источник</b>: ошибка <code>{html.escape(safe(e))[:300]}</code>")

    if ETHERSCAN_API_KEY:
        try:
            data = etherscan_get(module="account", action="txlistinternal", address=addr,
                                 page=1, offset=50, sort="desc")
            res = data.get("result") if isinstance(data.get("result"), list) else []
            items = [{"to": t.get("to", ""), "value": int(t.get("value") or 0),
                      "block": int(t["blockNumber"]), "hash": t["hash"]} for t in res]
            note = ""
            if data.get("status") != "1" and res == [] and "No transactions" not in str(data.get("message")):
                note = f" (ответ: {html.escape(str(data.get('result')))[:200]})"
            lines.append(summarize("Etherscan (последние 50)", items) + note)
        except Exception as e:
            lines.append(f"<b>Etherscan</b>: ошибка <code>{html.escape(safe(e))[:300]}</code>")

    w = state["wallets"].get(addr)
    if w:
        lines.append(f"\nБот следит с блока {w['next_block']} и сообщает только о том, что пришло после.")
    else:
        lines.append("\nЭтот кошелёк не в списке слежения — добавьте /add.")
    lines.append(f"Источник бота: <b>{source_label()}</b>")
    return "\n".join(lines)


def source_label() -> str:
    if SOURCE == "blockscout" and runtime.get("fallback"):
        return "запасной (узлы Ethereum) — Blockscout недоступен"
    return SOURCE


# ---------------------------------------------------------------- команды

HELP = (
    "<b>Бот слежения за нулевыми транзакциями ETH</b>\n\n"
    "/add &lt;адрес&gt; [метка] — следить за кошельком\n"
    "/remove &lt;адрес&gt; — перестать следить\n"
    "/list — список кошельков\n"
    "/contracts [адрес] — контракты, приславшие 0 ETH (только с ETH на балансе)\n"
    "/creators [адрес] — кто создал эти контракты (только с балансом больше $"
    f"{MIN_CREATOR_USD:g})\n"
    "/test [адрес] — что видят источники данных по кошельку\n"
    "/check — проверить прямо сейчас\n"
    "/status — состояние бота\n\n"
    "Под каждым уведомлением есть кнопки «📜 Контракт» и «👤 Создатель»."
)


def run_for_wallets(chat_id: int, args: list, func, usage: str) -> None:
    targets = [args[0]] if args else list(state["wallets"])
    if not targets:
        send(chat_id, usage)
        return
    for addr in targets:
        if not ADDRESS_RE.match(addr):
            send(chat_id, "Неверный адрес. Нужен формат 0x + 40 символов.")
            continue
        try:
            send_long(chat_id, func(addr.lower()))
        except Exception as e:
            send(chat_id, error_text(e))


def error_text(e: Exception) -> str:
    if isinstance(e, BlockscoutDown):
        return (f"⏳ {html.escape(str(e))}. Это сбой на стороне сервиса Blockscout, а не бота. "
                "Попробуйте через 10–15 минут.")
    if isinstance(e, (RpcRateLimited, requests.RequestException)):
        return ("⏳ Бесплатные узлы Ethereum сейчас перегружены и просят подождать. "
                "Нажмите кнопку ещё раз через 2–3 минуты.")
    return f"Ошибка: <code>{html.escape(safe(e))[:500]}</code>"


def handle_message(msg: dict) -> None:
    chat_id = msg["chat"]["id"]
    user_id = msg.get("from", {}).get("id")
    text = (msg.get("text") or "").strip()
    # Кнопки нижнего меню присылают обычный текст — превращаем его в команды.
    text = {MENU_CONTRACTS: "/contracts", MENU_CREATORS: "/creators",
            MENU_TEST: "/test", MENU_LIST: "/list"}.get(text, text)
    if not text.startswith("/"):
        return

    if user_id not in ADMIN_IDS:
        send(chat_id, f"Нет доступа. Ваш Telegram ID: <code>{user_id}</code>")
        return

    parts = text.split()
    cmd = parts[0].split("@")[0].lower()
    args = parts[1:]

    if cmd in ("/start", "/help"):
        send(chat_id, HELP + f"\n\nВаш ID: <code>{user_id}</code>", MENU_KEYBOARD)
    elif cmd == "/add":
        if not args:
            send(chat_id, "Использование: /add 0x... [метка]")
            return
        try:
            send(chat_id, add_wallet(args[0], " ".join(args[1:])))
        except Exception as e:
            send(chat_id, f"Ошибка: <code>{html.escape(safe(e))}</code>")
    elif cmd == "/remove":
        addr = args[0].lower() if args else ""
        if state["wallets"].pop(addr, None) is not None:
            save_state()
            send(chat_id, f"Удалён <code>{addr}</code>.")
        else:
            send(chat_id, "Такого кошелька нет в списке. Использование: /remove 0x...")
    elif cmd == "/list":
        if not state["wallets"]:
            send(chat_id, "Список пуст. Добавьте кошелёк: /add 0x...")
            return
        rows = [f"• <code>{a}</code>" + (f" — {html.escape(w['label'])}" if w.get("label") else "")
                for a, w in state["wallets"].items()]
        send(chat_id, "<b>Отслеживаемые кошельки:</b>\n" + "\n".join(rows))
    elif cmd in ("/contracts", "/creators", "/senders"):
        report = creators_report if cmd == "/creators" else contracts_report

        def job():
            send(chat_id, "Смотрю всю историю нулевых транзакций — это может занять несколько минут…")
            run_for_wallets(chat_id, args, lambda a: report(a, lambda t: send(chat_id, t)),
                            f"Использование: {cmd} 0x... (или сначала добавьте кошелёк /add)")
        run_report(chat_id, job)
    elif cmd == "/test":
        run_report(chat_id, lambda: run_for_wallets(
            chat_id, args, diagnose, "Использование: /test 0x... (или сначала добавьте кошелёк /add)"))
    elif cmd == "/check":
        send(chat_id, "Проверяю…")
        check_all()
        send(chat_id, "Готово." if not runtime["last_error"] else
             f"Готово с ошибками: <code>{html.escape(runtime['last_error'][:1500])}</code>")
    elif cmd == "/status":
        send(chat_id,
             f"Источник: <b>{source_label()}</b>\n"
             f"Интервал проверки: {CHECK_INTERVAL} с\n"
             f"Кошельков: {len(state['wallets'])}\n"
             f"Режим: {'все внутренние транзакции' if NOTIFY_ALL else 'только нулевые'}\n"
             f"Последняя проверка: {runtime['last_check'] or 'ещё не было'}\n"
             f"Последняя ошибка: {html.escape(runtime['last_error'] or 'нет')}")
    else:
        send(chat_id, HELP, MENU_KEYBOARD)


_report_lock = threading.Lock()


def run_report(chat_id: int, job) -> None:
    """Долгие отчёты — в фоне: бот сразу подтверждает сообщение Telegram и не «зависает».
    Одновременно идёт только один отчёт, повторное нажатие не запускает второй."""
    if not _report_lock.acquire(blocking=False):
        send(chat_id, "⏳ Уже собираю отчёт — дождитесь его, он придёт сюда.")
        return

    def wrapper():
        try:
            job()
        except Exception as e:
            log.exception("Ошибка отчёта: %s", safe(e))
            send(chat_id, error_text(e))
        finally:
            _report_lock.release()
    threading.Thread(target=wrapper, daemon=True).start()


# ---- только одна копия бота
# Если на хостинге случайно запущено несколько копий с одним токеном, каждая шлёт
# свои уведомления — всё приходит по 2–3 раза. Копии договариваются через Telegram:
# при запуске бот записывает свою метку в скрытое описание (язык «la»), а более
# старые копии, увидев чужую, более новую метку, замолкают.
INSTANCE_ID = f"{time.time():.3f}-{os.getpid()}"
INSTANCE_LANG = "la"


def claim_instance() -> None:
    try:
        tg("setMyShortDescription", short_description=f"instance {INSTANCE_ID}",
           language_code=INSTANCE_LANG)
    except Exception as e:
        log.warning("Не удалось записать метку копии: %s", safe(e))


_instance_cache = {"time": 0.0, "active": True}


def is_active_instance() -> bool:
    """False, если запущена копия новее этой. Спрашиваем Telegram не чаще раза в 20 с."""
    if time.time() - _instance_cache["time"] < 20:
        return _instance_cache["active"]
    _instance_cache.update(time=time.time(), active=_check_instance())
    return _instance_cache["active"]


def _check_instance() -> bool:
    try:
        text = tg("getMyShortDescription", language_code=INSTANCE_LANG).get("short_description", "")
    except Exception:
        return True  # Telegram не ответил — продолжаем работать
    other = text.replace("instance ", "", 1).strip()
    if not other or other == INSTANCE_ID:
        return True
    try:
        if float(other.split("-")[0]) < float(INSTANCE_ID.split("-")[0]):
            claim_instance()  # чужая метка старше нашей — мы новее, забираем
            return True
    except ValueError:
        claim_instance()
        return True
    return False


def handle_callback(cb: dict) -> None:
    user_id = cb.get("from", {}).get("id")
    chat_id = (cb.get("message") or {}).get("chat", {}).get("id", user_id)
    data = cb.get("data") or ""

    if user_id not in ADMIN_IDS:
        tg("answerCallbackQuery", callback_query_id=cb["id"], text="Нет доступа")
        return

    kind, _, key = data.partition(":")
    if kind in ("con", "cre"):
        info = state.get("buttons", {}).get(key)
        if not info:
            tg("answerCallbackQuery", callback_query_id=cb["id"], show_alert=True,
               text="Кнопка устарела (бот перезапускался). Используйте кнопки меню внизу.")
            return
        tg("answerCallbackQuery", callback_query_id=cb["id"], text="Смотрю баланс…")

        def job():
            try:
                if kind == "con":
                    send_long(chat_id, contract_button_report(info["from"]))
                else:
                    send_long(chat_id, creator_button_report(info["from"], info.get("block", 0)))
            except Exception as e:
                send(chat_id, error_text(e))
        threading.Thread(target=job, daemon=True).start()
    else:
        tg("answerCallbackQuery", callback_query_id=cb["id"])


# ---------------------------------------------------------------- запуск

def validate_config() -> bool:
    problems = []
    if not BOT_TOKEN:
        problems.append("BOT_TOKEN не задан")
    if SOURCE not in ("blockscout", "etherscan", "trace"):
        problems.append("SOURCE должен быть blockscout, etherscan или trace")
    if SOURCE == "etherscan" and not ETHERSCAN_API_KEY:
        problems.append("ETHERSCAN_API_KEY не задан (нужен для SOURCE=etherscan)")
    if SOURCE == "trace" and not rpc_urls():
        problems.append("RPC_URL или FALLBACK_RPCS не заданы (нужны для SOURCE=trace)")
    for p in problems:
        log.error("Ошибка настройки: %s", p)
    return not problems


def main() -> None:
    if not validate_config():
        log.error("Задайте переменные окружения (образец — .env.example) и запустите снова.")
        sys.exit(1)

    if not ADMIN_IDS:
        log.warning("ADMIN_IDS не задан: бот только сообщает ID. Напишите ему /start, "
                    "впишите свой ID в ADMIN_IDS и перезапустите.")

    state.setdefault("wallets", {})
    for w in INITIAL_WALLETS:
        try:
            result = add_wallet(w)
            log.info(re.sub(r"<[^>]+>", "", result))
        except Exception as e:
            log.error("Не удалось добавить %s: %s", w, safe(e))

    claim_instance()  # с этого момента более старые копии замолкают
    log.info("Бот запущен. Источник: %s, кошельков: %d", SOURCE, len(state["wallets"]))
    if ADMIN_IDS:
        notify_admins(f"🤖 Бот запущен. Источник: {SOURCE}. Слежу за кошельками: "
                      f"{len(state['wallets'])}. /help — команды.", MENU_KEYBOARD)

    offset = None
    next_check = 0.0
    conflict_notified = 0.0
    while True:
        if not is_active_instance():
            # Запущена более новая копия бота — эта молчит, чтобы не было дублей.
            log.warning("Запущена более новая копия бота — эта копия ждёт и ничего не делает")
            time.sleep(60)
            continue

        if ADMIN_IDS and time.time() >= next_check:
            check_all()
            next_check = time.time() + CHECK_INTERVAL

        wait = int(next_check - time.time()) if ADMIN_IDS else 25
        try:
            updates = tg("getUpdates", offset=offset, timeout=max(1, min(25, wait)),
                         allowed_updates=["message", "callback_query"])
        except Exception as e:
            log.error("Telegram getUpdates: %s", safe(e))
            # «Conflict» — Telegram видит другую запущенную копию с тем же токеном
            if "conflict" in str(e).lower() and time.time() - conflict_notified > 6 * 3600:
                conflict_notified = time.time()
                notify_admins("⚠️ Telegram сообщает, что запущено <b>несколько копий бота</b> с одним "
                              "токеном — поэтому сообщения приходят по 2–3 раза. Откройте bothost "
                              "и оставьте один запущенный бот (остальные остановите или удалите).")
            time.sleep(5)
            continue

        for update in updates:
            offset = update["update_id"] + 1
            try:
                if "message" in update:
                    handle_message(update["message"])
                elif "callback_query" in update:
                    handle_callback(update["callback_query"])
            except Exception as e:
                log.exception("Ошибка обработки: %s", safe(e))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Остановлен.")
