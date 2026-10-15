#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CF 优选 IP 测速系统 - Python 版
适用于 OpenWrt / Kwrt / N1 / 任意 Linux 的 python3（仅用标准库，无第三方依赖）。

功能：拉取多源 -> 按 IP 去重 -> 并发延迟测速(TCP握手,16并发) -> 并发下载测速(TLS+5MB,12并发错峰)
      -> 格式化输出 "IP:端口#地区 中文角标[速度Mbps]" -> 上传 GitHub
      -> DNS 更新时机：时间点测速、每天汇总(day)均不更新 DNS（day 仅生成 24hour.txt 供日常挑选）；7 天汇总(best)更新普通域名，topbest 更新 *best 域名

开源说明：本脚本零第三方依赖，所有可调参数集中在下方「配置」区；密钥一律从同目录 cf_best.conf 读取，
         脚本本体不含任何敏感信息，可直接公开。

用法：
  python3 cf_best.py fetch            # 拉取并去重，生成 candidates.txt + region.txt
  python3 cf_best.py slot 0           # 模拟/执行某个时间点测速，生成并上传 timeX.txt
  python3 cf_best.py day              # 合并当天 time 文件 -> 测速 -> dayN.txt -> 上传
  python3 cf_best.py best             # 合并 day1..day7 -> 测速 -> best.txt -> 上传
  python3 cf_best.py slot 20 --day    # 20点且归档当天
"""

import sys, os, re, time, json, base64, socket, ssl, random, subprocess, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE == '.':
    HERE = '.'

# ---------------- 配置 ----------------
SOURCES = [
    "https://bestcf.pages.dev/tiancheng/all.txt",
    "https://bestcf.pages.dev/random-region/TW/all.txt",
    "https://bestcf.pages.dev/random-region/KR/100.txt",
    "https://bestcf.pages.dev/random-region/HK/100.txt",
    "https://bestcf.pages.dev/random-region/SG/all.txt",
    "https://bestcf.pages.dev/random-region/JP/all.txt",
    "https://bestcf.pages.dev/random-region/US/100.txt",
    "https://bestcf.pages.dev/gslege/SG.txt",
    "https://bestcf.pages.dev/gslege/US.txt",
    "https://bestcf.pages.dev/s5gy/all.txt",
    "https://bestcf.pages.dev/lzj/all.txt",
    "https://raw.githubusercontent.com/svip-s/cloudflare_ip/refs/heads/main/best_ips.txt",
    "https://raw.githubusercontent.com/LancelotRar/best-cf-ips/refs/heads/main/best-cf-ip-scanned-top400.txt",
    "https://raw.githubusercontent.com/kele9988/cf-best-ip/main/web.txt",  # 本机 index.html 同步的优选结果
]

# raw.githubusercontent.com 在部分网络出口会被 RST(Errno 104)/超时，实测直连成功率仅约 1/5。
# 下面按"实测可用性 + 速度"排序，直连失败时逐个回退（2026-10-13 在 cf-best 容器内实测全部命中）。
# {o}=owner {r}=repo {ref}=分支 {path}=文件路径 {raw}=原始完整 URL
GH_RAW_MIRRORS = [
    "https://gh-proxy.com/{raw}",
    "https://fastly.jsdelivr.net/gh/{o}/{r}@{ref}/{path}",
    "https://ghproxy.net/{raw}",
    "https://cdn.jsdelivr.net/gh/{o}/{r}@{ref}/{path}",
]
_GH_RAW_RE = re.compile(
    r"^https?://raw\.githubusercontent\.com/([^/]+)/([^/]+)/(?:refs/heads/)?([^/]+)/(.+)$")

# 测速端点 SNI（用于 TLS 下载测速，代替 speed.cloudflare.com 以规避其限流，可与 index.html 保持一致）
BEST_HOSTS = ["bestcf.cmliussss.hidns.vip", "ns.psb.kdns.fr"]
# Cloudflare DNS 自动更新时按地区创建子域名的地区码集合（按需增删）
DNS_REGIONS = ["HK", "JP", "SG", "US", "TW", "KR"]


def gh_mirror_urls(url):
    """把 raw.githubusercontent.com 链接展开为 [直连, 镜像1, 镜像2, ...]；非 raw 链接原样返回。"""
    urls = [url]
    m = _GH_RAW_RE.match(url)
    if not m:
        return urls
    o, r, ref, path = m.groups()
    for tpl in GH_RAW_MIRRORS:
        try:
            u = tpl.format(o=o, r=r, ref=ref, path=path, raw=url)
        except Exception:
            continue
        if u not in urls:
            urls.append(u)
    return urls


def http_get_src(url, timeout=30):
    """带镜像回退的抓取。

    raw.githubusercontent.com 直连在本网络约 1/5 成功率，失败后会进入 3 次重试
    （含 5s/10s 退避），若每个镜像也各重试 3 次，一个失败源最坏会拖掉数分钟。
    因此这里对 GitHub raw 链接改为「单次快速尝试 + 镜像链」，把失败源的代价压到
    10~15s 级；非 raw 链接仍走原有的 3 次重试策略，行为不变。"""
    cands = gh_mirror_urls(url)
    if len(cands) == 1:
        return http_get(url, timeout=timeout)
    last = None
    for i, u in enumerate(cands):
        try:
            txt = http_get_once(u, timeout=10 if i == 0 else 15)
            if i:
                print(f"  ^ 直连失败, 已由镜像接管: {u.split('/')[2]}")
            return txt
        except Exception as e:
            last = e
    raise last


REGION_CN = {
    "SG": "新加坡", "HK": "香港", "TW": "台湾", "JP": "日本", "US": "美国",
    "KR": "韩国", "DE": "德国", "IN": "印度", "NL": "荷兰", "FR": "法国",
    "GB": "英国", "CA": "加拿大", "AU": "澳大利亚", "MY": "马来西亚", "ID": "印尼",
    "TH": "泰国", "VN": "越南", "PH": "菲律宾", "RU": "俄罗斯", "BR": "巴西",
    "SG2": "新加坡", "HK2": "香港", "JP2": "日本", "US2": "美国",
}
SUP = str.maketrans("0123456789", "\u2070\u00b9\u00b2\u00b3\u2074\u2075\u2076\u2077\u2078\u2079")

# Cloudflare 机房三字码(IATA) -> 国家ISO码。照抄 index.html 的 countryFromColo(colo) 思路：
# 实时请求 /cdn-cgi/trace 拿 colo，查表得国家码。覆盖常见/优选机房，查不到返回空走源文本兜底。
COLO_CN = {
    "HKG": "HK", "SIN": "SG", "NRT": "JP", "HND": "JP", "KIX": "JP", "ITM": "JP", "FUK": "JP", "NGO": "JP",
    "ICN": "KR", "GMP": "KR", "PUS": "KR",
    "TPE": "TW", "KHH": "TW", "RMQ": "TW",
    "LAX": "US", "SFO": "US", "SEA": "US", "PDX": "US", "DEN": "US", "SLC": "US", "PHX": "US",
    "SAN": "US", "SJC": "US", "OAK": "US", "LAS": "US", "IAD": "US", "EWR": "US", "JFK": "US",
    "LGA": "US", "ORD": "US", "MDW": "US", "ATL": "US", "MIA": "US", "BOS": "US", "DFW": "US",
    "IAH": "US", "AUS": "US", "MSP": "US", "DTW": "US", "CLT": "US", "BWI": "US", "TPA": "US",
    "FRA": "DE", "MUC": "DE", "DUS": "DE", "HAM": "DE", "BER": "DE", "STR": "DE", "CGN": "DE",
    "LHR": "GB", "LGW": "GB", "MAN": "GB", "EDI": "GB", "BHX": "GB", "GLA": "GB",
    "CDG": "FR", "ORY": "FR", "LYS": "FR", "MRS": "FR", "NCE": "FR",
    "AMS": "NL", "EIN": "NL", "MAD": "ES", "BCN": "ES", "MXP": "IT", "FCO": "IT", "LIN": "IT", "NAP": "IT",
    "WAW": "PL", "KRK": "PL", "ARN": "SE", "HEL": "FI", "CPH": "DK", "OSL": "NO", "PRG": "CZ",
    "VIE": "AT", "ZRH": "CH", "GVA": "CH", "LIS": "PT", "IST": "TR", "SAW": "TR", "ATH": "GR",
    "OTP": "RO", "BUD": "HU", "SOF": "BG", "DXB": "AE", "AUH": "AE", "SHJ": "AE", "RUH": "SA", "JED": "SA",
    "TLV": "IL", "SYD": "AU", "MEL": "AU", "BNE": "AU", "PER": "AU", "ADL": "AU", "CBR": "AU", "OOL": "AU",
    "AKL": "NZ", "CHC": "NZ", "BOM": "IN", "DEL": "IN", "MAA": "IN", "BLR": "IN", "HYD": "IN", "CCU": "IN",
    "DAC": "BD", "BKK": "TH", "DMK": "TH", "HKT": "TH", "CNX": "TH", "SGN": "VN", "HAN": "VN", "DAD": "VN",
    "KUL": "MY", "PEN": "MY", "CGK": "ID", "DPS": "ID", "MNL": "PH", "CEB": "PH", "CMB": "LK",
    "YYZ": "CA", "YVR": "CA", "YUL": "CA", "YYC": "CA", "YOW": "CA", "YEG": "CA",
    "GRU": "BR", "GIG": "BR", "BSB": "BR", "EZE": "AR", "SCL": "CL", "MEX": "MX", "GDL": "MX", "MTY": "MX",
    "PTY": "PA", "SJO": "CR", "JNB": "ZA", "CPT": "ZA", "NBO": "KE", "LOS": "NG", "CAI": "EG",
    "KHI": "PK", "LHE": "PK", "ISB": "PK", "KBP": "UA", "LED": "RU", "DME": "RU", "SVO": "RU",
}

# 测速参数（对齐你的需求：延迟16并发防风控，下载3并发不超300M带宽）
LATENCY_THREADS = 16
LATENCY_TIMEOUT = 0.5    # 秒(对齐 index.html 500ms 超时)
LATENCY_SAMPLES = 1      # 每 IP 延迟采样次数(对齐 index.html:一次探测即可)
DL_THREADS = 12
DL_BYTES = 5000000      # 5MB 下载测速（单节点测速足够）
DL_TIMEOUT = 10          # 秒
MIN_MBPS = 10            # 只保留 ≥ 该速度(Mbps) 的结果，过滤 0.00Mbps 等低速（同 index.html 达标速度）
SLOT_HOURS = [0, 4, 8, 12, 16, 20]

STATE_FILE = os.path.join(HERE, "state.json")
CONF_FILE = os.path.join(HERE, "cf_best.conf")
# ---- 中档增量池：每期抓源合并 + 连续未达标淘汰 ----
POOL_STATS_FILE = os.path.join(HERE, "pool_stats.json")   # 池内每IP统计：consec_fail/seen/hit
PRUNE_N = 7          # 连续 N 期未达标 -> 移出候选池
POOL_MAX = 5000      # 候选池上限，超出按 hit 数截断

_code_map_cache = {}
_code_map_sig = -1

def code_of_map():
    """地区码查找表：pool_stats.json 持久化的地区码(历史池IP) + region.txt(当次源解析) 合并。
    pool 兜底、region 当次覆盖：历史池IP地区码不因 region.txt 每期重写而丢失。
    带 mtime 缓存：同一进程内文件未变时不重复解析大 JSON(pool_stats)。"""
    global _code_map_sig
    sig = 0
    for fp in (POOL_STATS_FILE, os.path.join(HERE, "region.txt")):
        try:
            sig = sig * 31 + int(os.path.getmtime(fp))
        except OSError:
            sig = sig * 31
    if sig != _code_map_sig:
        cmap = {}
        if os.path.exists(POOL_STATS_FILE):
            try:
                for k, r in json.load(open(POOL_STATS_FILE, encoding="utf-8")).items():
                    c = (r or {}).get("code", "") if isinstance(r, dict) else ""
                    if c:
                        cmap.setdefault(k, c)
            except Exception:
                pass
        rp = os.path.join(HERE, "region.txt")
        if os.path.exists(rp):
            for line in open(rp, encoding="utf-8"):
                k, _, v = line.rstrip("\n").partition(",")
                if v:
                    cmap[k] = v
        _code_map_cache.clear()
        _code_map_cache.update(cmap)
        _code_map_sig = sig
    return _code_map_cache

def load_conf():
    conf = {}
    if os.path.exists(CONF_FILE):
        for line in open(CONF_FILE, encoding="utf-8"):
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                conf[k.strip()] = v.strip()
    return conf

def reflux_url():
    # 方案A: 按 remote_dir 回流"本节点"上一轮上传的 best.txt
    #   N1(remote_dir 空) -> main/best.txt ; 飞牛(remote_dir=fnos) -> main/fnos/best.txt
    conf = load_conf()
    owner, repo = conf.get("owner"), conf.get("repo")
    if not (owner and repo):
        return None
    rd = (conf.get("remote_dir") or "").strip().strip("/")
    path = "best.txt" if not rd else rd + "/best.txt"
    return f"https://raw.githubusercontent.com/{owner}/{repo}/main/{path}"

# ---------------- 网络下载 ----------------
def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 cf-best"})
    with urlopen_retry(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def http_get_once(url, timeout=15):
    """单次尝试、不做重试与退避 —— 供镜像链使用，避免失败源拖长整轮时间。"""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 cf-best"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def fetch_all():
    seen = {}          # key -> code
    sources = list(SOURCES)
    ru = reflux_url()
    if ru:
        sources.append(ru)   # 回流本节点上一轮 best.txt, 优质自沉淀/劣质自淘汰
    for url in sources:
        try:
            txt = http_get_src(url)
        except Exception as e:
            print("ERR", url, e); continue
        print("OK  ", url)
        for m in re.finditer(r"(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})", txt):
            ip, port = m.group(1), m.group(2)
            code = ""
            cm = re.search(r"\b(TW|HK|JP|SG|US|KR|DE|IN|NL|FR|GB|CA|AU|MY|ID|TH|VN|PH|RU|BR)\b", txt[max(0, m.start()-80):m.end()+80])
            if cm: code = cm.group(1)
            seen.setdefault(f"{ip}:{port}", code)
    cands = [f"{k}#{v}" if v else k for k, v in seen.items()]
    with open(os.path.join(HERE, "candidates.txt"), "w") as f:
        f.write("\n".join(cands))
    with open(os.path.join(HERE, "region.txt"), "w") as f:
        for k, v in seen.items():
            f.write(f"{k},{v}\n")
    print(f"dedup={len(cands)} -> candidates.txt")
    return cands

# ---------------- 延迟测速 ----------------
def measure_one(item):
    ip, port = item.split(":")[0], int(item.split(":")[1])
    best = None
    for _ in range(LATENCY_SAMPLES):
        try:
            t0 = time.perf_counter()
            with socket.create_connection((ip, port), timeout=LATENCY_TIMEOUT):
                dt = (time.perf_counter() - t0) * 1000
            best = dt if best is None else min(best, dt)
        except Exception:
            pass
        time.sleep(0.05)
    return item, best

def measure_latency(items):
    reach = {}
    def w(x): return measure_one(x)
    with ThreadPoolExecutor(max_workers=LATENCY_THREADS) as ex:
        for i, (item, rtt) in enumerate(ex.map(w, items)):
            if rtt is not None:
                reach[item] = round(rtt, 0)
            if (i + 1) % 128 == 0 or i + 1 == len(items):
                print(f"latency {i+1}/{len(items)} reachable={len(reach)}")
    return reach

# ---------------- 下载测速 ----------------
def hex_label(ip):
    return "".join(f"{int(p):02X}" for p in ip.split("."))

def dl_one(item):
    ip, port = item.split(":")[0], int(item.split(":")[1])
    label = hex_label(ip)
    for host in BEST_HOSTS:
        sni = f"{label}.{host}"
        url = f"https://{sni}/__down?bytes={DL_BYTES}&_t={int(time.time()*1000)}"
        try:
            # -D - 把响应头输出到stdout(含 cf-ray), 零额外请求拿到 colo -> 国家码(同 GUI)
            r = subprocess.run(
                ["curl", "-k", "-s", "-o", "/dev/null", "-D", "-",
                 "--max-time", str(DL_TIMEOUT),
                 "--connect-to", f"{sni}:{port}:{ip}:{port}",
                 "-w", "\nSPEED=%{speed_download}\n", url],
                capture_output=True, text=True, timeout=DL_TIMEOUT + 2)
            out = r.stdout or ""
            mbps = 0.0
            if "SPEED=" in out:
                try:
                    mbps = float(out.split("SPEED=", 1)[1].splitlines()[0].strip()) * 8 / 1e6
                except ValueError:
                    mbps = 0.0
            # 从响应头 cf-ray 解析 colo -> 国家码
            country = ""
            hdr = out.split("SPEED=", 1)[0]
            for line in hdr.splitlines():
                if line.lower().startswith("cf-ray:"):
                    v = line.split(":", 1)[1].strip()
                    colo = v.rsplit("-", 1)[1].strip().upper() if "-" in v else ""
                    country = COLO_CN.get(colo, "")
                    break
            if mbps > 0:
                return item, 0, round(mbps, 2), country
        except Exception:
            pass
    return item, None, None, ""

def measure_download(items):
    out = []
    STAGGER = 0.2   # 错峰间隔(秒): 同一批并发节点错开启动, 避免瞬时抢带宽
    def w(args):
        i, item = args
        time.sleep((i % DL_THREADS) * STAGGER)
        return dl_one(item)
    with ThreadPoolExecutor(max_workers=DL_THREADS) as ex:
        for i, (item, ms, mbps, country) in enumerate(ex.map(w, enumerate(items))):
            if mbps is not None:
                out.append((item, ms, mbps, country))
            if (i + 1) % 64 == 0 or i + 1 == len(items):
                print(f"dl {i+1}/{len(items)} success={len(out)}")
    return out

# ---------------- 格式化 ----------------
def fmt(idx, item, rtt, mbps):
    ip, port = item.split(":")[0], item.split(":")[1]
    code = code_of_map().get(item, "")
    cn = REGION_CN.get(code, code)
    sup = str(idx).translate(SUP)   # idx 为该地区内序号(从1起)
    if not code:
        cn = "未知"
        return f"{item}#{cn}{sup}[{mbps:.2f}Mbps]"
    return f"{item}#{code} {cn}{sup}[{mbps:.2f}Mbps]"

# ---------------- 完整一轮测速 -> 输出 ----------------
def run_measure(items, outfile, do_dns=False):
    print("==== latency ====")
    reach = measure_latency(items)
    print(f"reachable={len(reach)}")
    print("==== download ====")
    hits = measure_download(list(reach.keys()))
    hits = [h for h in hits if h[2] >= MIN_MBPS]   # 只保留 ≥ MIN_MBPS 的结果，过滤低速
    hits.sort(key=lambda x: -x[2])
    # 按地区分组，地区内单独编号(每个地区从 ¹ 重新数)
    code_of = code_of_map()
    # 地区判定：pool持久化/源文本 优先，下载响应头 cf-ray colo国家兜底(零额外请求,同GUI)，并写回pool
    stats = None
    for item, rtt, mbps, country in hits:
        if not code_of.get(item, "") and country:
            code_of[item] = country
            if stats is None:
                stats = load_pool_stats()
            rec = stats.get(item)
            if isinstance(rec, dict):
                rec["code"] = country
    if stats is not None:
        save_pool_stats(stats)
    cnt = {}
    lines = []
    for item, rtt, mbps, country in hits:
        c = code_of.get(item, "")
        cnt[c] = cnt.get(c, 0) + 1
        lines.append(fmt(cnt[c], item, rtt, mbps))
    with open(outfile, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))
    print(f"pass={len(hits)} -> {outfile}")
    # DNS 更新由调用方控制：普通域名在 7 天汇总(best)时更新、*best 域名在 topbest 时更新；
    # 本函数仅按 do_dns 标志执行，day/时间点测速不触发（day 只产出 24hour.txt）
    if do_dns:
        cf_dns_update(hits)
    return hits

# ---------------- GitHub 上传 ----------------

def urlopen_retry(req, timeout=60, retries=3, base=5):
    """urllib.request.urlopen 加重试+指数退避，覆盖直连出口的间歇性 RST/超时。
    HTTPError(如409/422)立即上抛给调用方逻辑处理，不重试。"""
    last = None
    for i in range(retries):
        try:
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError:
            raise
        except Exception as e:
            last = e
            if i < retries - 1:
                time.sleep(base * (2 ** i))
    raise last

def gh_upload(local, ghpath):
    conf = load_conf()
    owner, repo, token = conf.get("owner"), conf.get("repo"), conf.get("token")
    if not (owner and repo and token):
        print("no cf_best.conf (owner/repo/token)"); return False
    rd = (conf.get("remote_dir") or "").strip().strip("/")
    if rd:
        ghpath = f"{rd}/{ghpath}"
    api = f"https://api.github.com/repos/{owner}/{repo}/contents/{ghpath}"
    data = open(local, "rb").read()
    body = {"message": "update by cf scheduler", "content": base64.b64encode(data).decode()}
    req = urllib.request.Request(api, method="PUT", headers={
        "Authorization": "token " + token,
        "User-Agent": "cf-scheduler",
        "Accept": "application/vnd.github+json",
    }, data=json.dumps(body).encode())
    try:
        with urlopen_retry(req, timeout=60) as r:
            print(f"uploaded {ghpath}"); return True
    except urllib.error.HTTPError as e:
        if e.code in (409, 422):   # 409 或 422(文件已存在缺 sha) 都取 sha 覆盖
            # 获取 sha 再覆盖
            g = urllib.request.Request(api, headers={"Authorization": "token "+token, "User-Agent": "cf-scheduler"})
            try:
                with urlopen_retry(g, timeout=60) as r:
                    sha = json.load(r).get("sha")
                body["sha"] = sha
                req2 = urllib.request.Request(api, method="PUT", headers={
                    "Authorization": "token "+token, "User-Agent": "cf-scheduler",
                    "Accept": "application/vnd.github+json"}, data=json.dumps(body).encode())
                with urlopen_retry(req2, timeout=60) as r:
                    print(f"updated {ghpath}"); return True
            except Exception as ex:
                print("update fail", ex); return False
        else:
            print("upload fail", e.code, e.read()[:200]); return False
    except Exception as ex:
        print("upload err", ex); return False

# ---------------- Cloudflare DNS 自动更新 ----------------
def cf_dns_update(hits, sub="", per=None):
    """测速完成后自动更新 Cloudflare DNS，每个地区子域名指向最快的 N 个 IP。并发删除+创建。
    sub=""  -> jp.kele9988.dpdns.org (普通)；sub="best" -> jpbest.kele9988.dpdns.org (topbest)"""
    conf = load_conf()
    if not conf.get("cf_dns_enable", "").lower() in ("1", "true", "yes", "on"):
        return
    api_token = conf.get("cf_token", "").strip()
    zone_name = conf.get("cf_zone", "kele9988.dpdns.org").strip()
    if per is not None:
        per_region = per
    else:
        try:
            per_region = int(conf.get("cf_per_region", "20"))
        except ValueError:
            per_region = 20
    if not api_token or not zone_name:
        print("CF DNS: token 或 zone 为空，跳过"); return
    regions = DNS_REGIONS
    code_of = code_of_map()
    by_region = {r: [] for r in regions}
    for h in hits:
        item = h[0]; mbps = h[2]
        code = code_of.get(item, "")
        if code in by_region:
            ip = item.split(":")[0]
            by_region[code].append((ip, mbps))
    headers = {"Authorization": "Bearer " + api_token, "Content-Type": "application/json"}
    try:
        req = urllib.request.Request(
            f"https://api.cloudflare.com/client/v4/zones?name={zone_name}", headers=headers)
        with urlopen_retry(req, timeout=20) as r:
            zj = json.load(r)
        if not zj.get("success") or not zj.get("result"):
            print(f"CF DNS: 获取 zone 失败: {zj}"); return
        zone_id = zj["result"][0]["id"]
        print(f"CF DNS: zone={zone_name} id={zone_id}")
    except Exception as e:
        print(f"CF DNS: 获取 zone 异常: {e}"); return
    base = f"https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records"

    def _del(rec_id):
        try:
            del_req = urllib.request.Request(f"{base}/{rec_id}", headers=headers, method="DELETE")
            urlopen_retry(del_req, timeout=10)
        except Exception:
            pass

    def _create(args):
        subh, ip = args
        try:
            body = json.dumps({"type": "A", "name": subh, "content": ip, "ttl": 60, "proxied": False}).encode()
            creq = urllib.request.Request(base, data=body, headers=headers, method="POST")
            urlopen_retry(creq, timeout=10)
            return True
        except Exception as e:
            print(f"CF DNS: {subh} 创建 {ip} 失败: {e}"); return False

    total = 0
    for code in regions:
        ips = by_region.get(code, [])[:per_region]
        subh = f"{code.lower()}{sub}.{zone_name}"
        if not ips:
            print(f"CF DNS: {code} 无达标IP，跳过 {subh}"); continue
        # 并发删除旧记录
        try:
            req = urllib.request.Request(f"{base}?name={subh}&type=A&per_page=100", headers=headers)
            with urlopen_retry(req, timeout=20) as r:
                old = json.load(r)
            old_ids = [rec["id"] for rec in old.get("result", [])]
            if old_ids:
                with ThreadPoolExecutor(max_workers=8) as ex:
                    list(ex.map(_del, old_ids))
        except Exception:
            pass
        # 并发创建新记录
        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(_create, [(subh, ip) for ip, _ in ips]))
        created = sum(results)
        total += created
        print(f"CF DNS: {subh} <- {created} 个IP (最快 {ips[0][1]:.1f}Mbps)")
    print(f"CF DNS: 完成，共更新 {total} 条记录")

# ---------------- 状态 / 归档 ----------------
def load_state():
    if os.path.exists(STATE_FILE):
        try: return json.load(open(STATE_FILE))
        except Exception: pass
    return {"dayIndex": 1, "date": ""}

def save_state(s):
    json.dump(s, open(STATE_FILE, "w"))

def today():
    return time.strftime("%Y-%m-%d")

def slots_dir():
    d = os.path.join(HERE, "timefiles"); os.makedirs(d, exist_ok=True); return d

def day_dir():
    d = os.path.join(HERE, "dayfiles"); os.makedirs(d, exist_ok=True); return d

# ---------------- 跨时段统计筛选(替代 day 的重测速) ----------------
def cross_aggregate(files, outfile, min_occurs=None, label="aggregate", per_region=None):
    """跨期聚合筛选(不重新测速)：files 里每个文件代表一个"期"(time=时段, day=天, best=周)。
    每个 IP:port 统计: 达标期数(速度≥MIN_MBPS)与出现期数；
    保留: 达标期数≥min_occurs 且 出现期数≥min_occurs（杜绝只在少数期冒尖的假稳定）；
    排序: 最差期速度降序(网络波动也够用=最稳) -> 平均降序；行内显示跨期平均速度。
    返回 hits 列表；DNS 更新由调用方在文件上传成功后进行（保证先上传后更新的一致顺序）。"""
    conf = load_conf()
    if min_occurs is None:
        min_occurs = int(conf.get("min_occurs", 4))   # 默认4: 至少过半期稳定达标
    # 自适应下限：min_occurs 不超过实际存在的期数，避免源文件不足时全部被过滤导致空结果
    nfiles = sum(1 for fp in files if os.path.exists(fp))
    if nfiles:
        min_occurs = max(1, min(min_occurs, nfiles))
    d = {}   # item -> {期: mbps}
    file_code = {}   # item -> 该文件行内的地区码（优先，继承文件已标注的地区）
    for fp in files:
        if not os.path.exists(fp): continue
        slot = os.path.basename(fp)   # 期标识：完整文件名(time0.txt/day1.txt/best_1.txt 均唯一区分)
        for line in open(fp, encoding="utf-8"):
            line = line.strip()
            if not line: continue
            m = re.match(r'^([\d.]+:\d+)#([A-Za-z0-9]*)', line)
            if not m: continue
            item = m.group(1)
            fc = m.group(2).upper()
            if fc and item not in file_code:
                file_code[item] = fc
            m2 = re.search(r'\[([\d.]+)Mbps\]', line)
            if not m2: continue
            d.setdefault(item, {})[slot] = float(m2.group(1))
    rows = []
    for item, speeds in d.items():
        vals = list(speeds.values())
        hit = sum(1 for v in vals if v >= MIN_MBPS)
        occurs = len(vals)
        if hit < min_occurs or occurs < min_occurs: continue
        rows.append((item, min(vals), sum(vals) / len(vals)))   # item, 最差, 平均
    rows.sort(key=lambda x: (-x[1], -x[2]))   # 最差降序 -> 平均降序
    # 按地区分组独立编号：文件内地区码优先，pool/region 兜底
    code_of = code_of_map()
    # 文件内已标注地区但 code_of_map 没有的，临时补进缓存，保证 fmt 输出文件地区(不因 pool/region 缺失而变未知)
    for item, fc in file_code.items():
        if fc and not code_of.get(item, ""):
            code_of[item] = fc
    if per_region is not None:
        _by = {}
        for _it, _w, _a in rows:
            _c = file_code.get(_it, "") or code_of.get(_it, "")
            _by.setdefault(_c, []).append((_it, _w, _a))
        rows = []
        for _c in sorted(_by):
            rows.extend(_by[_c][:per_region])
    cnt, hits, lines = {}, [], []
    for item, worst, avg in rows:
        c = file_code.get(item, "") or code_of.get(item, "")
        cnt[c] = cnt.get(c, 0) + 1
        lines.append(fmt(cnt[c], item, 0, avg))
        hits.append((item, 0, avg))
    with open(outfile, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))
    print(f"{label}: 去重 {len(d)} ip, 保留(达标&出现>={min_occurs}) {len(rows)} -> {outfile}")
    return hits

# ---------------- 每日汇总：每地区最快 N 个IP -> 24hour.txt ----------------
def day_topn(hits, outfile, per=3):
    """每天汇总(day聚合后)：按地区码分地区，每地区取速度最快 per 个IP写入文件。
    地区码：pool 持久化优先、region.txt 当次覆盖。用于仓库 24hour.txt，每天直接覆盖；
    仅供日常快速挑选，不代表跨期最稳。"""
    code_of = code_of_map()
    by_region = {}
    for item, rtt, mbps in hits:
        c = code_of.get(item, "")
        by_region.setdefault(c, []).append((item, mbps))
    lines = []
    for c in sorted(by_region):
        ips = sorted(by_region[c], key=lambda x: -x[1])[:per]   # 每地区按速度降序取 top per
        for i, (item, mbps) in enumerate(ips, 1):
            lines.append(fmt(i, item, 0, mbps))
    with open(outfile, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))
    print(f"day_topn: {sum(len(v) for v in by_region.values())}个达标IP分{len(by_region)}地区，每地区top{per} -> {outfile}")

# ---------------- 中档：增量候选池 + 淘汰 ----------------
def _norm_rec(rec):
    """把 pool 统计记录规范化：兼容旧版本遗留的各种 schema（缺字段/非 dict），
    统一为完整结构，避免 prune_pool/refresh_pool 直接取下标时 KeyError。"""
    if not isinstance(rec, dict):
        rec = {}
    try:
        cf = int(rec.get("consec_fail", 0) or 0)
    except Exception:
        cf = 0
    try:
        seen = int(rec.get("seen", 0) or 0)
    except Exception:
        seen = 0
    try:
        hit = int(rec.get("hit", 0) or 0)
    except Exception:
        hit = 0
    return {"consec_fail": cf, "seen": seen, "hit": hit, "code": (rec.get("code") or "")}

def load_pool_stats():
    raw = {}
    if os.path.exists(POOL_STATS_FILE):
        try:
            raw = json.load(open(POOL_STATS_FILE))
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        return {}
    # 规范化每条记录（兼容历史遗留 schema），后续 save_pool_stats 会自然写回
    return {k: _norm_rec(v) for k, v in raw.items()}

def save_pool_stats(s):
    json.dump(s, open(POOL_STATS_FILE, "w"))

def refresh_pool():
    """每期增量抓源合并进候选池(历史∪最新新IP)，并初始化统计。
    解决：源随机更新下，7天固定池会错过中间出现的新优质IP。"""
    st = load_state()
    cyc = st.get("cycle_id", 1)
    cf = os.path.join(HERE, f"candidates_c{cyc}.txt")
    hist = []
    if os.path.exists(cf):
        hist = [l.strip() for l in open(cf, encoding="utf-8") if l.strip()]
    # 抓源拿最新候选，合并去重（保留历史，吸收新IP）
    cands = fetch_all()
    latest = [c.split("#")[0] for c in cands]
    merged = []
    seen = set(hist)
    for it in latest:
        if it not in seen:
            seen.add(it); merged.append(it)
    all_items = hist + merged
    stats = load_pool_stats()
    if len(all_items) > POOL_MAX:
        # 稳定IP(hit>=1)优先保留；给新IP(hit==0)留试用名额，避免池满时新IP全被砍
        stable = [it for it in all_items if stats.get(it, {}).get("hit", 0) >= 1]
        fresh  = [it for it in all_items if stats.get(it, {}).get("hit", 0) == 0]
        fresh_quota = max(200, POOL_MAX // 5)   # 新IP试用名额（保证新IP有机会进池试测）
        keep_fresh = fresh[:fresh_quota]
        keep_stable = stable[:POOL_MAX - len(keep_fresh)]
        removed = set(all_items) - set(keep_stable) - set(keep_fresh)
        for it in removed:
            stats.pop(it, None)   # 清理被截断移出IP的统计，防孤儿膨胀
        all_items = keep_stable + keep_fresh
    with open(cf, "w", encoding="utf-8") as f:
        f.write("\n".join(all_items) + "\n")
    for it in all_items:
        stats.setdefault(it, {"consec_fail": 0, "seen": 0, "hit": 0})
    # 回填地区码：region.txt 覆盖到的IP持久化其地区码(历史池IP地区不因region重写丢失)
    rp = os.path.join(HERE, "region.txt")
    if os.path.exists(rp):
        for line in open(rp, encoding="utf-8"):
            k, _, v = line.rstrip("\n").partition(",")
            if v and k in stats:
                stats[k]["code"] = v
    save_pool_stats(stats)
    print(f"candidates(中档每期刷新): 历史{len(hist)} + 新增{len(merged)} = {len(all_items)}")
    return all_items

def prune_pool(items, hits, n=PRUNE_N):
    """每期测速后：达标清零连续计数、未达标累加；连续 n 期未达标 -> 移出候选池。"""
    hit_items = {item for item, *_ in hits}
    stats = load_pool_stats()
    to_remove = []
    for item in items:
        rec = stats.get(item)
        if rec is None:
            rec = {"consec_fail": 0, "seen": 0, "hit": 0}; stats[item] = rec
        rec["seen"] += 1
        if item in hit_items:
            rec["consec_fail"] = 0
            rec["hit"] += 1
        else:
            rec["consec_fail"] += 1
            if rec["consec_fail"] >= n:
                to_remove.append(item)
    if to_remove:
        st = load_state(); cyc = st.get("cycle_id", 1)
        cf = os.path.join(HERE, f"candidates_c{cyc}.txt")
        rm = set(to_remove)
        remaining = [it for it in items if it not in rm]
        with open(cf, "w", encoding="utf-8") as f:
            f.write("\n".join(remaining) + "\n")
        for it in to_remove:
            stats.pop(it, None)
        print(f"prune: 淘汰{len(to_remove)}个连续{n}期未达标IP，池剩{len(remaining)}")
    save_pool_stats(stats)

def run_slot(hour, finalize=False):
    print(f"==== slot {hour} ====")
    items = refresh_pool()   # 中档：每期增量抓源合并(历史∪新IP)
    tf = os.path.join(slots_dir(), f"time{hour}.txt")
    hits = run_measure(items, tf, do_dns=False)   # 时间点测速不更新 DNS；DNS 在 7 天汇总(best)时更新
    prune_pool(items, hits)   # 中档：淘汰连续未达标IP
    gh_upload(tf, f"time{hour}.txt")
    st = load_state()
    if today() != st.get("date"):
        # 跨日只更新日期（dayIndex 周期计数由 run_day 按自然日推进）
        st["date"] = today()
    save_state(st)
    if finalize:
        run_day()

def run_day():
    st = load_state()
    if today() != st.get("date"):
        # 跨日只更新日期（dayIndex 周期计数由下方按自然日推进）
        st["date"] = today()
        save_state(st)
    # 直接扫描今天实际生成的 time 文件（按文件 mtime 匹配当天，避免漏 slot）
    tod = today()
    files = []
    for h in SLOT_HOURS:
        fp = os.path.join(slots_dir(), f"time{h}.txt")
        if os.path.exists(fp) and time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(fp))) == tod:
            files.append(fp)
    if not files:
        print("no time files today"); return
    idx = st.get("dayIndex", 1)
    df = os.path.join(day_dir(), f"day{idx}.txt")
    hits = cross_aggregate(files, df, label=f"day{idx}")   # 跨时段筛选，不重测速
    gh_upload(df, f"day{idx}.txt")
    # 普通DNS只在7天汇总(best)更新；每天汇总只生成每地区最快3个IP -> 24hour.txt 覆盖上传
    if hits:
        tf10 = os.path.join(HERE, "24hour.txt")
        day_topn(hits, tf10)
        gh_upload(tf10, "24hour.txt")
    else:
        print("day_topn: 本日无跨时段稳定达标IP，跳过 24hour.txt（保留仓库上次结果）")
    # 修复：dayIndex 按“自然日”推进，而非按 run_day 调用次数推进。
    # 同一自然日重复调用（如多个 slot 都带 --day，或手动重跑 day）只覆盖重写同一 day 文件，不前进槽位；
    # 只有跨到新的一天才把 dayIndex 推进到下一个槽位，并在填满 7 个槽位后触发 best。
    advanced = (st.get("dayRunDate") != tod)
    if advanced:
        st["dayRunDate"] = tod
        st["dayIndex"] = idx + 1
    save_state(st)
    if advanced and idx >= 7:
        run_best()
        st = load_state()
        st["dayIndex"] = 1
        st["dayRunDate"] = ""
        st["cycle_id"] = st.get("cycle_id", 1) + 1   # 进入下一7天周期，新周期固定池在首跑时抓取
        save_state(st)

def run_best():
    files = [os.path.join(day_dir(), f"day{i}.txt") for i in range(1, 8)
             if os.path.exists(os.path.join(day_dir(), f"day{i}.txt"))]
    if not files:
        print("no day files for best"); return
    bf = os.path.join(HERE, "best.txt")
    hits = cross_aggregate(files, bf, label="best", per_region=20)   # 跨 7 天聚合，不重测速
    gh_upload(bf, "best.txt")
    cf_dns_update(hits)   # 上传成功后再更新普通DNS
    # topbest: 每个周期备份 best.txt，累计 4 个后聚合 -> topbest.txt 并更新 *best DNS
    do_topbest()

def do_topbest():
    st = load_state()
    n = int(st.get("bestBackup", 0)) + 1
    bkdir = os.path.join(HERE, "best_backup"); os.makedirs(bkdir, exist_ok=True)
    src = os.path.join(HERE, "best.txt")
    bk = os.path.join(bkdir, f"best_{n}.txt")
    if os.path.exists(src):
        with open(src, "rb") as a, open(bk, "wb") as b:
            b.write(a.read())
    st["bestBackup"] = n
    save_state(st)
    print(f"topbest: 已备份第 {n} 个 best.txt")
    if n >= 4:
        bks = [os.path.join(bkdir, f"best_{i}.txt") for i in range(1, n + 1)
               if os.path.exists(os.path.join(bkdir, f"best_{i}.txt"))]
        tb = os.path.join(HERE, "topbest.txt")
        hits = cross_aggregate(bks, tb, label=f"topbest({n})")   # 跨 4 个 best 聚合
        gh_upload(tb, "topbest.txt")
        # 更新到 *best.kele9988.dpdns.org，每地区速度最快 10 个（上传成功后再更新）
        cf_dns_update(hits, sub="best", per=10)
        # 重置备份计数并清空备份
        st["bestBackup"] = 0
        save_state(st)
        for b in bks:
            try: os.remove(b)
            except Exception: pass
        print("topbest: 完成，重置备份计数，进入下一轮累计")

def _pid_alive(pid):
    """跨平台判断进程是否存活（用于清理僵尸锁文件）。

    pid <= 1 一律视为无效：定时任务是 `docker run --rm` 起的临时容器，容器内 python3
    恒为 PID 1，而宿主机 PID 1 永活 —— 一旦锁文件残留 "1"，os.kill(1,0) 会永远成功，
    导致每一轮 slot 都被误判为"另一实例在跑"而直接跳过（2026-09 曾因此整天空转）。"""
    if not pid or pid <= 1:
        return False
    if os.name == "nt":
        try:
            # 不解码：中文 Windows 的 tasklist 输出不是 UTF-8，text=True 会抛 UnicodeDecodeError
            out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                                 capture_output=True, timeout=5).stdout
            return str(pid).encode() in out
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False

LOCK_STALE_SEC = 90 * 60   # 锁文件超过 90 分钟视为残留（单轮实测耗时 12~24 分钟）

def _lock_is_stale(lockfile, raw):
    """判断锁文件是否属于"已经退出的实例"。

    容器场景下 PID 恒为 1、且每个临时容器的主机名都不同，无法靠 kill(0) 判断，
    因此用 主机名比对 + 文件年龄 双重判据：
      - 同主机名          → 按 PID 存活判断（真并发会被正确拦住）
      - 旧格式(无主机名)  → PID<=1 或 PID 已死即视为残留
      - 跨容器            → 用锁文件年龄兜底，超时即视为残留
    锁文件格式： <hostname>:<pid>:<epoch>  （旧格式纯 pid 仍兼容）"""
    host, pid = "", 0
    parts = (raw or "").strip().split(":")
    try:
        if len(parts) >= 3:
            host, pid = parts[0], int(parts[1])
        elif parts and parts[0].strip().isdigit():
            pid = int(parts[0])
    except Exception:
        pid = 0
    # PID<=1 或无法解析 → 必然是残留（容器内 python3 恒为 PID 1，宿主机 PID 1 永活）
    # 注意：该规则只对"本机/旧格式"锁生效；跨容器时 pid=1 是正常值，交给文件年龄兜底。
    if host and host == socket.gethostname():
        return not _pid_alive(pid)
    if not host:
        if pid <= 1:
            return True
        if not _pid_alive(pid):
            return True
    try:
        age = time.time() - os.path.getmtime(lockfile)
    except OSError:
        return True
    return age > LOCK_STALE_SEC

def acquire_lock():
    """单实例锁：防止并发开多个实例同时测速抢带宽、抢候选池。
    跨平台实现：锁文件 + O_EXCL 原子创建（Windows/Linux 均生效，替代原 fcntl 方案）。
    拿不到锁(确实有实例在跑)则本次跳过；若锁文件属于已退出的实例则自动清理后继续。"""
    lockfile = os.path.join(HERE, ".run.lock")
    try:
        fd = os.open(lockfile, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raw = ""
        try:
            with open(lockfile) as f:
                raw = f.read()
        except Exception:
            pass
        if not _lock_is_stale(lockfile, raw):
            print(f"另一个 cf_best 实例正在运行(lock={raw.strip()[:48]})，本次跳过")
            sys.exit(0)
        print(f"清理残留锁文件(lock={raw.strip()[:48] or '空'})")
        try:
            os.remove(lockfile)
            fd = os.open(lockfile, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except Exception:
            print("锁文件异常，本次跳过")
            sys.exit(0)
    try:
        os.write(fd, f"{socket.gethostname()}:{os.getpid()}:{int(time.time())}".encode())
        os.fsync(fd)
    except Exception:
        pass
    return fd   # 持有 fd 即加锁；main 的 finally 中关闭并删除锁文件

def main():
    lock = None
    lock = acquire_lock()
    try:
        args = sys.argv[1:]
        if not args:
            print(__doc__); return
        cmd = args[0]
        if cmd == "fetch":
            fetch_all()
        elif cmd == "slot":
            hour = int(args[1]) if len(args) > 1 else int(time.strftime("%H"))
            run_slot(hour, "--day" in args)
        elif cmd == "day":
            run_day()
        elif cmd == "best":
            run_best()
        else:
            print(__doc__)
    finally:
        if lock is not None:
            try:
                os.close(lock)
                lf = os.path.join(HERE, ".run.lock")
                if os.path.exists(lf): os.remove(lf)
            except Exception:
                pass

if __name__ == "__main__":
    main()
