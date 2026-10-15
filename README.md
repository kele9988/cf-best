# cf-best · Cloudflare 优选 IP 测速工具

自动从多个公开源拉取 Cloudflare 边缘节点 IP，进行**并发延迟 + 下载**测速，按地区筛选并保留「跨多个时段都稳定高速」的 IP，结果同步到 GitHub，并可一键更新 Cloudflare DNS 让域名指向最优节点。

> 纯 Python 标准库实现，**零第三方依赖**，一条命令即可运行，亦可用 Docker。

## 特性

- **多源抓取 + 镜像回退**：内置 14 个公开源（含 bestcf.pages.dev 系列、GitHub raw），针对 `raw.githubusercontent.com` 直连高失败率做了镜像回退链（gh-proxy / jsDelivr 等）。
- **两层测速**：TCP 握手延迟（16 并发、500ms 超时）+ TLS 5MB 下载速度（12 并发错峰，用 curl 跑满带宽），自动按地区标注。
- **跨期稳定筛选**：不是单次冒尖，而是跨多个时段/多天聚合，保留「最少稳定达标期数」的 IP，输出最稳的一批。
- **增量候选池 + 淘汰**：每期合并新源、连续 N 期未达标自动移出，池子只涨好 IP。
- **GitHub 同步**：结果（time/day/best/topbest/24hour）自动提交到你的公开仓库，供首页或下游脚本拉取。
- **Cloudflare DNS 自动更新**：测速后按地区把最快的 N 个 IP 写入 `*.your-domain` 的 A 记录（可选）。
- **单实例锁**：防并发重复跑、自动清理僵尸锁（兼容容器 PID=1 场景）。

## 原理概览

```
多源抓取 ─┐
          ├─▶ 去重候选池 ─▶ 延迟测速 ─▶ 下载测速 ─▶ 格式化
回流best ─┘                                          │
                                                    ▼
                              跨期/跨天聚合(稳定筛选) ─▶ GitHub 上传 ─▶ (可选) Cloudflare DNS 更新
```

- **slot**：单个时段测速，产出 `timeH.txt`
- **day**：合并当天各 slot → `dayN.txt` + 每地区最快 3 个 `24hour.txt`
- **best**：合并 7 个 day → `best.txt` 并更新普通域名 DNS
- **topbest**：每 4 个 best 聚合 → `topbest.txt` 并更新 `*best` 域名 DNS

## 快速开始

### 方式一：Docker（推荐）

```bash
git clone https://github.com/kele9988/cf-best.git
cd cf-best
cp cf_best.conf.example cf_best.conf   # 填入你的 token
docker build -t cf-best .
# 每天 6 个时段测速，参考 crontab.example
docker run --rm -v "$PWD":/app cf-best python3 -u /app/cf_best.py slot "$(date +%H)"
```

### 方式二：直接运行

```bash
# 需要 Python 3.8+，以及 curl（用于下载测速）
pip 无需安装任何依赖
cp cf_best.conf.example cf_best.conf
python3 cf_best.py fetch        # 拉取并去重候选
python3 cf_best.py slot 0       # 执行某时段测速
```

## 配置（cf_best.conf）

详见 `cf_best.conf.example`。全部为 `KEY=VALUE` 格式：

| 键 | 说明 |
|---|---|
| `owner` / `repo` | GitHub 仓库，结果上传目标 |
| `token` | GitHub PAT，需 `contents:write` 权限 |
| `cf_dns_enable` | 是否启用 Cloudflare DNS 更新（`1`/`0`） |
| `cf_token` | Cloudflare API Token（Zone:DNS:Edit） |
| `cf_zone` | 你的域名，例如 `example.com` |
| `cf_per_region` | 每个地区最多写入的 A 记录数 |
| `remote_dir` | （可选）多节点部署时用不同目录区分，避免互相覆盖 |
| `min_occurs` | （可选）跨期聚合最少稳定期数，默认 4 |

> ⚠️ `cf_best.conf` 含密钥，**已记入 `.gitignore`，切勿提交。**

## 调度（crontab 示例）

参考 `crontab.example`：每天 0/4/8/12/16 点各跑一个 slot，20 点带 `--day` 归档当天，7 天滚动触发 `best` 汇总。

## 产物说明

| 文件 | 含义 |
|---|---|
| `timeH.txt` | 某时段测速结果 |
| `dayN.txt` | 第 N 天跨时段聚合 |
| `best.txt` | 7 天聚合，周期末主推 |
| `topbest.txt` | 4 个 best 聚合，最稳一批 |
| `24hour.txt` | 每地区当日最快 3 个，每日覆盖 |

输出格式：`IP:端口#地区 中文角标[速度Mbps]`，例如：
`104.18.93.220:443#AU 澳大利亚¹[14.84Mbps]`

## 自定义

脚本顶部集中了所有可调参数（无需改逻辑）：

- `SOURCES`：抓取源列表
- `GH_RAW_MIRRORS`：GitHub raw 镜像回退链
- `BEST_HOSTS`：TLS 下载测速用的 SNI 端点（默认替代 speed.cloudflare.com 以避开限流）
- `DNS_REGIONS`：Cloudflare DNS 自动更新覆盖的地区码
- `LATENCY_THREADS` / `DL_THREADS` / `DL_BYTES` / `MIN_MBPS` 等测速参数

## FAQ

**Q: 没有 Cloudflare 域名，能用吗？**
A: 能。设 `cf_dns_enable=0`，脚本只做测速并把结果上传 GitHub，DNS 更新跳过。

**Q: 为什么下载测速依赖 curl？**
A: curl 的 C 层 SSL 实现比纯 Python 快得多，能真实跑满带宽，避免把测速瓶颈算到网络上。

**Q: 测速结果和 GUI 优选工具对不上？**
A: 本项目以「跨期稳定」为筛选目标，不是单次峰值；更适合需要长时间稳定观看视频的场景。

## License

[MIT](LICENSE) © kele9988
