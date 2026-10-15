# 架构与原理详解

## 1. 数据来源（SOURCES）
内置 14 个公开抓取源：
- 11 个 `bestcf.pages.dev` 系列（按地区/随机分区），社区维护的高质量候选 IP 池
- 3 个 GitHub raw 源（svip-s/cloudflare_ip、LancelotRar/best-cf-ips、kele9988/cf-best-ip 的 web.txt 回流）

### GitHub raw 镜像回退
`raw.githubusercontent.com` 在部分出口直连成功率仅约 1/5（Errno 104 RST）。
脚本对 GitHub raw 链接做「单次快速尝试 + 镜像链」回退：
`直连 → gh-proxy.com → fastly.jsdelivr.net → ghproxy.net → cdn.jsdelivr.net`，
每个镜像只尝试一次，避免失败源拖长整轮。

## 2. 去重与候选池
- `fetch_all`：拉取所有源、正则提取 `IP:PORT`、按前后文识别地区码，写 `candidates.txt` + `region.txt`。
- `refresh_pool`：每期把「历史候选 ∪ 本轮新源」合并，初始化统计；超过 `POOL_MAX`（默认 5000）时
  优先保留历史命中 IP，给新 IP 留约 1/5 试用名额，防止优质新 IP 被池满截断。
- `prune_pool`：每期测速后达标清零、未达标累加；**连续 `PRUNE_N`（默认 7）期未达标**移出候选池。

## 3. 两层测速
- **延迟**：`socket.create_connection` TCP 握手，16 并发、500ms 超时、每 IP 1 次采样。
- **下载**：调用 `curl` 对 `https://{hex(ip)}.{BEST_HOST}/__down?bytes=5MB` 发起 TLS 下载，
  用 `--connect-to` 把 SNI 钉到目标 IP（绕过 DNS），`-w SPEED` 取下载速率；
  同时从 `cf-ray` 响应头解析 colo → 国家码，零额外请求补全地区。
  12 并发、0.2s 错峰启动避免瞬时抢带宽。

## 4. 格式化
`IP:PORT#地区 中文上标序号[速度Mbps]`，地区名由 `REGION_CN` 映射，序号按地区内从 ¹ 重排。

## 5. 跨期稳定筛选（核心）
`cross_aggregate` 不重新测速，而是跨多个文件（时段/天/best）聚合：
- 每个 `IP:PORT` 统计「达标期数」「出现期数」（速度 ≥ `MIN_MBPS` 算达标）
- 保留：达标期数 ≥ `min_occurs`（默认 4，自适应不超过实际期数）且 出现期数 ≥ `min_occurs`
- 排序：最差期速度降序 → 平均降序（最稳优先）
- 可选 `per_region`：按地区各取前 N

此机制保证选出的 IP 在多个时段都高速，而非某次单点冒尖。

## 6. 调度周期
- `slot H`：某时段测速 → `timeH.txt`（不更新 DNS）
- `run_day`：合并当天 time → `dayN.txt`、`24hour.txt`（每地区 top3，不更新 DNS）
- `run_best`（每 7 天）：合并 7 个 day → `best.txt` → 更新普通域名 DNS
- `do_topbest`：每 4 个 best 聚合 → `topbest.txt` → 更新 `*best` 域名 DNS
- `dayIndex` 按自然日推进，跨 7 天触发 `best`，进入新周期

## 7. GitHub 同步
`gh_upload` 用 `owner/repo/token` 把结果 PUT 到仓库（遇 409/422 自动取 sha 覆盖）。

## 8. Cloudflare DNS 自动更新（可选）
`cf_dns_update`：读 `cf_dns_enable` 开关，查 zone id，按 `DNS_REGIONS` 每个地区把最快
`cf_per_region` 个 IP 并发删除旧记录 + 创建 A 记录（`proxied:false, ttl:60`）。
子域名形如 `hk.your-domain` / `hkbest.your-domain`。

## 9. 单实例与锁
`acquire_lock` 用 `O_EXCL` 原子创建 `.run.lock`；`_lock_is_stale` 用「主机名比对 + 文件年龄（90 分钟）」
双重判据处理容器 PID=1 场景，残留锁自动清理后继续，避免整天空转。

## 10. 运行依赖
- Python 3.8+（标准库）
- `curl`（下载测速）
- 可选：Docker（见 Dockerfile）
