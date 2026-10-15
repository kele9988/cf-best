# CF 优选测速容器：只提供 python 运行时 + curl（下载测速用 C 高效 SSL，跑满带宽）
# cf_best.py / cf_best.conf / 数据目录 全部通过卷挂载到 /app，不写进镜像
FROM python:3.11-alpine
WORKDIR /app
RUN apk add --no-cache ca-certificates curl
CMD ["python3", "cf_best.py"]
