# 115 直链是按 UA 签名的：跨盘复制 403 的真实原因

日期：2026-10-20

## 现场

真机把 `196 4K.mp4`（2.79GB）从 115 复制到夸克，任务在 `phase=upload` 前就失败：

```
源文件下载失败：403 Client Error: Forbidden for url:
https://cdnfhnfile.115cdn.net/6ad37d.../196%204K.mp4?t=...&d=vip-1964423917-e5gq1usw5hp9vzzdn-1-0&...
```

取链本身是成功的（大文件走电脑端 downurl，拿到了直链），失败发生在**下载**这一步。
把同一个链接换几套请求头做定点测试，结果如下（同一账号、同一秒内取到的链接）。
两个通道的规矩**不一样**，这是最容易踩错的地方：

**大文件（>1GB，走 downurl 通道，UA = `Mozilla/5.0 115-media-hub`）**

| 请求头 | 结果 |
|---|---|
| `User-Agent: Chrome/131`（复制引擎的默认下载 UA）+ 账号 Cookie | **403** |
| `User-Agent: Mozilla/5.0 115-media-hub`（**取链时用的 UA**）+ 账号 Cookie | 206 |
| 同上，完全不带 Cookie | 206 |

**小文件（走 webapi `/files/download` 通道，UA = 小程序 UA）**

| 请求头 | 结果 |
|---|---|
| Chrome UA + 账号 Cookie | **403** |
| **取链 UA** + 账号 Cookie | **403** ← 光对 UA 还不够 |
| 取链 UA + 响应下发的路径级 Cookie | 206 |

## 两个约束

1. **直链按 UA 签名**。115 签链接时把请求的 UA 掺进了签名，下载侧换一个 UA 就是 403。
   跨盘复制引擎原来统一用 Chrome UA 下载，正好踩中。
2. **webapi 通道还额外要路径级 Cookie**（响应下发的 `acw_tc=...; <hash>=...; path=/<目录 hash>/`），
   只带账号 Cookie 即使 UA 正确也照样 403。downurl 通道没这条限制。

另外确认：HTTP Range 分段是能用的（`bytes=0-8388607`、中间段、末尾段都返回 206 且 Content-Range 正确），
所以分段下载逻辑本身没问题，唯一坏掉的是请求头。

## 修法

取链与下载**共用一份请求头**，由取链方产出、下载方原样使用：

- `pan115_downurl.build_115_download_headers(cookie, user_agent, extra_cookie)`：统一产出
  `User-Agent / Referer / Origin / Cookie`，Cookie 支持把取链响应的 Set-Cookie 拼在账号 Cookie 后面。
- `pan115.resolve_115_download_info` 两条通道都在返回值里带上 `headers`（和 `cookie`）：
  - webapi 通道：UA = `_CLOUD115_WEBAPI_USER_AGENT`（小程序 UA，取链用的就是它）+ 路径级 Cookie；
  - downurl 通道：UA = `Mozilla/5.0 115-media-hub`（downurl 请求用的就是它）+ 可选的 Set-Cookie。
- `cross_cloud_copy._download_headers` 本来就把 provider 返回的 `headers` 合并覆盖，
  所以引擎侧无需改逻辑，只要 provider 把话说明白。
- webapi 通道万一没下发路径级 Cookie，就改走电脑端 downurl（它只认 UA）；
  回退再失败则保留 webapi 那条直链，不把整个取链判死。

新增测试固定在 `tests/test_pan115_downurl.py`（下载头与取链 UA 一致、Cookie 合并）
和 `tests/test_cross_cloud_copy.py`（provider 给的 UA 不会被默认 UA 顶掉）。

真机复验（2026-10-20，同一账号）：

- 大文件（downurl 通道，2.79GB）：分段下前 48MB，3 段全部 206，约 7.1MB/s，SHA1 正常累计；
- 小文件（webapi 通道，31MB）：206，Content-Range 正确。

## 给后续复用的提醒

任何新加「用网盘直链下载」的链路（新网盘作源、离线任务的取链校验等），都不能自己另编 UA：
**取链用什么头，下载就用什么头**。115 这条尤其如此；夸克是 Referer 敏感，阿里目前没这个限制。
