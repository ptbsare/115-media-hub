# 参与贡献

感谢你愿意给这个项目添点东西。这份文档很短，读完能省掉几轮来回沟通。

## 先说明白两件事

- **本项目采用 MIT 许可证免费开源，没有付费版。** 提交代码不会有报酬，但会留在 commit 历史里。
- **功能类改动建议先开 issue 说明使用场景**，再动手写代码，避免做完发现方向不对。修 bug 可以直接提 PR，不用等回复。

## 本地跑起来

### Docker（推荐）

```bash
git clone https://github.com/xianer235/115-media-hub.git
cd 115-media-hub
```

**先改 `compose.yml` 里的挂载路径。** 仓库里那份是作者本机的绝对路径（`/Users/xianer/...`），直接启动会在你的机器上建出一串不存在的目录。把三行 `volumes` 的左边换成你自己的目录：

```yaml
volumes:
  - /你的路径/strm:/app/strm
  - /你的路径/logs:/app/logs
  - /你的路径/config:/app/config
  # 跨盘复制的暂存区，可选；不映射就落在 config 卷里
  - /你的路径/storage:/app/storage
```

然后：

```bash
docker compose up -d
```

打开 `http://127.0.0.1:18080`。登录凭据由环境变量 `MH_USERNAME` / `MH_PASSWORD` 提供（不再有默认的 `admin/admin123`），可以在 `compose.yml` 的 `environment` 里加，或者启动后在设置页改。

`config` 目录装的是数据库、Cookie 与全部设置，**升级镜像不会清掉它**，但删掉它等于重置。

### 直接跑

Python 3.12：

```bash
pip install -r requirements.txt
python main.py
```

## 跑测试

**必须走容器入口，不要在本机直接 `python -m unittest`。**

```bash
scripts/test_in_container.sh                                   # 全量，2~3 分钟
scripts/test_in_container.sh --changed                         # 只跑与本次改动相关的模块
scripts/test_in_container.sh tests.test_pan115_upload --quiet  # 单个模块
```

原因：本机安全策略会拦「删除文件」这个系统调用（项目目录同样拦），而测试的临时目录清理、SQLite journal、跨盘复制的临时分片都必须真删。本机跑会刷出一堆假失败，看到的不一定是代码坏了。

前端 JS 改动不在这套测试里，改完自己过一遍语法：

```bash
node --check static/js/modules/xxx.js
```

提交前至少跑一次 `--changed`，确认没有新增失败。

## 代码约定

- **不新增第三方依赖。** 现有依赖够用；确实需要时，请在 PR 里说明为什么标准库或现有依赖做不到。（历史上连 115 上传加密都是纯 Python 重写的。）
- **新增 `app/**` 下的模块必须登记到 `docs/superpowers/modules.md`**，否则 `tests.test_modules_doc` 会失败。
- **前端是原生 JS**（IIFE 与 ES module 混用），没有构建步骤，改完直接刷新页面。
- **不要提交** `config/`、`logs/`、Cookie、`webhook_secret` 或任何个人配置。`.gitignore` 已经覆盖，提交前还是看一眼 `git status`。
- 落点、命名、跨盘复制这类边界规则写在 `docs/superpowers/conventions.md`，改到相关代码前先读一遍，能避开大部分踩坑。

## 提交信息

用中文，说清「做了什么」和「为什么」，不要只写 `fix` 或 `update`。一次提交只做一件事，别把无关改动混在一起。

## 提 PR

- 标题写清改了什么，正文说明动机与验证方式
- 贴 `scripts/test_in_container.sh --changed` 的结果
- 涉及界面的改动附截图或录屏
- 关联 issue 用 `#123` 引用
- 涉及依赖增减、数据库表结构变更的，单独在正文里点出来

## 报告问题

用 [Issue 模板](https://github.com/xianer235/115-media-hub/issues/new/choose) 提交，填清版本号、部署方式和日志。

**日志里的 `115 Cookie`、`webhook_secret`、账号密码请打码**（保留前 4 位够定位就行）。完整 Cookie 等于账号控制权，公开贴出来会被扫走。

安全问题请不要开公开 issue，见 [SECURITY.md](SECURITY.md)。
