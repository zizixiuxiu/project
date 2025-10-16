# 项目使用说明（快速上手）

本项目用于从文档中按“维度”分块并自动生成题目与规则。此 README 聚焦让其他开发者在配置好环境与密钥后能够**直接运行**项目并得到输出结果。

> 注：仓库已移除明文 API keys。请在本地使用环境变量或 `.claude/settings.local.json`（基于示例 `.claude/settings.local.example.json`）提供真实密钥。

---

## 一、先决条件

- 操作系统：Windows（PowerShell 示例命令）
- Python 3.8+
- 建议：本地模型至少 8GB 内存（处理大文档或并发时更高）调用api则无需求
- 推荐使用虚拟环境管理依赖

## 二、依赖安装

在仓库根目录运行（PowerShell）：

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

如果没有 `requirements.txt`，请安装下列依赖：

```powershell
pip install requests pymongo httpx pandas python-docx pdf2docx docxcompose
```

## 三、配置密钥与本地设置（两种方式）

A. 使用环境变量（推荐）

在 PowerShell 会话中设置（或在 CI 中使用 Secrets）：

```powershell
$env:QWEN_API_KEY = "your_qwen_api_key"
$env:DOUBAO_API_KEY = "your_doubao_api_key"
$env:DEEPSEEK_API_KEY = "your_deepseek_api_key"
# 可选：MongoDB 连接串
$env:MONGODB_URI = "mongodb://user:pass@host:27017/dbname"
```

B. 使用本地示例配置文件（仅限本地，不要提交）

1. 复制示例文件：

```powershell
copy .claude\settings.local.example.json .claude\settings.local.json
```

2. 编辑 `.claude\settings.local.json`，填写本地路径与必要权限字段。不要把真实文件提交到 Git。

> 项目 `code/config.py` 中的 `API_KEYS` 默认为空。代码会优先使用通过 `get_api_config()` 读取到的值；如果你更改了 `code/config.py`，请确保不要把真实密钥提交。

### 在哪儿添加 API Key（优先级与示例）

推荐的优先级（从高到低）：

1. 环境变量（推荐，适合临时测试、CI / Secrets 注入）
2. 本地未提交的覆盖配置文件（例如 `.claude/settings.local.json` 或 `code/config_local.py`，推荐长期本地开发使用）
3. 直接修改 `code/config.py` 中的 `API_KEYS`（最直接但**不推荐**，会有被误提交风险）

具体说明与示例：

- A. 环境变量（推荐）

   在 PowerShell 中设置（仅当前会话有效）：

   ```powershell
   $env:QWEN_API_KEY = "your_qwen_api_key"
   $env:DOUBAO_API_KEY = "your_doubao_api_key"
   $env:DEEPSEEK_API_KEY = "your_deepseek_api_key"
   # 可选：MongoDB 连接串
   $env:MONGODB_URI = "mongodb://user:pass@host:27017/dbname"
   ```

   说明：某些脚本（例如 `extract_paper_information.py`）会优先读取环境变量。如果你用 CI（GitHub Actions / GitLab CI），把密钥放到 Secrets 并注入环境变量是最佳实践。

- B. 本地示例配置文件（安全且长期）

   1) 复制示例并编辑（只在本地存在）：

   ```powershell
   copy .claude\settings.local.example.json .claude\settings.local.json
   # 编辑 .claude\settings.local.json，填写本地路径或权限等字段
   ```

   2) 可选：在 `code/` 下创建 `config_local.py`（不要提交）来覆盖 `API_KEYS`：

   ```python
   # code/config_local.py （示例，仅本地）
   API_KEYS = {
         "qwen": "your_qwen_api_key",
         "doubao": "your_doubao_api_key",
         "deepseek": "your_deepseek_api_key",
   }
   ```

   然后在 `code/config.py` 顶部（本地）增加一段可选加载逻辑：

   ```python
   try:
         from .config_local import API_KEYS as LOCAL_API_KEYS
         API_KEYS.update(LOCAL_API_KEYS)
   except Exception:
         pass
   ```

   并确保将 `code/config_local.py` 添加到 `.gitignore`，避免误提交。

- C. 直接修改 `code/config.py`（最简单但不安全）

   打开 `code/config.py`，找到 `API_KEYS` 字典，直接把对应服务的值替换为你的密钥：

   ```python
   API_KEYS = {
         "qwen": "your_qwen_key_here",
         "doubao": "your_doubao_key_here",
         "deepseek": "your_deepseek_key_here",
   }
   ```

   如果采用该方法，请务必在提交前将其恢复为空或从提交中移除（或使用本地覆盖方案替代）。

### 验证密钥是否生效（快速检查）

1. 设置好密钥（采用上面任意一种方式）。
2. 在 PowerShell 中运行示例脚本并观察日志输出（会有 AI 请求或警告信息）：

```powershell
python code\extract_paper_information.py
```

3. 如果脚本中出现权限或认证错误，请检查环境变量名、拼写与服务端控制台中的 key 状态。


## 四、目录与主要脚本说明

- `code/`：核心脚本。
  - `generate_paper_information_dimension_based.py`：主要的处理流程入口（维度化、生成、规则提取、合并结果）。
  - `extract_paper_information.py`：文档转换、标题提取等工具函数，也包含可独立运行的 pipeline 示例。
  - `aiTaskRunner.py`：AI 调用封装器（支持同步/异步与 speculative 模式）。
  - `config.py`：配置（注意：请使用本地配置或环境变量提供密钥）。
- `prompt/`：prompt 文本文件（索引、题目结构、规则）。
- `files/`：中间产物与输出（请在 `.gitignore` 中忽略 `files/result/` 等）。

## 五、快速运行示例

1. 激活虚拟环境并确保依赖已安装（见上文）。
2. 准备输入文档（.docx/.pdf/.xlsx/.csv 等），将其放到能被脚本读取的 URL 或本地路径；示例脚本 `extract_paper_information.py` 演示从 URL 下载并处理文件。
3. 执行主流程（以 `generate_paper_information_dimension_based.py` 为例）：

```powershell
python code\generate_paper_information_dimension_based.py
```

或者运行 `extract_paper_information.py` 的独立示例（需设置 API key 环境变量）：

```powershell
python code\extract_paper_information.py
```

> 注意：当缺少 API key 时，脚本会打印警告并可能跳过与外部 API 的调用。你可以在本地先跑转换/分块等不依赖 AI 的步骤以测试流水线。

## 六、输出位置与格式

主要输出（根据脚本参数）通常写入 `files/` 子目录：

- 主结果：`{title}_dimension_based_final.json`（包含题目、规则、维度统计）。
- 处理日志：`{title}_processing_log.json`（详细处理过程、错误信息）。
- 分块 JSON：`files/04-分块后的json/*chunks*.json`。

结果均为 UTF-8 编码的 JSON 或 Word 文档（.docx），便于后续处理与人工复核。

## 七、常见问题与故障排查

1. 如果出现 `ModuleNotFoundError`：
   - 确认在虚拟环境内并已安装 `requirements.txt` 中的依赖。
2. 如果 AI 请求报错或超时：
   - 检查环境变量中 API key 是否正确。
   - 检查 `code/config.py` 中的 `API_SERVICES` 与 `SERVICE_MAPPING` 配置是否指向正确的 base_url 与 model。
   - 如网络不通或超时，可适当增大请求超时时间，或降低并发数（在 `config.py` 中调整 `TASK_CONFIG["max_workers"]`）。
3. 如果 MongoDB 写入失败：
   - 检查 MongoDB 是否正在运行、网络是否可达，或使用 `MONGODB_URI` 环境变量覆盖本地配置。

## 八、上传到 GitHub 的前置检查（强烈建议）

在推送之前：

1. 确保 `.gitignore` 中包含 `.claude/`, `files/result/`, `__pycache__/` 等本地/敏感路径。
2. 确认 `code/config.py` 中没有明文密钥（仓库已尝试替换，但请再次确认）。
3. 若曾误提交密钥，请先到各服务页面废弃旧密钥并生成新密钥，然后参照仓库清理与通知流程（README 中另有说明）。

## 九、安全与团队流程建议

- 在 CI（例如 GitHub Actions）中使用 Secrets 存放真实密钥。
- 对敏感操作（如强制推送重写历史）提前通知团队，并提供重新克隆说明。
- 使用密码管理器或 Vault 管理生产密钥，不要在代码或 commit message 中暴露密钥。

---

如需，我可以：
- 生成一份 `requirements.txt`（我会基于代码扫描生成并放到仓库根）。
- 帮你把 README 中的示例命令改成更适合你的生产环境（如 Docker、CI workflow）。
- 自动生成团队通知模板和步骤脚本来执行历史清理（基于 git-filter-repo 或 BFG）。

如果现在可以，我将把 `requirements.txt` 文件也写入仓库（基于当前代码的 imports）。
