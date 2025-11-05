# FINRA Spider

一个用于爬取 FINRA（美国金融业监管局）数据的 Python 工具。该工具通过 OAuth2 认证访问 FINRA API，支持分页获取数据并自动保存为 CSV 文件。

## 功能特性

- 🔐 OAuth2 认证机制，自动获取和刷新 Access Token
- 📊 支持分页获取大量数据
- 💾 定期自动保存数据到 CSV 文件，防止数据丢失
- 🔄 自动重试机制，处理网络错误和速率限制
- ⚙️ 支持环境变量和命令行参数配置
- 📈 实时显示数据获取进度

## 环境要求

- Python >= 3.9
- 依赖包：
  - pandas >= 2.3.3
  - python-dotenv >= 1.2.1
  - requests >= 2.32.5

## 安装

1. 克隆或下载项目到本地

2. 安装依赖：
```bash
# 如果使用 uv
uv sync

# 或使用 pip
pip install -r requirements.txt
```

## 配置

### 环境变量配置

在项目根目录创建 `.env` 文件，配置以下环境变量：

```env
# 必需配置
USERNAME=你的API用户名
PASSWORD=你的API密码

# 可选配置（有默认值）
TOKEN_URL=https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token
BASE_URL=https://api.finra.org/data/group/OTCMarket/name/
limit=5000
retry_delay_seconds=60
MAX_RETRIES=10
TOKEN_REFRESH_INTERVAL=300
SAVE_INTERVEL=100
DATASET=monthlySummary
```

### 环境变量说明

| 变量名 | 说明 | 默认值 | 必需 |
|--------|------|--------|------|
| `USERNAME` | FINRA API 用户名 | - | ✅ |
| `PASSWORD` | FINRA API 密码 | - | ✅ |
| `TOKEN_URL` | Token 获取地址 | `https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token` | ❌ |
| `BASE_URL` | API 基础地址 | `https://api.finra.org/data/group/OTCMarket/name/` | ❌ |
| `limit` | 每次请求的数据量限制 | `5000` | ❌ |
| `retry_delay_seconds` | 速率限制后的重试等待时间（秒） | `60` | ❌ |
| `MAX_RETRIES` | 网络错误的重试次数 | `10` | ❌ |
| `TOKEN_REFRESH_INTERVAL` | Token 刷新间隔（秒） | `300` | ❌ |
| `SAVE_INTERVEL` | 数据保存间隔（每 N 次请求保存一次） | `100` | ❌ |
| `DATASET` | 默认数据集名称 | `monthlySummary` | ❌ |

## 使用方法

### 基本用法

```bash
python main.py --dataset monthlySummary
```

### 命令行参数

```bash
python main.py [选项]
```

#### 必需参数

- `--dataset`: 要获取的数据集名称（必需）

#### 可选参数

- `--save_intervel`: 保存间隔（默认：从环境变量读取或 100）
- `--limit`: 每次请求的数据量限制（默认：从环境变量读取或 5000）
- `--retry_delay_seconds`: 速率限制后的重试等待时间（默认：从环境变量读取或 60）
- `--max_retries`: 网络错误的重试次数（默认：从环境变量读取或 10）
- `--token_refresh_interval`: Token 刷新间隔（默认：从环境变量读取或 300）
- `--username`: API 用户名（默认：从环境变量读取）
- `--password`: API 密码（默认：从环境变量读取）

### 使用示例

```bash
# 使用默认配置获取 monthlySummary 数据集
python main.py --dataset monthlySummary

# 自定义保存间隔和每次请求的数据量
python main.py --dataset monthlySummary --save_intervel 50 --limit 1000

# 自定义重试参数
python main.py --dataset monthlySummary --retry_delay_seconds 120 --max_retries 5

# 使用命令行参数覆盖环境变量的用户名和密码
python main.py --dataset monthlySummary --username your_username --password your_password
```

## 工作原理

1. **认证流程**：
   - 使用用户名和密码通过 OAuth2 获取 Access Token
   - Token 会在指定时间间隔后自动刷新

2. **数据获取**：
   - 使用分页方式获取数据（通过 `limit` 和 `offset` 参数）
   - 每次请求获取指定数量的记录
   - 当返回的记录数少于 `limit` 时，表示已获取完所有数据

3. **错误处理**：
   - 网络错误时自动重试，最多重试 `MAX_RETRIES` 次
   - 速率限制时等待 `retry_delay_seconds` 秒后重试
   - 发生错误时会先保存已获取的数据

4. **数据保存**：
   - 每 `SAVE_INTERVEL` 次请求自动保存一次数据
   - 发生错误时也会保存已获取的数据
   - 数据以 CSV 格式保存，使用 `|` 作为分隔符，文件名为 `{dataset}.csv`

## 输出文件

- 数据保存为 CSV 格式，文件名为 `{数据集名称}.csv`
- 使用 `|` 作为分隔符
- 使用 UTF-8 编码

## 注意事项

1. 确保 `.env` 文件中的 `USERNAME` 和 `PASSWORD` 配置正确
2. 如果遇到速率限制，程序会自动等待并重试
3. 大数据集可能需要较长时间，建议在稳定网络环境下运行
4. 程序会在每个保存间隔和错误发生时自动保存数据，防止数据丢失

## 许可证

本项目遵循 MIT 许可证。

