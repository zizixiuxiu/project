"""
基于维度分块的配置文件
更新配置以使用新的增强版提示词和优化设置
"""

from pathlib import Path

# 基础目录配置
BASE_DIR = Path(__file__).parent
OUTPUT_BASE_DIR = BASE_DIR / "output"
CHUNKS_DIR = OUTPUT_BASE_DIR / "chunks"
MERGED_DIR = OUTPUT_BASE_DIR / "merged"
RESULT_DIR = OUTPUT_BASE_DIR / "result"

# MongoDB 配置
MONGODB_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "username": None,
    "password": None,
    "db_name": "doc_db",
    "collection_name": "doc_batches",
    "auth_source": "admin",
    "auth_mechanism": "SCRAM-SHA-256",
    "server_selection_timeout_ms": 5000
}

# 提示词文件配置 - 使用增强版
PROMPT_FILES = {
    "index": "prompt/找题索引_维度增强版.txt",  # 使用新的维度增强版
    "question": "prompt/提取题目结构化数据.txt",
    "rule": "prompt/提取规则.txt"
}

# 任务配置 - 针对维度分块优化
TASK_CONFIG = {
    "max_tokens": 8000,
    "retry_count": 3,
    "max_workers": 4,  # 维度并行处理的最大并发数
    "timeout": 120
}

# 默认执行配置
DEFAULT_EXECUTION_CONFIG = {
    "title": "社会组织评估指标",
    "target_batch_id": None,
    "concurrent_tasks": 2  # 规则提取和题目生成的并发数
}

# API 服务映射 - 为维度分块优化
SERVICE_MAPPING = {
    "find_content_index": "doubao",  # 索引查找使用doubao
    "generate_questions": "deepseek",  # 题目生成使用deepseek
    "extract_rules": "doubao"  # 规则提取使用doubao
}

# API 配置
API_CONFIGS = {
    "doubao": {
        "api_key": "your_doubao_api_key",
        "base_url": "https://ark.cn-beijing.volces.com/api/v3",
        "model": "ep-20241127112957-cjvh2"
    },
    "deepseek": {
        "api_key": "your_deepseek_api_key",
        "base_url": "https://api.deepseek.com/v1",
        "model": "deepseek-chat"
    },
    "zhipu": {
        "api_key": "your_zhipu_api_key",
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "model": "glm-4-flash"
    }
}

# Speculative Decoding 配置
SPECULATIVE_CONFIG = {
    "enabled": True,
    "draft_model": "glm-4-flash",
    "speculative_times": 3,
    "enable_for_services": ["doubao", "deepseek"]
}

def get_api_config(service_name: str) -> dict:
    """
    获取指定服务的API配置
    """
    service = SERVICE_MAPPING.get(service_name, "doubao")
    return API_CONFIGS.get(service, API_CONFIGS["doubao"])

def get_speculative_config(service_name: str = None) -> dict:
    """
    获取指定服务的Speculative Decoding配置
    """
    base_config = SPECULATIVE_CONFIG.copy()

    if service_name:
        service = SERVICE_MAPPING.get(service_name, "doubao")
        if service not in base_config["enable_for_services"]:
            base_config["enabled"] = False

    return base_config

def get_optimal_chunk_params(content_length: int) -> tuple:
    """
    根据内容长度获取最优的分块参数
    注意：维度分块模式下，这个函数主要用于向后兼容
    """
    if content_length < 2000:
        return 1500, 300
    elif content_length < 5000:
        return 2500, 500
    elif content_length < 10000:
        return 3500, 700
    else:
        return 4000, 800

def _ensure_directories_exist():
    """确保所有必需的目录存在"""
    directories = [OUTPUT_BASE_DIR, CHUNKS_DIR, MERGED_DIR, RESULT_DIR]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

# 维度分块特有配置
DIMENSION_CONFIG = {
    "max_dimensions_per_batch": 10,  # 每批处理的最大维度数
    "dimension_content_max_length": 15000,  # 单个维度内容的最大长度
    "auto_split_large_dimensions": True,  # 是否自动拆分过大的维度
    "validate_dimension_completeness": True,  # 是否验证维度完整性
    "sort_questions_by_number": True  # 是否按题号排序题目
}

# 输出格式配置
OUTPUT_FORMAT_CONFIG = {
    "structure": "dimension_grouped",  # 按维度分组的结构
    "include_processing_stats": True,  # 包含处理统计信息
    "include_dimension_details": True,  # 包含维度详细信息
    "save_processing_log": True,  # 保存处理日志
    "validate_output_format": True  # 验证输出格式
}

def validate_dimension_config():
    """验证维度分块配置的有效性"""
    errors = []

    # 检查提示词文件是否存在
    for key, file_path in PROMPT_FILES.items():
        full_path = Path(file_path)
        if not full_path.exists():
            errors.append(f"提示词文件不存在: {file_path}")

    # 检查API配置
    for service, config in API_CONFIGS.items():
        if not config.get("api_key") or config["api_key"] == f"your_{service}_api_key":
            errors.append(f"需要配置 {service} 的API密钥")

    # 检查并发配置
    max_workers = TASK_CONFIG["max_workers"]
    if max_workers < 1 or max_workers > 10:
        errors.append(f"max_workers 应该在 1-10 之间，当前值: {max_workers}")

    return errors

def print_dimension_config_summary():
    """打印维度分块配置摘要"""
    print("=" * 60)
    print("基于维度分块的配置摘要")
    print("=" * 60)
    print(f"索引提取提示词: {PROMPT_FILES['index']}")
    print(f"题目生成提示词: {PROMPT_FILES['question']}")
    print(f"规则提取提示词: {PROMPT_FILES['rule']}")
    print(f"最大并发维度数: {TASK_CONFIG['max_workers']}")
    print(f"最大维度内容长度: {DIMENSION_CONFIG['dimension_content_max_length']}")
    print(f"启用Speculative Decoding: {SPECULATIVE_CONFIG['enabled']}")

    print("\nAPI服务映射:")
    for task, service in SERVICE_MAPPING.items():
        print(f"  {task}: {service}")

    print("\n维度处理配置:")
    for key, value in DIMENSION_CONFIG.items():
        print(f"  {key}: {value}")

    # 验证配置
    errors = validate_dimension_config()
    if errors:
        print("\n配置问题:")
        for error in errors:
            print(f"  ❌ {error}")
    else:
        print("\n✅ 配置验证通过")

    print("=" * 60)

if __name__ == "__main__":
    # 确保目录存在
    _ensure_directories_exist()

    # 打印配置摘要
    print_dimension_config_summary()