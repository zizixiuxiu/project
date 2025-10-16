import os
import json
import re
import time
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient

# ======================== 配置 ========================
# 目录
OUTPUT_BASE_DIR = Path("files")
CHUNKS_DIR = OUTPUT_BASE_DIR / "05-提取的文件"
MERGED_DIR = OUTPUT_BASE_DIR / "merged_content"
RESULT_DIR = OUTPUT_BASE_DIR / "result"
for dir_path in [CHUNKS_DIR, MERGED_DIR, RESULT_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# MongoDB 配置
MONGODB_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "username": "",
    "password": "",
    "db_name": "doc_db",
    "collection_name": "doc_batches",
    "auth_source": "admin",
    "auth_mechanism": "SCRAM-SHA-256",
    "server_selection_timeout_ms": 5000
}

# API Keys
API_KEYS = {
    "qwen": "sk-skquafrfvnqufpraqiecpqeqjfgokxvglprxvhkeexoosucc",
    "doubao": "sk-Hz4rMMxMt2qSts4RKGDAW2byVNTXpf56v5SlejZHgDzrQV1N",
    "deepseek": "sk-fdb99a0a3b85405fa67a55f08379385a",
    "qwen-plus": "sk-941d9edd6276473984da8d4f2b57a86a",
    "doubao-self": "5dd533df-1d14-4aa2-9154-a641b8872eab",
    "kimi": "sk-difiIo3iJdUqkcRqeSGU74M7iwf1ESIaonOEJZtMhb9Wdk28",
    "doubao-lite": "sk-Hz4rMMxMt2qSts4RKGDAW2byVNTXpf56v5SlejZHgDzrQV1N"
}

# API 服务配置
API_SERVICES = {
    "qwen": {
        "base_url": "https://api.siliconflow.cn/v1",
        "model": "Qwen/Qwen3-235B-A22B-Thinking-2507",
        "max_tokens": 8000,
        "temperature": 0
    },
    "qwen-plus":{
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "model": "qwen-plus",
        "max_tokens": 8000,
        "temperature": 0
    },
    "doubao": {
        "base_url": "http://219.153.12.49:3000/v1",
        "model": "doubao-1-5-pro-32k",
        "max_tokens": 8000,
        "temperature": 0
    },
    "deepseek": {
        "base_url": "https://api.deepseek.com/v1",
        "model": "deepseek-chat",
        "max_tokens": 8000,
        "temperature": 0
    },
    "doubao-self": {
        "base_url": "https://ark.cn-beijing.volces.com/api/v3/",
        "model": "doubao-seed-1-6-flash-250828",
        "max_tokens": 8000,
        "temperature": 0
    },
    "kimi": {
        "base_url": "https://api.moonshot.cn/v1",
        "model": "kimi-k2-turbo-preview",
        "max_tokens": 8000,
        "temperature": 0
    },
    "doubao-lite": {
        "base_url": "http://219.153.12.49:3000/v1",
        "model": "doubao-1-5-lite-32k",
        "max_tokens": 4000,
        "temperature": 0
    },
}

# 默认任务参数
TASK_CONFIG = {
    "max_tokens": 6000,  # 适当降低，减少等待时间
    "retry_count": 2,    # 恢复重试次数，应对网络问题
    "max_workers": 100,  # 大幅增加并行度，支持所有分块同时处理
    "chunk_size": 3200,
    "overlap_size": 1000
}

# Speculative Decoding 配置
SPECULATIVE_CONFIG = {
    "enabled": False,  # 禁用Speculative Decoding
    "speculative_times": 4,  # 进一步增加推测次数
    "use_native_speculative": True,
    "draft_service": "doubao-lite",  # 使用豆包轻量级服务
    "draft_model": "doubao-1-5-lite-32k",
    "draft_tokens": 150,  # 增加草稿token数
    "supports_native": False,
    "quality_threshold": 0.7,
    "enable_for_services": ["generate_questions", "extract_rules", "find_content_index"]  # 扩展支持范围
}

# 服务映射
SERVICE_MAPPING = {
    "find_content_index": "doubao",
    "generate_questions": "qwen",
    "extract_rules": "doubao"
}

PROMPT_FILES = {
    "index": "prompt/找题索引.txt",
    "question": "prompt/提取题目结构化数据.txt",
    "rule": "prompt/提取规则.txt"
}

# 默认执行配置
DEFAULT_EXECUTION_CONFIG = {
    "title": "全市性行业协会（商会）评估指标",
    "target_batch_id": 4,
    "concurrent_tasks": 20  # 增加并发任务数
}
# ======================== 工具函数 ========================
def get_api_config(service_name: str) -> dict:
    """获取服务对应的 API 配置"""
    api_service = SERVICE_MAPPING.get(service_name, "doubao")
    config = API_SERVICES[api_service].copy()
    config["api_key"] = API_KEYS[api_service]
    return config

def log_saved_file(file_path: Path) -> None:
    try:
        log_file = OUTPUT_BASE_DIR / "saved_files.log"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {file_path}\n")
        print(f"[记录] 已写入日志: {file_path}")
    except Exception as e:
        print(f"[警告] 写入日志失败: {e}")

def get_mongodb_uri() -> str:
    config = MONGODB_CONFIG
    if config["username"] and config["password"]:
        auth = f"{config['username']}:{config['password']}@"
    else:
        auth = ""
    return f"mongodb://{auth}{config['host']}:{config['port']}/{config['db_name']}?authSource={config['auth_source']}&authMechanism={config['auth_mechanism']}"

def get_speculative_config(service_name: str = None) -> dict:
    config = SPECULATIVE_CONFIG.copy()
    if service_name:
        config["enabled"] = (
            SPECULATIVE_CONFIG["enabled"] and
            service_name in SPECULATIVE_CONFIG["enable_for_services"]
        )
    return config

def get_optimal_chunk_params(content_length: int) -> Tuple[int, int]:
    """根据内容长度获取最优分块参数"""
    if content_length < 5000:
        return 3200, 800
    elif content_length < 20000:
        return 4000, 1000
    else:
        return 4800, 1200

def _ensure_directories_exist():
    """确保所有必要的目录存在"""
    for dir_path in [CHUNKS_DIR, MERGED_DIR, RESULT_DIR]:
        dir_path.mkdir(parents=True, exist_ok=True)

# HTTP 调用 Qwen
def call_qwen_http(prompt: str, api_key: str, model: str = None, max_tokens: int = None, temperature: float = None) -> dict:
    model = model or API_SERVICES["qwen"]["model"]
    max_tokens = max_tokens or API_SERVICES["qwen"]["max_tokens"]
    temperature = temperature if temperature is not None else API_SERVICES["qwen"]["temperature"]

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature
    }

    try:
        resp = requests.post(f"{API_SERVICES['qwen']['base_url']}/chat/completions", headers=headers, json=payload, timeout=300)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e), "raw_response": resp.text if 'resp' in locals() else ""}

# ======================== 核心任务处理 ========================
def process_task(prompt: str, service_name: str) -> dict:
    api_conf = get_api_config(service_name)
    spec_conf = get_speculative_config(service_name)

    # Speculative Decoding
    if spec_conf["enabled"]:
        draft_res = call_qwen_http(prompt, api_conf["api_key"], model=spec_conf["draft_model"], max_tokens=spec_conf["draft_tokens"])
        main_res = call_qwen_http(prompt, api_conf["api_key"], model=api_conf.get("model"), max_tokens=api_conf.get("max_tokens"))
        # 可根据质量阈值判断，这里直接返回 main
        return main_res
    else:
        if SERVICE_MAPPING[service_name] == "qwen":
            result = call_qwen_http(prompt, api_conf["api_key"], model=api_conf.get("model"), max_tokens=api_conf.get("max_tokens"))
            # 从chat/completions响应中提取content
            if "choices" in result and len(result["choices"]) > 0:
                content = result["choices"][0].get("message", {}).get("content", "")
                return {"content": content, "raw_response": result}
            elif "error" in result:
                return result
            else:
                return {"error": "Unexpected response format", "raw_response": result}
        else:
            # 其他服务可以实现 runner 或 HTTP 调用
            return {"error": "Service not implemented"}

def process_pipeline_full(prompts: List[str], service_name: str, concurrent_tasks: int = 10) -> List[dict]:
    results = []
    with ThreadPoolExecutor(max_workers=concurrent_tasks) as executor:
        future_to_prompt = {executor.submit(process_task, prompt, service_name): prompt for prompt in prompts}
        for future in as_completed(future_to_prompt):
            prompt = future_to_prompt[future]
            try:
                res = future.result()
                results.append({"prompt": prompt, "response": res})
            except Exception as e:
                results.append({"prompt": prompt, "response": {"error": str(e)}})
    return results

# ======================== MongoDB 写入 ========================
def save_to_mongodb(results: List[dict]) -> None:
    uri = get_mongodb_uri()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[MONGODB_CONFIG["db_name"]]
    collection = db[MONGODB_CONFIG["collection_name"]]
    for item in results:
        collection.insert_one({
            "prompt": item["prompt"],
            "response": item["response"],
            "timestamp": datetime.now()
        })
    print(f"[MongoDB] 写入 {len(results)} 条记录")
