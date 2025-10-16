import asyncio
import httpx
import requests
import re
import json
import time
from typing import Optional, Any, Dict, List

class AITaskRunner:
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None,
                 model: Optional[str] = None, max_tokens: Optional[int] = None,
                 enable_speculative: bool = False, speculative_times: int = 2):
        """初始化 AITaskRunner"""
        from config import get_api_config

        default_config = get_api_config("generate_questions")
        self.api_key = api_key or default_config["api_key"]
        self.base_url = base_url or default_config["base_url"]
        self.model = model or default_config["model"]
        self.max_tokens = max_tokens or default_config.get("max_tokens", 8000)

        # Speculative 配置
        self.enable_speculative = enable_speculative
        self.speculative_times = speculative_times
        self.use_native_speculative = True

        # 从config获取Speculative配置
        from config import SPECULATIVE_CONFIG, API_SERVICES, API_KEYS
        self.spec_config = SPECULATIVE_CONFIG.copy()

        # 配置草稿模型服务（仅在启用Speculative时）
        if self.enable_speculative and "draft_service" in self.spec_config:
            draft_service = self.spec_config["draft_service"]
            if draft_service in API_SERVICES and draft_service in API_KEYS:
                self.draft_api_key = API_KEYS[draft_service]
                self.draft_base_url = API_SERVICES[draft_service]["base_url"]
                self.draft_model = API_SERVICES[draft_service]["model"]
                print(f"[Speculative] 草稿模型配置: {draft_service} | {self.draft_model}")
            else:
                print(f"[警告] 草稿服务 {draft_service} 配置不完整，禁用Speculative Decoding")
                self.enable_speculative = False
        elif not self.enable_speculative:
            print("[Speculative] 已禁用 - 跳过草稿模型配置")

    # ------------------- 同步调用 -------------------
    def run_task(self, prompt: str, response_format: str = "text") -> Optional[Any]:
        """同步执行任务"""
        try:
            # 如果启用Speculative Decoding，先用草稿模型生成
            if self.enable_speculative and hasattr(self, 'draft_api_key'):
                return self._run_with_speculative(prompt, response_format)
            else:
                return self._run_standard(prompt, response_format)

        except Exception as e:
            print(f"[AITaskRunner] 同步调用失败: {e}")
            return None

    def _run_standard(self, prompt: str, response_format: str = "text") -> Optional[Any]:
        """标准调用方式"""
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "max_tokens": self.max_tokens
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        timeout = 300
        resp = requests.post(f"{self.base_url}/chat/completions",
                             headers=headers, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        content = self._parse_response(data)
        if response_format == "json":
            return self._extract_json(content)
        return content

    def _run_with_speculative(self, prompt: str, response_format: str = "text") -> Optional[Any]:
        """使用Speculative Decoding的调用方式"""
        print(f"[Speculative] 开始推测执行，草稿模型: {self.draft_model}")

        # 先用草稿模型生成
        draft_payload = {
            "model": self.draft_model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "max_tokens": self.spec_config.get("draft_tokens", 150)
        }

        draft_headers = {
            "Authorization": f"Bearer {self.draft_api_key}",
            "Content-Type": "application/json"
        }

        try:
            # 草稿模型调用
            draft_resp = requests.post(f"{self.draft_base_url}/chat/completions",
                                     headers=draft_headers, json=draft_payload, timeout=60)
            draft_resp.raise_for_status()
            draft_data = draft_resp.json()
            draft_content = self._parse_response(draft_data)
            print(f"[Speculative] 草稿生成完成，长度: {len(draft_content)} 字符")

            # 主模型基于草稿进行优化
            enhanced_prompt = f"{prompt}\n\n参考草稿：\n{draft_content}\n\n请基于上述草稿进行改进和完善："

            main_payload = {
                "model": self.model,
                "messages": [{"role": "user", "content": enhanced_prompt}],
                "temperature": 0,
                "max_tokens": self.max_tokens
            }

            main_headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            main_resp = requests.post(f"{self.base_url}/chat/completions",
                                    headers=main_headers, json=main_payload, timeout=300)
            main_resp.raise_for_status()
            main_data = main_resp.json()
            main_content = self._parse_response(main_data)

            print(f"[Speculative] 主模型优化完成，最终长度: {len(main_content)} 字符")

            if response_format == "json":
                return self._extract_json(main_content)
            return main_content

        except Exception as e:
            print(f"[Speculative] 推测执行失败，回退到标准模式: {e}")
            return self._run_standard(prompt, response_format)

    # ------------------- 异步调用 -------------------
    async def run_task_async(self, prompt: str, response_format: str = "text") -> Optional[Any]:
        """异步执行任务，支持 speculative"""
        try:
            content = None
            if self.enable_speculative:
                if self.use_native_speculative and self.spec_config.get("supports_native", False):
                    content = await self._native_speculative_call_async(prompt)
                if content is None:
                    content = await self._compatible_speculative_call_async(prompt)
            else:
                content = await self._normal_call_async(prompt)

            if content is None:
                return None
            if response_format == "json":
                return self._extract_json(content)
            return content

        except Exception as e:
            print(f"[AITaskRunner Async] 调用失败: {e}")
            return None

    # ------------------- Speculative 异步调用 -------------------
    async def _native_speculative_call_async(self, prompt: str) -> Optional[str]:
        """原生 speculative 调用"""
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                payload = {
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0,
                    "max_tokens": self.max_tokens,
                    "speculative": {
                        "draft_model": self.spec_config["draft_model"],
                        "draft_tokens": self.spec_config["draft_tokens"]
                    }
                }
                resp = await client.post(f"{self.base_url}/chat/completions",
                                         headers={"Authorization": f"Bearer {self.api_key}"},
                                         json=payload)
                resp.raise_for_status()
                data = resp.json()
                return self._parse_response(data)
        except Exception as e:
            print(f"[native_speculative async] 原生调用失败，降级: {e}")
            return None

    async def _compatible_speculative_call_async(self, prompt: str) -> Optional[str]:
        """兼容模式 speculative 调用"""
        responses: List = []
        for i in range(self.speculative_times):
            try:
                model_to_use = self.model if i == 0 else self.spec_config["draft_model"]
                content = await self._normal_call_async(prompt, model=model_to_use)
                score = self._evaluate_response_quality(content, prompt)
                responses.append((content, score))
                await asyncio.sleep(0.05)
            except Exception as e:
                print(f"[compatible_speculative async] 第{i+1}次调用失败: {e}")

        if not responses:
            return None
        return max(responses, key=lambda x: x[1])[0]

    # ------------------- 普通异步调用 -------------------
    async def _normal_call_async(self, prompt: str, model: Optional[str] = None) -> Optional[str]:
        """异步普通调用，兼容任意 HTTP API"""
        model_to_use = model or self.model
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                payload: Dict[str, Any] = {
                    "model": model_to_use,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0,
                    "max_tokens": self.max_tokens
                }
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }

                resp = await client.post(f"{self.base_url}/chat/completions", headers=headers, json=payload)
                if resp.status_code != 200:
                    try:
                        error_data = resp.json()
                        print(f"[API错误] 状态码: {resp.status_code}, 详情: {error_data}")
                    except:
                        print(f"[API错误] 状态码: {resp.status_code}, 响应文本: {resp.text}")
                    return None

                data = resp.json()
                return self._parse_response(data)

        except Exception as e:
            print(f"[normal_call async] 调用失败: {e}")
            return None

    # ------------------- 响应处理 -------------------
    def _parse_response(self, data: Dict) -> Optional[str]:
        """解析返回结果，兼容 Chat / Embedding / Raw JSON"""
        if not data:
            return None
        # Chat 类型
        if "choices" in data and len(data["choices"]) > 0:
            return data["choices"][0].get("message", {}).get("content", "").strip()
        # Embedding 类型
        if "data" in data and len(data["data"]) > 0 and "embedding" in data["data"][0]:
            return data["data"][0]["embedding"]
        # 默认返回 JSON 字符串
        return json.dumps(data)

    def _extract_json(self, content: str) -> Optional[Dict]:
        """从响应中提取 JSON"""
        try:
            cleaned = re.sub(r"```(?:json)?\n(.*?)```", r"\1", content, flags=re.S).strip()
            cleaned = re.sub(r"<result>(.*?)</result>", r"\1", cleaned, flags=re.S).strip()
            return json.loads(cleaned)
        except:
            try:
                return json.loads(content)
            except:
                return {"raw_response": content}

    def _evaluate_response_quality(self, content: str, prompt: str) -> float:
        """评估响应质量，用于 speculative"""
        if not content:
            return 0.0
        score = 0.0
        if len(content) > 50:
            score += 0.3
        try:
            json.loads(content)
            score += 0.5
        except:
            pass
        if any(keyword in content for keyword in ["question", "rule", "data"]):
            score += 0.2
        return score

