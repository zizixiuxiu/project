#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

def debug_boundary_detection():
    """调试边界检测"""

    test_content = """
    评估要点：
    - 财务管理规范性
    - 资金使用透明度
    - 审计制度完善程度

    区域性商会管理评估体系
    组织架构评估
    """

    # 现有的正则表达式模式
    next_evaluation_patterns = [
        r'全市性.*?评估指标$',
        r'.*?评估标准$',
        r'.*?评估办法$',
        r'.*?评估体系$',
        r'.*?管理评估.*?$',
        r'民办非企业单位.*?评估.*?指标$',
        r'基金会.*?评估.*?指标$',
        r'社会团体.*?评估.*?指标$',
        r'异地商会.*?评估.*?指标$',
        r'行业协会.*?评估.*?指标$',
        r'区域性.*?评估.*?$',
        r'.*?商会.*?评估.*?$'
    ]

    lines = test_content.split('\n')

    for i, line in enumerate(lines):
        line_stripped = line.strip()
        print(f"第{i+1}行: '{line_stripped}'")

        if line_stripped == "区域性商会管理评估体系":
            print(f"  -> 目标行找到!")

            # 测试每个模式
            for j, pattern in enumerate(next_evaluation_patterns):
                if re.search(pattern, line_stripped):
                    print(f"  -> 匹配模式{j+1}: {pattern}")
                else:
                    print(f"  -> 不匹配模式{j+1}: {pattern}")

            # 检查额外验证条件
            print(f"  -> 长度检查: {len(line_stripped)} < 50? {len(line_stripped) < 50}")
            print(f"  -> 包含'评估': {'评估' in line_stripped}")
            print(f"  -> 包含关键词: {('指标' in line_stripped or '标准' in line_stripped or '办法' in line_stripped or '体系' in line_stripped)}")

if __name__ == "__main__":
    debug_boundary_detection()