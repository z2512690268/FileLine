#!/bin/bash
# source /root/miniconda3/etc/profile.d/conda.sh
# conda activate llama
# 检查传入参数的数量
if [ "$#" -eq 1 ]; then
  # 如果只有一个参数
  Y=$1
  echo "正在执行: python main.py pipeline run $Y"
  python main.py pipeline run "$Y"
elif [ "$#" -eq 2 ]; then
  # 如果有两个参数
  Y=$1
  G=$2
  echo "正在执行: python main.py pipeline run $Y --global-config $G"
  python main.py pipeline run "$Y" --global-config "$G"
else
  # 如果参数数量不符合要求，打印使用说明
  echo "使用方法:"
  echo "  请提供Yaml路径: $0 [Y]"
  echo "  请提供Yaml和Global配置文件路径: $0 [Y] [G]"
  exit 1
fi