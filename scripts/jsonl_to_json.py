import json
import sys


def jsonl_to_json(input_file, output_file=None, output_as_array=True):
    """
    将 JSONL 文件转换为 JSON 文件

    参数:
        input_file: 输入的 JSONL 文件路径
        output_file: 输出的 JSON 文件路径（默认与输入文件同名，扩展名改为 .json）
        output_as_array: 是否输出为 JSON 数组（True）还是保持换行分隔（False）
    """

    # 默认输出文件名
    if output_file is None:
        output_file = input_file.rsplit('.jsonl', 1)[0] + '.json'

    # 读取 JSONL 文件
    data = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:  # 跳过空行
                continue
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"警告: 第 {line_num} 行解析失败: {e}")
                continue

    # 写入 JSON 文件
    with open(output_file, 'w', encoding='utf-8') as f:
        if output_as_array:
            # 输出为标准 JSON 数组
            json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            # 输出为换行分隔的 JSON（与 JSONL 相同格式，只是扩展名不同）
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')

    print(f"转换完成！共处理 {len(data)} 条记录")
    print(f"输出文件: {output_file}")
    return data


# 使用示例
if __name__ == "__main__":
    # 方式1: 直接调用函数
    # jsonl_to_json('data.jsonl')
    # jsonl_to_json('data.jsonl', 'output.json')

    # 方式2: 命令行参数
    if len(sys.argv) < 2:
        print("用法: python jsonl_to_json.py <input.jsonl> [output.json]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    jsonl_to_json(input_path, output_path)