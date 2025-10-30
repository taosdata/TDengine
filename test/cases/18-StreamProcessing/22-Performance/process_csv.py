import csv
import os
import argparse
import sys

def is_number(value):
    """判断字符串是否为纯数字(整数或浮点数)"""
    if value == '' or value is None:
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False

def add_tbname_column(input_file, output_file, prefix='r_', include_header=True):
    """
    在CSV文件最前面添加tbname列,内容为 prefix + name列的值
    非纯数字的字符串值加上单引号,纯数字不加引号
    空值填充为0
    适用于大文件处理,逐行读写,内存占用小
    
    Args:
        input_file: 输入CSV文件路径
        output_file: 输出CSV文件路径
        prefix: tbname列的前缀,默认为'r_'
        include_header: 是否包含表头,默认为True
    """
    print(f"开始处理文件: {input_file}")
    print(f"tbname前缀: {prefix}")
    print(f"包含表头: {'是' if include_header else '否'}")
    
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误: 文件不存在 - {input_file}")
        return False
    
    # 获取文件大小
    file_size = os.path.getsize(input_file)
    print(f"文件大小: {file_size / (1024*1024):.2f} MB")
    
    processed_rows = 0
    empty_count = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.DictReader(infile)
            
            # 检查name列是否存在
            if 'name' not in reader.fieldnames:
                print("错误: CSV文件中没有找到'name'列")
                return False
            
            # 新的字段名列表: tbname放在最前面
            fieldnames = ['tbname'] + reader.fieldnames
            
            # 使用csv.writer手动写入,以便控制引号
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONE, escapechar='\\')
            
            # 根据参数决定是否写入表头
            if include_header:
                writer.writerow(fieldnames)
            
            # 逐行处理,适合大文件
            for row in reader:
                # 添加tbname列,值为 'prefix' + name (tbname总是字符串,加引号)
                tbname = f"'{prefix}{row['name']}'"
                
                # 构建新行
                new_row = [tbname]
                for field in reader.fieldnames:
                    value = row[field]
                    # 如果值为空,填充为0
                    if value == '' or value is None:
                        new_row.append('0')
                        empty_count += 1
                    # 如果是纯数字,不加引号
                    elif is_number(value):
                        new_row.append(value)
                    # 非纯数字的字符串,加上单引号
                    else:
                        new_row.append(f"'{value}'")
                
                writer.writerow(new_row)
                
                processed_rows += 1
                
                # 每处理10000行显示一次进度
                if processed_rows % 10000 == 0:
                    print(f"已处理 {processed_rows} 行...")
        
        print(f"\n处理完成!")
        print(f"总共处理了 {processed_rows} 行数据")
        print(f"空值填充为0的数量: {empty_count}")
        print(f"输出文件: {output_file}")
        
        # 显示输出文件大小
        output_size = os.path.getsize(output_file)
        print(f"输出文件大小: {output_size / (1024*1024):.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"处理过程中出错: {e}")
        return False

def main():
    """主函数,处理命令行参数"""
    parser = argparse.ArgumentParser(
        description='CSV文件处理工具: 在CSV文件最前面添加tbname列',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 基本用法(使用默认前缀 r_,保留表头)
  python3 process_csv.py -i /root/core/readings1.csv
  
  # 指定输出文件
  python3 process_csv.py -i /root/core/readings1.csv -o /root/core/output.csv
  
  # 指定自定义前缀
  python3 process_csv.py -i /root/core/readings1.csv -p d_
  
  # 不保留表头(适合数据库导入)
  python3 process_csv.py -i /root/core/readings1.csv --no-header
  
  # 完整示例(所有参数)
  python3 process_csv.py -i /root/core/readings1.csv -o /root/core/result.csv -p truck_r_ --no-header

功能说明:
  1. 在CSV文件最前面添加tbname列
  2. tbname列的值 = 前缀 + name列的值
  3. 字符串值自动添加单引号,纯数字不加引号
  4. 空值自动填充为0
  5. 支持大文件处理(逐行读写,内存占用小)
  6. 可选择是否保留表头(默认保留)
        '''
    )
    
    parser.add_argument(
        '-i', '--input',
        required=True,
        help='输入CSV文件的完整路径(必需)',
        metavar='FILE'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='输出CSV文件的完整路径(可选,默认在输入文件同目录下生成 *_with_tbname.csv)',
        metavar='FILE'
    )
    
    parser.add_argument(
        '-p', '--prefix',
        default='r_',
        help='tbname列的前缀(可选,默认为 r_)',
        metavar='PREFIX'
    )
    
    parser.add_argument(
        '--no-header',
        action='store_true',
        help='不包含表头(默认保留表头,添加此参数则不保留表头,适合数据库导入)'
    )
    
    args = parser.parse_args()
    
    # 处理输入文件路径
    input_file = args.input
    
    # 处理输出文件路径
    if args.output:
        output_file = args.output
    else:
        # 自动生成输出文件名
        input_dir = os.path.dirname(input_file)
        input_basename = os.path.basename(input_file)
        input_name, input_ext = os.path.splitext(input_basename)
        output_file = os.path.join(input_dir, f"{input_name}_with_tbname{input_ext}")
    
    # 处理前缀
    prefix = args.prefix
    
    # 处理表头选项
    include_header = not args.no_header
    
    # 执行转换
    success = add_tbname_column(input_file, output_file, prefix, include_header)
    
    if success:
        print("\n预览前5行结果:")
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i < 5:
                        print(line.strip())
                    else:
                        break
        except Exception as e:
            print(f"预览失败: {e}")
        
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()