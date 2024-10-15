import os
import json
import subprocess
import sys
sys.path.append(r'C:\Users\admin\Documents\GitHub\task\src')

# 定义文件路径
company_page_path = r'C:\Users\admin\Documents\GitHub\task\src\core\Company\CompanyList\CompanyPage.json'

# 读取 JSON 文件
def load_company_page():
    if not os.path.exists(company_page_path):
        print(f"Error: {company_page_path} does not exist.")
        return None

    with open(company_page_path, 'r') as f:
        company_page = json.load(f)
    return company_page

# 根据公司名称查找对应的爬虫脚本路径
def get_crawler_script(company_name):
    # 构建对应公司爬虫的路径
    crawler_path = os.path.join(r'C:\Users\admin\Documents\GitHub\task\src\core\Company\CompanyList', company_name, f'{company_name}Crawler.py')

    if os.path.exists(crawler_path):
        return crawler_path
    else:
        return None

# 运行指定的爬虫脚本
def run_crawler_script(crawler_script):
    try:
        result = subprocess.run(['python', crawler_script], check=True)
        if result.returncode == 0:
            print(f"Successfully ran the crawler for {crawler_script}")
        else:
            print(f"Error running the crawler for {crawler_script}")
    except subprocess.CalledProcessError as e:
        print(f"Error running the crawler script: {e}")

# 主程序
def main():
    # 加载公司页面
    company_page = load_company_page()
    if not company_page:
        return

    print("CompanyPage:", company_page)  # 打印公司页面内容进行调试

    # 遍历 company_page 中的公司名称并查找爬虫脚本路径
    for company_name in company_page:
        print(f"Processing company: {company_name}")

        # 查找爬虫脚本路径
        crawler_script = get_crawler_script(company_name)
        if crawler_script:
            print(f"Found crawler script for {company_name}: {crawler_script}")
            # 运行爬虫脚本
            run_crawler_script(crawler_script)
        else:
            print(f"No crawler found for {company_name}")

# 入口点
if __name__ == "__main__":
    main()
