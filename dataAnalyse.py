import pandas as pd
import matplotlib.pyplot as plt
import ast
import numpy as np

# 分析电影数据集，包括movie_metadata.csv, keywords.csv, ratings.csv, credits.csv

# 提取adults、status、video字段的具体取值情况
def check_values(metadata):
    print("Values of 'adult' column:")
    print(metadata['adult'].value_counts())

    print("\nValues of 'status' column:")
    print(metadata['status'].value_counts())

    print("\nValues of 'video' column:")
    print(metadata['video'].value_counts())

    # 获取 'adult' 字段的具体取值情况
    adult_values = metadata['adult'].value_counts()

    # 绘制 'adult' 字段的柱状图
    plt.figure(figsize=(8, 6))
    adult_values.plot(kind='bar', color='skyblue')
    plt.title('Values of "adult" column')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

    # 获取 'status' 字段的具体取值情况
    status_values = metadata['status'].value_counts()

    # 绘制 'status' 字段的柱状图
    plt.figure(figsize=(8, 6))
    status_values.plot(kind='bar', color='lightgreen')
    plt.title('Values of "status" column')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

    # 获取 'video' 字段的具体取值情况
    video_values = metadata['video'].value_counts()

    # 绘制 'video' 字段的柱状图
    plt.figure(figsize=(8, 6))
    video_values.plot(kind='bar', color='lightcoral')
    plt.title('Values of "video" column')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

# 查看各字段的值结构和缺省值情况
def check_nullValues(metadata):

    # 获取变量类型和缺失值信息
    tab_info = pd.DataFrame(metadata.dtypes).T.rename(index={0: 'column type'})
    tab_info = pd.concat([tab_info, pd.DataFrame(metadata.isnull().sum()).T.rename(index={0: 'null values'})])
    tab_info = pd.concat([tab_info, pd.DataFrame(metadata.isnull().sum() / metadata.shape[0] * 100).T.
                         rename(index={0: 'null values (%)'})])
    
    # 保存为CSV文件
    tab_info.to_csv('metadata_info.csv')

    # 绘制缺省值数量柱状图
    plt.figure(figsize=(10, 6))
    ax1 = tab_info.iloc[1].plot(kind='bar', color='blue', alpha=0.7, label='Null Values')
    plt.ylabel('Number of Null Values')
    plt.xlabel('Columns')
    plt.title('Number of Null Values in Each Column')
    plt.xticks(rotation=45)
    plt.legend()

    # 在每个柱形上方添加具体值
    for i in ax1.patches:
        plt.text(i.get_x() + i.get_width() / 2, i.get_height() + 0.1, str(int(i.get_height())), ha='center')

    plt.tight_layout()
    plt.show()

    # 绘制缺省值占比柱状图
    plt.figure(figsize=(10, 6))
    ax2 = tab_info.iloc[2].plot(kind='bar', color='orange', alpha=0.7, label='Null Values (%)')
    plt.ylabel('Percentage of Null Values (%)')
    plt.xlabel('Columns')
    plt.title('Percentage of Null Values in Each Column')
    plt.xticks(rotation=45)
    plt.legend()

    # 在每个柱形上方添加具体值
    for i in ax2.patches:
        plt.text(i.get_x() + i.get_width() / 2, i.get_height() + 0.1, f"{i.get_height():.2f}%", ha='center')

    plt.tight_layout()
    plt.show()

def time_language_histogram(metadata):
    # 限制电影播放时长(runtime)的范围
    metadata = metadata[metadata['runtime'] <= 780]

    # 绘制电影播放时长(runtime)的直方图
    plt.figure(figsize=(10, 6))
    plt.hist(metadata['runtime'].dropna(), bins=30, color='skyblue', edgecolor='black')
    plt.xlabel('Runtime')
    plt.ylabel('Frequency')
    plt.title('Histogram of Movie Runtime')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

    # 绘制不同语言电影数量(original_language)的直方图
    plt.figure(figsize=(10, 6))
    metadata['original_language'].value_counts().plot(kind='bar', color='lightgreen')
    plt.xlabel('Original Language')
    plt.ylabel('Number of Movies')
    plt.title('Histogram of Number of Movies by Original Language')
    plt.xticks(rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

    # 打印每个具体值的频率表格
    runtime_counts = metadata['runtime'].value_counts().reset_index().rename(columns={'index': 'Runtime', 'runtime': 'Count'})
    language_counts = metadata['original_language'].value_counts().reset_index().rename(columns={'index': 'Original Language', 'original_language': 'Count'})

    # 将数据导出为CSV文件
    runtime_counts.to_csv('runtime_counts.csv', index=False)
    language_counts.to_csv('language_counts.csv', index=False)

# 分析源数据
def metadata_analyse(filepath):
    metadata = pd.read_csv(filepath)

    check_values(metadata)

    check_nullValues(metadata)

    time_language_histogram(metadata)

# 分析'genres','production_countries','production_companies','spoken_languages'字段的数据
def check_data(filepath, columns):
    # 读取数据集，指定分隔符为 "|"
    metadata = pd.read_csv(filepath, low_memory=False)

    # 遍历每一列
    for column in columns:
        # 统计每个独立的值出现的次数
        data_counts = {}

        # 遍历每一行
        for index, row in metadata.iterrows():
            # 将当前行的数据转换为字符串类型，然后拆分成单个值，并去重
            data_list = str(row[column]).split("|")
            # 统计每个值的出现次数
            for data in data_list:
                data_counts[data] = data_counts.get(data, 0) + 1

        # 打印每个值的计数
        for data, count in data_counts.items():
            print(f"{data}: {count}")

        print("Total unique values:", len(data_counts))

if __name__ == "__main__":
    # metadata_analyse("../archive/movies_metadata.csv")
    columns = ['genres','production_countries','spoken_languages']
    check_data("./movies.csv",columns)
    
