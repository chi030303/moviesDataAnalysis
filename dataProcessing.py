import pandas as pd

def clean_and_export_metadata(filepath, columns_to_drop, output_filepath='movies_metadata.csv'):
    """
    清理元数据，通过删除指定的列，并将清理后的数据输出到一个 CSV 文件中。

    参数：
    - filepath: str
        输入元数据 CSV 文件的文件路径。
    - columns_to_drop: list of str
        要删除的列名列表。
    - output_filepath: str, 可选
        输出清理后的元数据 CSV 文件的文件路径。默认为 'cleaned_metadata.csv'。
    """
    # 读取数据
    metadata = pd.read_csv(filepath)
    
    # 删除指定的列
    metadata_cleaned = metadata.drop(columns=columns_to_drop, errors='ignore')
    return metadata_cleaned

# 提取并分割字段下的多个值
def split_data(data, columns):
    for column in columns:
        try:
            # 将字段的字符串转换为列表
            data[column] = data[column].apply(lambda x: [entry['name'] for entry in eval(x)])
            # 将列表中的字符串连接起来，用'|'分隔
            data[column] = data[column].apply(lambda x: '|'.join(x))
        except Exception as e:
            print(f"Error occurred while processing column '{column}': {e}")

# 提取poster_path列并写入另一个文件
def extract_and_remove_column(metadata, column_name, output_filepath):
    # 提取poster_path和对应的id，写入另一个文件
    poster_data = metadata[['id', column_name]]
    poster_data.to_csv(output_filepath, index=False)

    # 删除poster_path列
    metadata.drop(columns=[column_name], inplace=True)

def data_processing(filepath, output_filepath):
    try:
        metadata = pd.read_csv(filepath)
        metadata = clean_and_export_metadata(filepath,['belongs_to_collection', 'homepage', 'tagline', 'video', 'adult', 'imdb_id'])
        
        # 需要处理的字段
        columns_to_process = ['production_companies', 'production_countries', 'spoken_languages', 'genres']
        split_data(metadata, columns_to_process)

        # 提取poster_path和对应的id，写入另一个文件，并删除poster_path列
        extract_and_remove_column(metadata, 'poster_path', "./poster_path.csv")

        new_column_order = ['id', 'title', 'original_title', 'genres','original_language','spoken_languages','overview','runtime','release_date','production_companies','production_countries','status','budget','revenue','popularity','vote_count','vote_average']

        # 使用 reindex 方法重新排列列顺序
        metadata = metadata.reindex(columns=new_column_order)

        # 根据 "id" 列的值进行升序排序
        metadata.sort_values(by='id', ascending=True, inplace=True)
        metadata.to_csv(output_filepath, index=False)
    except Exception as e:
        print("An error occurred during data processing:", e)


# 提取样本
def extract_sample(filepath,output_filepath):
    # 读取整个 CSV 文件
    data = pd.read_csv(filepath)

    # 截取前十列
    data_subset = data.head(10)

    # 将截取后的数据导出为新的 CSV 文件
    data_subset.to_csv(output_filepath, index=False)

# 处理credits.csv文件
# 需要把演员分割了，需要把导演找出来
def handle_credits(filepath,output_filepath):
    credits = pd.read_csv(filepath)

    # 在 crew 列中查找所有 job 属性为 "Director" 的数据条目
    director_data = []
    actor_data = []
    crew_dicts = credits['crew'].apply(lambda x: eval(x))
    cast_dicts = credits['cast'].apply(lambda x: eval(x))
    for idx, credit_member in enumerate(crew_dicts):
        director_names = '|'.join([member['name'] for member in credit_member if member['job'] == 'Director'])

        # 仅获取前15个演员的名字和角色
        actors = [member for member in cast_dicts[idx]][:15]
        actor_names = '|'.join([member['name'] for member in actors])
        character_names = '|'.join([member['character'] for member in actors])

        director_data.append({'id': credits.loc[idx, 'id'], 'director': director_names, 'actor': actor_names, 'character': character_names})

    # 创建包含导演名字和对应 ID 的 DataFrame
    credits_df = pd.DataFrame(director_data)

    # 将导演信息写入到新的 CSV 文件中
    credits_df.to_csv(output_filepath, index=False)

# 处理keywords.csv数据集
def handle_keywords(filepath,output_filepath):
    keywords = pd.read_csv(filepath)

    # 提取keywords字段下的id和name，写入到新的DataFrame
    keyword_data = []
    for idx, row in keywords.iterrows():
        movie_id = row['id']
        for keyword in eval(row['keywords']):
            keyword_data.append({'movieId': movie_id, 'userId': keyword['id'], 'tag': keyword['name']})

    # 创建包含userId、tag和movieId的DataFrame
    keyword_df = pd.DataFrame(keyword_data)

    # 保存结果到新的CSV文件
    keyword_df.to_csv(output_filepath, index=False)

def merge_datasets(filepath):
    try:
        metadata = pd.read_csv(filepath)
        # 读取 credits 数据
        handle_credits("../archive/credits.csv","./credits.csv")
        credits = pd.read_csv("./credits.csv")

        # 将 credits 数据集中的 'id' 列转换为与 metadata 数据集中 'id' 列相同的数据类型
        credits['id'] = credits['id'].astype(metadata['id'].dtype)

        # 将演员、饰演角色、导演合并到电影数据集中
        metadata = metadata.merge(credits, on='id')
    except Exception as e:
        print("An error occurred while processing credits.csv:", e)

if __name__ == "__main__":
    # 提取样本试验函数功能
    # extract_sample("../archive/movies_metadata.csv","./metadatatest.csv")

    # 处理数据，删除多余字段，对一些字段值进行分割；并将处理后的数据写入原文件
    data_processing("../archive/movies_metadata.csv","./movies.csv")
    # handle_keywords("../archive/keywords.csv","./keywords.csv")
    merge_datasets("./movies.csv")