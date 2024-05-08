import pandas as pd

# 删除不需要的列
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
def handle_credits(filepath, output_filepath):
    try:
        credits = pd.read_csv(filepath)

        # 初始化导演、演员和角色列表
        director_list = []
        actor_list = []
        character_list = []

        # 处理 crew 列中的导演信息
        for crew_data in credits['crew']:
            crew_data = eval(crew_data)  # 将字符串转换为列表
            directors = [member['name'] for member in crew_data if member['job'] == 'Director']
            director_list.append('|'.join(directors) if directors else '')  # 如果没有导演，将其设为空字符串

        # 处理 cast 列中的演员和角色信息，仅获取前15个演员
        for cast_data in credits['cast']:
            cast_data = eval(cast_data)  # 将字符串转换为列表
            actors = [member['name'] for member in cast_data[:15]]
            characters = [member['character'] for member in cast_data[:15]]
            actor_list.append('|'.join(actors))
            character_list.append('|'.join(characters))

        # 创建包含导演、演员和角色信息的 DataFrame
        credits_df = pd.DataFrame({'id': credits['id'], 'director': director_list, 'actor': actor_list, 'character': character_list})

        # 将导演、演员和角色信息写入到新的 CSV 文件中
        credits_df.to_csv(output_filepath, index=False)

    except Exception as e:
        print("An error occurred while handling credits.csv:", e)

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

# 将演员、导演数据合并到电影数据集里
def concat_datasets(metadata_filepath, credits_filepath):
    try:
        # 读取 metadata 和 credits 数据集
        metadata = pd.read_csv(metadata_filepath)
        handle_credits(credits_filepath, "./credits.csv")
        credits = pd.read_csv("./credits.csv")

        # 选择 credits 数据集中的 director、actor 和 character 列
        credits_selected = credits[['id', 'director', 'actor', 'character']]

        # 使用 concat 函数将 metadata 数据集和 credits 中选定的列堆叠在一起
        merged_data = pd.concat([metadata, credits_selected], axis=1)

        # 保存合并后的数据集到文件
        merged_data.to_csv("./movies.csv", index=False)

    except Exception as e:
        print("An error occurred while concatenating datasets:", e)

if __name__ == "__main__":
    # 提取样本试验函数功能
    # extract_sample("../archive/movies_metadata.csv","./metadatatest.csv")

    # 处理数据，删除多余字段，对一些字段值进行分割；并将处理后的数据写入原文件
    data_processing("../archive/movies_metadata.csv","./movies.csv")
    # 将movies数据集与credits数据集合并
    concat_datasets("./movies.csv","../archive/credits.csv")
    # 提取keywords，生成keywords数据集
    handle_keywords("../archive/keywords.csv","./keywords.csv")
    