import requests
import csv
import concurrent.futures
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError

# 处理相应字段的数据
def fetch_movie_data(movie_id):
    try:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key=6deed03784cec96e77ab2430599039f6&language=zh-CN"
        
        session = requests.Session()
        retry_strategy = HTTPAdapter(max_retries=3)  # 设置最大重试次数为3次
        session.mount('https://', retry_strategy)

        response = session.get(url, timeout=10)
        response.raise_for_status()
        
        movie_data = response.json()

        poster_path = movie_data.get('poster_path', '')
        poster_path = f"https://image.tmdb.org/t/p/original{poster_path}" if poster_path else ''

        title = movie_data.get('title', '')
        genres_list = movie_data.get('genres', [])
        genre_names = "|".join([genre['name'] for genre in genres_list])

        return {
            'id': movie_id,
            'poster_path': poster_path,
            'title': title,
            'genres': genre_names,
        }
    except RetryError:
        print(f"Max retries exceeded while fetching movie data for ID {movie_id}")
        return None
    except Exception as e:
        print(f"Error occurred while fetching movie data for ID {movie_id}: {e}")
        return None

# 获取电影的海报url，中文类别，中文标题
def get_json_data(movie_ids):
    try:
        movies_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            future_to_movie = {executor.submit(fetch_movie_data, movie_id): movie_id for movie_id in movie_ids}

            for future in concurrent.futures.as_completed(future_to_movie):
                movie_id = future_to_movie[future]
                try:
                    movie = future.result()
                    if movie:
                        movies_data.append(movie)
                        print(f"Movie ID: {movie_id}")
                        print(f"Poster Path: {movie['poster_path']}")
                        print(f"Title: {movie['title']}")
                        print(f"Genres: {movie['genres']}")
                        print("--------------------------------------")
                except Exception as e:
                    print(f"Error occurred while fetching movie data for ID {movie_id}: {e}")

        return movies_data
    except Exception as e:
        print(f"Error occurred during concurrent data fetching: {e}")

# 读取movies.csv，获取电影id
def read_movie_ids_from_csv(csv_file):
    try:
        movie_ids = []
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                movie_ids.append(row['id'])
        return movie_ids
    except Exception as e:
        print(f"Error occurred while reading movie IDs from CSV file: {e}")
        return []

def write_movie_data_to_csv(movies_data, output_csv_file):
    try:
        with open(output_csv_file, 'w', newline='', encoding='utf-8') as file:
            # writer = csv.DictWriter(file, fieldnames=['id', 'poster_path', 'title', 'genres', 'overview'])
            writer = csv.DictWriter(file, fieldnames=['id', 'poster_path', 'title', 'genres'])
            writer.writeheader()
            for movie_data in movies_data:
                writer.writerow(movie_data)
        print(f"Data successfully written to {output_csv_file}")
    except Exception as e:
        print(f"Error occurred while writing data to CSV file: {e}")

if __name__ == "__main__":
    input_csv_file = 'movies.csv'
    output_csv_file = 'extra_data.csv'

    movie_ids = read_movie_ids_from_csv(input_csv_file)
    movies_data = get_json_data(movie_ids)
    if movies_data:
        write_movie_data_to_csv(movies_data, output_csv_file)
