import csv
from collections import Counter

def analyze_words(filename="짬처리.csv"):
    """
    CSV 파일에서 모든 단어의 빈도를 분석합니다.

    Args:
        filename (str): 분석할 CSV 파일 이름 (기본값: "cleaned.csv").

    Returns:
        Counter: 단어별 빈도를 담은 Counter 객체.
                 오류 발생 시 None을 반환합니다.
    """
    word_counts = Counter()
    try:
        with open(filename, 'r', encoding='cp949') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                for cell in row:
                    words = cell.lower().split()
                    word_counts.update(words)
        return word_counts
    except FileNotFoundError:
        print(f"오류: '{filename}' 파일을 찾을 수 없습니다.")
        return None
    except Exception as e:
        print(f"오류 발생: {e}")
        return None

def find_frequent_words_count(filename="짬처리.csv", min_count=3):
    """
    CSV 파일에서 특정 횟수 이상 등장하는 단어들의 총 개수를 반환합니다.

    Args:
        filename (str): 분석할 CSV 파일 이름 (기본값: "cleaned.csv").
        min_count (int): 최소 등장 횟수 (기본값: 5).

    Returns:
        int: 최소 등장 횟수 이상 나타난 단어들의 총 개수.
             파일을 찾을 수 없거나 오류 발생 시 0을 반환합니다.
    """
    word_counts = analyze_words(filename)
    if word_counts is None:
        return 0

    frequent_word_count = sum(1 for count in word_counts.values() if count >= min_count)
    return frequent_word_count

def find_frequent_word_list(filename="짬처리.csv", min_count=5):
    """
    CSV 파일에서 특정 횟수 이상 등장하는 단어들의 리스트를 반환합니다.

    Args:
        filename (str): 분석할 CSV 파일 이름 (기본값: "cleaned.csv").
        min_count (int): 최소 등장 횟수 (기본값: 5).

    Returns:
        list: 최소 등장 횟수 이상 나타난 단어들의 리스트.
              파일을 찾을 수 없거나 오류 발생 시 빈 리스트를 반환합니다.
    """
    word_counts = analyze_words(filename)
    if word_counts is None:
        return []

    frequent_words = [word for word, count in word_counts.items() if count >= min_count]
    return frequent_words

if __name__ == "__main__":
    frequent_count = find_frequent_words_count()
    frequent_word_list = find_frequent_word_list()

    if frequent_word_list is not None:
        print(f"'cleaned.csv' 파일 분석 결과:")
        print(f"- {5}번 이상 등장하는 단어 개수: {frequent_count}개")
        print(f"- {5}번 이상 등장하는 단어 목록:")
        print(frequent_word_list)