import json

def addnum(file_path):
    """
    JSON 파일의 데이터를 'no' 필드 값을 기준으로 내림차순으로 정렬합니다.

    Args:
        file_path (str): JSON 파일의 경로.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: 파일을 찾을 수 없습니다: {file_path}")
        return
    except json.JSONDecodeError:
        print(f"Error: JSON 디코딩 오류: {file_path}")
        return

    for item in data:
        if 'no' in item:
            item['no'] = item['no'] - 104460

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"'{file_path}' 10만더하기 성공")
    except IOError:
        print(f"Error: 파일 쓰기 오류: {file_path}")

def reverse_json_no(file_path):
    """
    JSON 파일에서 'no' 필드의 값을 역순으로 변경합니다.

    Args:
        file_path (str): JSON 파일의 경로.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: 파일을 찾을 수 없습니다: {file_path}")
        return
    except json.JSONDecodeError:
        print(f"Error: JSON 디코딩 오류: {file_path}")
        return

    max_no = 105860  # 알려주신 'no' 필드의 최댓값

    for item in data:
        if 'no' in item:
            item['no'] = max_no - item['no'] + 1

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"'{file_path}' 파일의 'no' 필드 값을 역순으로 변경했습니다.")
    except IOError:
        print(f"Error: 파일 쓰기 오류: {file_path}")

def sort_json_by_no_ascending(file_path):
    """
    JSON 파일의 데이터를 'no' 필드 값을 기준으로 오름차순으로 정렬합니다.

    Args:
        file_path (str): JSON 파일의 경로.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: 파일을 찾을 수 없습니다: {file_path}")
        return
    except json.JSONDecodeError:
        print(f"Error: JSON 디코딩 오류: {file_path}")
        return

    # 'no' 필드 값을 기준으로 오름차순 정렬 (reverse=False 또는 생략)
    sorted_data = sorted(data, key=lambda item: item.get('no', 0))

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, indent=4, ensure_ascii=False)
        print(f"'{file_path}' 파일의 데이터를 'no' 필드 기준으로 오름차순 정렬했습니다.")
    except IOError:
        print(f"Error: 파일 쓰기 오류: {file_path}")

# 사용할 JSON 파일 경로를 여기에 입력하세요.
file_path = 'add.json'
#reverse_json_no(file_path)
#sort_json_by_no_ascending(file_path)
addnum(file_path)