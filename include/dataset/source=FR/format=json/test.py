import json
import os
import re

def clean_field_names(data):
    """
    Đệ quy duyệt qua JSON và thay thế các tên trường không hợp lệ bằng tên hợp lệ
    """
    if isinstance(data, dict):
        new_data = {}
        for key, value in data.items():
            # Thay dấu cách bằng dấu gạch dưới và đảm bảo tên trường hợp lệ
            new_key = re.sub(r'\s+', '_', key)
            new_data[new_key] = clean_field_names(value)
        return new_data
    elif isinstance(data, list):
        return [clean_field_names(item) for item in data]
    else:
        return data

def process_json_file(input_file, output_file):
    with open(input_file, 'r') as infile:
        data = [json.loads(line) for line in infile]  # Load newline-delimited JSON
    
    # Thay thế tên trường không hợp lệ
    cleaned_data = [clean_field_names(item) for item in data]
    
    with open(output_file, 'w') as outfile:
        for item in cleaned_data:
            json.dump(item, outfile)
            outfile.write('\n')  # Ghi dữ liệu đã làm sạch vào file đầu ra
            
input_folder = 'D:/Desktop/dbt/include/dataset/source=FR/format=json'
output_folder = 'D:/Desktop/dbt/include/dataset/source=FR/format=json'

for root, dirs, files in os.walk(input_folder):
    for filename in files:
        if filename.endswith('.json'):
            input_file_path = os.path.join(root, filename)
            relative_path = os.path.relpath(root, input_folder)
            output_subfolder = os.path.join(output_folder, relative_path)
            os.makedirs(output_subfolder, exist_ok=True)
            
            output_file_path = os.path.join(output_subfolder, filename)
            
            # Thực hiện chuyển đổi
            process_json_file(input_file_path, output_file_path)

print("Field name cleanup completed!")