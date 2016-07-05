import os
import re


def split_number_text(example):
    numbers = re.findall(r"(\d+(\.\d+([Ee]\d+)?)?)", example)
    text = re.sub(r"(\d+(\.\d+([Ee]\d+)?)?)", "", example)
    return numbers, text


def adjust_result(value, num_fraction1, num_fraction2):
    return 2 * num_fraction1 * num_fraction2 * value / (num_fraction1 + num_fraction2)


def read_extractions(folder_path, dest_folder):
    for sub_path in os.listdir(folder_path):
        sub_path = os.path.join(folder_path, sub_path, "clusters")
        if os.path.isdir(sub_path):
            for sub_path2 in os.listdir(sub_path):
                sub_path2 = os.path.join(sub_path, sub_path2)
                if os.path.isdir(sub_path2):
                    for file_name in os.listdir(sub_path2):
                        if file_name.endswith(".jl"):
                            file_path = os.path.join(sub_path2, file_name)
                            os.rename(file_path, os.path.join(dest_folder, file_name))


if __name__ == "__main__":
    read_extractions("/Users/minhpham/projects/dig-data/sample-datasets/escorts", "data/test-memex")
