import os
import re


def split_number_text(example):
    numbers = re.findall(r"(\d+(\.\d+([Ee]\d+)?)?)", example)
    text = re.sub(r"(\d+(\.\d+([Ee]\d+)?)?)", "", example)
    return numbers, text


def adjust_result(num_fraction1, num_fraction2, value):
    if num_fraction1 == 0 and num_fraction2 == 0:
        return 0
    return 2 * num_fraction1 * num_fraction2 * value / (num_fraction1 + num_fraction2)


def read_extractions(folder_path, dest_folder):
    for sub_name in os.listdir(folder_path):
        sub_path = os.path.join(folder_path, sub_name, "clusters")
        if os.path.isdir(sub_path):
            with open(os.path.join(dest_folder, sub_name + ".jl"), "w+") as writer:
                for sub_name2 in os.listdir(sub_path):
                    sub_path2 = os.path.join(sub_path, sub_name2)
                    if os.path.isdir(sub_path2):
                        for file_name in os.listdir(sub_path2):
                            if file_name.endswith(".jl"):
                                file_path = os.path.join(sub_path2, file_name)
                                with open(file_path, "r") as reader:
                                    writer.write(reader.read())

if __name__ == "__main__":
    pass
    # read_extractions("/Users/minhpham/projects/dig-data/sample-datasets/escorts", "raw_data/test-memex")
