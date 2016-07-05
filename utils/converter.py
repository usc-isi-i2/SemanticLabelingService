import csv
import json
import os
from collections import OrderedDict
import sys
from xml.etree import ElementTree
from string import digits


def get_semantic_type(semantic_object):
    domain = semantic_object["domain"]["uri"].split("/")[-1]
    _type = semantic_object["type"]["uri"].split("/")[-1]
    return domain + "---" + _type


def convert_museum_to_std(folder_path):
    os.chdir(folder_path)

    for file_path in os.listdir("data"):

        semantic_type_map = OrderedDict()

        with open(os.path.join("model", "%s.model.json" % file_path), 'r') as f:
            data = json.load(f)
            node_array = data["graph"]["nodes"]

            for node in node_array:
                if "userSemanticTypes" in node:
                    semantic_object = node["userSemanticTypes"]
                    name = node["columnName"]
                    semantic_type_map[name] = get_semantic_type(semantic_object[0])

        obj_list = []

        if file_path.endswith("xml"):
            xml_tree = ElementTree.parse(os.path.join("data", file_path))
            root = xml_tree.getroot()

            for child in root:
                obj = {}
                for attrib_name in child.attrib.keys():
                    if attrib_name not in semantic_type_map:
                        obj[attrib_name] = child.attrib[attrib_name]
                obj_list.append(obj)
            obj_list.insert(0, semantic_type_map)

        elif file_path.endswith("json"):
            with open(os.path.join("data", file_path), 'r') as f:
                obj_list = json.load(f)
            obj_list.insert(0, semantic_type_map)

        else:
            with open(os.path.join("data", file_path)) as csv_file:
                reader = csv.DictReader(csv_file)
                headers = reader.fieldnames
                for row in reader:
                    obj = {}
                    for header in headers:
                        obj[header] = row[header]
                    obj_list.append({key: obj[key] for key in semantic_type_map if key in obj})
            obj_list.insert(0, semantic_type_map)

        name = os.path.splitext(file_path)[0]

        with open('../../data/museum/%s.csv' % name, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, semantic_type_map.keys(), delimiter=",", quotechar='"',
                                         quoting=csv.QUOTE_ALL)
            dict_writer.writeheader()
            for obj in obj_list:
                dict_writer.writerow(
                    {key: obj[key] for key in semantic_type_map if
                     key in obj})

    os.chdir("../..")


def convert_soccer_to_std(folder_path):
    os.chdir(folder_path)

    for file_path in os.listdir("data"):

        semantic_type_map = OrderedDict()

        with open(os.path.join("model", "%s.model.json" % file_path), 'r') as f:
            data = json.load(f)
            node_array = data["graph"]["nodes"]

            for node in node_array:
                if "userSemanticTypes" in node:
                    semantic_object = node["userSemanticTypes"]
                    name = node["columnName"]
                    semantic_type_map[name] = get_semantic_type(semantic_object[0])

        obj_list = []

        with open(os.path.join("data", file_path)) as csv_file:
            reader = csv.DictReader(csv_file)
            headers = reader.fieldnames
            for row in reader:
                obj = {}
                for header in headers:
                    obj[header] = row[header]
                obj_list.append({key: obj[key] for key in semantic_type_map if key in obj})
        obj_list.insert(0, semantic_type_map)

        name = os.path.splitext(file_path)[0]

        with open('../../data/soccer/%s.csv' % name, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, semantic_type_map.keys(), delimiter=",", quotechar='"',
                                         quoting=csv.QUOTE_ALL)
            dict_writer.writeheader()
            dict_writer.writerows(obj_list)

    os.chdir("../..")


def convert_others_to_std(folder_path, data_set_name):
    os.chdir(folder_path)

    for file_path in os.listdir("data"):

        semantic_type_map = OrderedDict()

        data_type_map = OrderedDict()

        with open(os.path.join("data", file_path), 'r') as f:
            num_types = int(f.readline())
            f.readline()
            for num_type in range(num_types):
                semantic_type = f.readline().strip()
                semantic_type_map[num_type] = "---".join(
                    [part.split("/")[-1] for part in semantic_type.replace("#", "").split("|")])
                num_values = int(f.readline())
                data_type_map[num_type] = []
                for num_val in range(num_values):
                    data_type_map[num_type].append(f.readline().split(" ", 1)[1].strip())
                f.readline()

        name = os.path.splitext(file_path)[0]

        with open("../../data/%s/%s.csv" % (data_set_name, name), 'wb') as output_file:
            writer = csv.writer(output_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(data_type_map.keys())
            writer.writerow(semantic_type_map.values())
            writer.writerows(zip(*data_type_map.values()))

    os.chdir("../..")


def convert_weapons_to_std(folder_path):
    os.chdir(folder_path)

    for file_path in os.listdir("data"):

        semantic_type_map = OrderedDict()

        with open(os.path.join("model", "%s.json" % os.path.splitext(file_path)[0]), 'r') as f:
            data = json.load(f)
            node_array = data["graph"]["nodes"]

            for node in node_array:
                if "userSemanticTypes" in node:
                    semantic_object = node["userSemanticTypes"]
                    name = str(node["columnName"]).translate(None, digits)
                    semantic_type_map[name] = get_semantic_type(semantic_object[0])

        obj_list = []

        with open(os.path.join("data", file_path)) as jl_file:
            for line in jl_file.readlines():
                obj = json.loads(line)
                obj = {str(key).translate(None, digits): obj[key] for key in obj}
                obj_list.append({key: obj[key] for key in semantic_type_map if key in obj})

        obj_list.insert(0, semantic_type_map)

        name = os.path.splitext(file_path)[0]

        with open('../../data/weapon/%s.csv' % name, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, semantic_type_map.keys(), delimiter=",", quotechar='"',
                                         quoting=csv.QUOTE_ALL)
            dict_writer.writeheader()
            dict_writer.writerows(obj_list)

    os.chdir("../..")


def convert_memex_to_std(folder_path):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "r") as f:
            type = os.path.splitext(file_name)[0]
            if os.path.splitext(file_name)[1] == ".json":
                obj_list = json.load(f)
            else:
                obj_list = []
                for line in f.readlines():
                    obj_list.append(line)
            obj_list = [{type: x.strip()} for x in obj_list]
            obj_list.insert(0, {type: type})
        with open("data/memex/%s.csv" % os.path.splitext(file_name)[0], "wb") as output_file:
            dict_writer = csv.DictWriter(output_file, [type], delimiter=",", quotechar='"',
                                         quoting=csv.QUOTE_ALL)
            dict_writer.writeheader()
            dict_writer.writerows(obj_list)


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    # convert_museum_to_std("raw_data/museum")
    # convert_soccer_to_std("raw_data/soccer")
    # convert_others_to_std("raw_data/city", "city")
    # convert_others_to_std("raw_data/weather_old", "weather_old")
    # convert_weapons_to_std("raw_data/weapons")
    convert_memex_to_std("raw_data/memex")
