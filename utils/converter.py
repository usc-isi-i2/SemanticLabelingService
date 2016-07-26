import csv
import json
import os
from collections import OrderedDict
import sys
from itertools import groupby
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
            print file_path
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
                    if attrib_name in semantic_type_map:
                        obj[attrib_name] = child.attrib[attrib_name]
                for attrib in child:
                    if attrib.tag in semantic_type_map:
                        obj[attrib.tag] = attrib.text
                obj_list.append(obj)
            obj_list.insert(0, semantic_type_map)

        elif file_path.endswith("json"):
            with open(os.path.join("data", file_path), 'r') as f:
                json_array = json.load(f)
                obj_list = []
                for node in json_array:
                    obj = {}
                    for field in node.keys():
                        if isinstance(node[field], dict):
                            for field1 in node[field].keys():
                                if field1 in semantic_type_map:
                                    obj[field1] = node[field][field1]
                        else:
                            if field in semantic_type_map:
                                if isinstance(node[field], list):
                                    obj[field] = " ".join([str(value) for value in node[field]])
                                else:
                                    obj[field] = str(node[field])
                    obj_list.append(obj)
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

        with open('../../data/museum2/%s.csv' % name, 'wb') as output_file:
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

        with open('../../data/soccer2/%s.csv' % name, 'wb') as output_file:
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

        max_len = 0
        for key in data_type_map:
            if len(data_type_map[key]) > max_len:
                max_len = len(data_type_map[key])

        for key in data_type_map:
            data_type_map[key] = data_type_map[key] + [""] * (max_len - len(data_type_map[key]))

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


def convert_escort_to_std(folder_path):
    os.chdir(folder_path)

    for file_path in os.listdir("data"):

        obj_list = []

        attribute_list = []

        with open(os.path.join("data", file_path)) as jl_file:
            for line in jl_file.readlines():
                obj = json.loads(line)
                attribute_list.extend(obj.keys())
                obj = {str(key).translate(None, digits): obj[key] for key in obj.keys()}
                obj_list.append(obj)

        name = os.path.splitext(file_path)[0]
        obj_list.insert(0, {str(key).translate(None, digits): key for key in set(attribute_list)})

        with open('../../data/test-memex/%s.csv' % name, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, obj_list[0].keys(), delimiter=",", quotechar='"',
                                         quoting=csv.QUOTE_ALL)
            dict_writer.writeheader()
            dict_writer.writerows(obj_list)

    os.chdir("../..")


def convert_weather_to_std(folder_path):
    os.chdir(folder_path)

    for file_path in os.listdir("data"):
        with open(os.path.join("data", file_path), 'r') as f:
            obj_list = []

            for line in f.readlines():
                node = json.loads(line)
                obj = {}
                for field in node.keys():
                    if isinstance(node[field], dict):
                        for field1 in node[field].keys():
                            obj[field1] = node[field][field1]
                    else:
                        if isinstance(node[field], list):
                            obj[field] = " ".join([str(value) for value in node[field]])
                        else:
                            obj[field] = str(node[field])
                obj_list.append(obj)
            obj_list.insert(0, {key: key for key in obj_list[0].keys()})

        data_list = obj_list[1:2001]
        data_list.sort(key=lambda x: str(x["lon"]) + "!" + str(x["lat"]))

        keys = [x for x in obj_list[0].keys() if x not in ["grnd_level", "sea_level", "sunrise"]]

        for i in range(0, 2000, 40):
            with open('../../data/weather2/%d.csv' % (i / 40), 'wb') as output_file:
                dict_writer = csv.DictWriter(output_file, keys, delimiter=",", quotechar='"',
                                             quoting=csv.QUOTE_ALL, extrasaction='ignore')
                dict_writer.writeheader()
                dict_writer.writerow(obj_list[0])
                for obj in data_list[i:i + 40]:
                    dict_writer.writerow(obj)


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    # convert_museum_to_std("raw_data/museum2")
    # convert_soccer_to_std("raw_data/soccer2")
    # convert_others_to_std("raw_data/city2", "city2")
    # convert_others_to_std("raw_data/weather_old2", "weather_old2")
    # convert_weapons_to_std("raw_data/weapons")
    # convert_memex_to_std("raw_data/memex")
    # convert_escort_to_std("raw_data/test-memex")
    convert_weather_to_std("raw_data/weather_new")
