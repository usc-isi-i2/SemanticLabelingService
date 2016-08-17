from numpy import linalg
from scipy.spatial.distance import euclidean
from scipy.stats import ks_2samp, mannwhitneyu

from utils.helpers import adjust_result


def ks_distribution_sim(list1, list2, num_fraction1, num_fraction2):
    if len(list1) > 0 and len(list2) > 0:
        return adjust_result(num_fraction1, num_fraction2, ks_2samp(list1, list2)[1])
    return 0


def mw_histogram_sim(list1, list2, num_fraction1, num_fraction2):
    if len(list1) > 0 and len(list2) > 0 and len(set(list1)) > 1 and len(set(list2)) > 1:
        return mannwhitneyu(list1, list2)[1]
    return 0


def jaccard_name_sim(str1, str2, num_fraction1, num_fraction2):
    tokens1 = [str1[i:i + 2] for i in range(len(str1.lower()))]
    tokens2 = [str2[i:i + 2] for i in range(len(str2.lower()))]
    return jaccard_similarity(tokens1, tokens2)


def jaccard_str_sim(list1, list2, num_fraction1, num_fraction2):
    if len(list1) > 0 and len(list2) > 0:
        return adjust_result(1 - num_fraction1, 1 - num_fraction2, jaccard_similarity(list1, list2))
    return 0


def jaccard_num_sim(list1, list2, num_fraction1, num_fraction2):
    if len(list1) > 0 and len(list2) > 0:
        # max1 = percentile(list1, 75)
        # min1 = percentile(list1, 25)
        # max2 = percentile(list2, 75)
        # min2 = percentile(list2, 25)
        # max1 = percentile(list1, 90)
        # min1 = percentile(list1, 10)
        # max2 = percentile(list2, 90)
        # min2 = percentile(list2, 10)
        max1 = max(list1)
        min1 = min(list1)
        max2 = max(list2)
        min2 = min(list2)
        max3 = max(max1, max2)
        min3 = min(min1, min2)
        if min2 > max1 or min1 > max2:
            return 0
        elif max3 == min3:
            return 0
        else:
            min4 = min(max1, max2)
            max4 = max(min1, min2)
            result = (min4 - max4) * 1.0 / (max3 - min3)
            return adjust_result(num_fraction1, num_fraction2, result)
    return 0


def jaccard_text_sim(list1, list2, num_fraction1, num_fraction2):
    if len(list1) > 0 and len(list2) > 0:
        return jaccard_similarity(list1, list2)
    return 0


def jaccard_similarity(x, y):
    if not x and not y:
        return 0
    intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
    union_cardinality = len(set.union(*[set(x), set(y)]))
    return intersection_cardinality / float(union_cardinality)
