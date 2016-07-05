from pyspark import SparkContext
from utils.similarity_tests import ks_distribution_sim, mw_histogram_sim, jaccard_num_sim, jaccard_str_sim, \
    jaccard_name_sim, jaccard_text_sim

sc = SparkContext()

debug_writer = open('debug.txt', 'w')

KS_NUM = "ks_num"
MW_HIST = "mw_hist"
JC_NUM = "jc_num"
JC_TEXT = "jc_text"
JC_NAME = "jc_name"
TF_TEXT = "tf_text"
JC_FULL_TEXT = "jc_full_text"

similarity_test_map = {KS_NUM: ks_distribution_sim, MW_HIST: mw_histogram_sim, JC_NUM: jaccard_num_sim,
                       JC_TEXT: jaccard_str_sim, JC_NAME: jaccard_name_sim, JC_FULL_TEXT: jaccard_text_sim}

DEBUG = False


features = [KS_NUM, JC_NUM, JC_TEXT, JC_NAME, JC_FULL_TEXT]