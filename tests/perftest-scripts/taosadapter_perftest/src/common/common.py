import sys
sys.path.append("../../")
from config.env_init import *

class Common:
    def __init__(self):
        self.ip_list = list()

    def genTelnetMulTagStr(self, count):
        tag_str = ""
        for i in range(0, count):
            if i < (count-1):
                tag_str += f't{i}={i} '
            else:
                tag_str += f't{i}={i}'
        return tag_str

    def genTelnetLine(self, tag_count):
        tag_str = self.genTelnetMulTagStr(tag_count)
        stb_line = "stb_${stb_counter} " + "1626006833640 " + "32.261068286779754 " + tag_str
        ltag = tag_str.split("=")
        ltag[-1] = "${tb_counter}"
        stag = '='.join(ltag)
        tb_line = "stb_${stb_counter} " + "1626006833640 " + "32.261068286779754 " + stag
        row_line = "stb_${stb_counter} " + "${ts_counter} " + "32.261068286779754 " + stag
        return stb_line, tb_line, row_line

    # def genJmx(self):
    #     if config["taosadapter_separate_deploy"]:
    #         for key in config:
    #             if "taosd_dnode" in str(key):
    #                 # self.ip_list.append(config[key]["ip"])
    #                 shutil.copyfile(self.telnetMixJmxFile, jmxfile)

    #     if 

if __name__ == '__main__':
    com = Common()
    print(com.genTelnetLine(10))