# Copyright © 2020 BangxinIot Technology Co.,Ltd. All rights reserved.
# None of the materials provided in this project (lib) may be reproduced or transmitted in whole or in part,
# in any form or by any means, electronic or mechanical, including photocopying, recording, or the use of
# any information storage and retrieval system, except as provided in the Terms and Conditions of Contract
# or agreement from BangxinIot. It is forbidden to use our technology for patent or software copyright 
# applications, except as permitted by the agreement. For permissions or further enquiries, visit:
# https://mathearth.com

'''
This UDF clamps input integers to the range [0, 2^32 - 1].
version: 1.0
Date: 2024-08-12
'''

# 最大编码值
MAX_ENCODING_VALUE = 2 ** 32 - 1

# 初始化和销毁函数
def init():
    pass


def destroy():
    pass


# 数据处理函数
#
def process(block):
    rows, _ = block.shape()
    results = []
    for i in range(rows):
        input_number = int(block.data(i, 0))
        if input_number < 0:
            result = 0
        elif input_number > MAX_ENCODING_VALUE:
            result = MAX_ENCODING_VALUE
        else:
            result = input_number
        results.append(result)
    return results