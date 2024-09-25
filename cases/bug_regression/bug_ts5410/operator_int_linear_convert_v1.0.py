# Copyright © 2020 BangxinIot Technology Co.,Ltd. All rights reserved.
# None of the materials provided in this project (lib) may be reproduced or transmitted in whole or in part,
# in any form or by any means, electronic or mechanical, including photocopying, recording, or the use of
# any information storage and retrieval system, except as provided in the Terms and Conditions of Contract
# or agreement from BangxinIot. It is forbidden to use our technology for patent or software copyright 
# applications, except as permitted by the agreement. For permissions or further enquiries, visit:
# https://mathearth.com

'''
This UDF converts an integer to another integer using a linear function.
version: 1.0
Date: 2024-08-12
'''


# 初始化和销毁函数
def init():
    pass


def destroy():
    pass

# 数据处理函数
# 编码范围：0-(2**32 -1)
def process(block):
    rows, _ = block.shape()
    results = []
    for i in range(rows):
        input_number = int(block.data(i, 0))
        parameter_a = float(block.data(i, 1))
        parameter_b = float(block.data(i, 2))
        result = int(parameter_a * input_number + parameter_b)
        results.append(result)
    return results