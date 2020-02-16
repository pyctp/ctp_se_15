# dd = {
#     "name": "18D_Block",
#     "xcc": {
#         "component": {
#             "core": [],
#             "platform": []
#         },
#     },
#     "uefi": {
#         "component": {
#             "core": [],
#             "platform": []
#         },
#     }
# }


class Dict(dict):
    # # self.属性写入 等价于调用dict.__setitem__
    __setattr__ = dict.__setitem__
    # # self.属性读取 等价于调用dict.__setitem__
    __getattribute__ = dict.__getitem__

    # # 等价于__setattr__ = dict.__setitem__
    # def __setattr__(self, key, value):
    #     dict.__setitem__(self, key, value)
    #
    # # 等价于__getattribute__ = dict.__getitem__ 或 __getattr__ = dict.__getitem__
    # def __getattribute__(self, item):
    #     return dict.__getitem__(self, item)


# 递归把dict转换成obj对象【兼容obj.属性和obj[属性]】
def dict_to_object(dictObj):
    if not isinstance(dictObj, dict):
        return dictObj
    inst = Dict()
    for k, v in dictObj.items():
        inst[k] = dict_to_object(v)
    return inst

#
# obj = dict_to_object(dd)
#
# print(obj["name"])
# print(obj.name)

# ————————————————
# 版权声明：本文为CSDN博主「比特币爱好者007」的原创文章，遵循
# CC
# 4.0
# BY - SA
# 版权协议，转载请附上原文出处链接及本声明。
# 原文链接：https: // blog.csdn.net / weixin_43343144 / article / details / 92764884