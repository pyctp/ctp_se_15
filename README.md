pyctp
=====

ctp wrapper for python (期货版)

CTP版本：MdApi().GetApiVersion(), TraderApi().GetApiVersion()

环境：python2.5 ~ python3.4，Windows或者Linux

编译：`python setup.py build`

安装：`python setup.py install`或者复制build下的ctp目录到某个sys.path目录。
# ctp_se_15

linux 64位 python 2.7.16 64位,测试通过。
windows 64位, python 2.7.16 64 位，测试通过。

如果linux下获取信息遇到权限问题，用下面命令：

sudo chmod o+r  /sys/firmware/dmi/tables/smbios_entry_point

sudo chmod o+r /dev/mem

sudo chmod o+r /sys/firmware/dmi/tables/DMI

即可。

api 测试程序 已经移到 test 目录下面。
