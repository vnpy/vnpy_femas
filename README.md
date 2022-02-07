# vn.py框架的飞马柜台交易接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于FEMAS期货柜台1.0.0版本开发的交易接口。

## 安装

安装需要基于2.7.0版本以上的[VN Studio](https://www.vnpy.com)。

直接使用pip命令：

```
pip install vnpy_femas
```

下载解压后在cmd中运行

```
python setup.py install
```

由于在安装的同时需要编译C++代码，因此在执行上述命令之前需要去微软[官网](https://my.visualstudio.com/Downloads?q=build%20tools)下载Visual Studio Build Tools。其中工作负荷选择Visual C++生成工具，同时推荐下载2017版。
