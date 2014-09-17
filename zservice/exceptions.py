#coding=utf8


class SerivceNotFound(Exception):
    """服务不存在"""
    pass

class ServiceNotMethod(Exception):
    """服务中没有该方法"""
    pass


class ServiceException(Exception):
    """服务执行异常"""
    pass


class InvalidUri(Exception):
    """无效的URI地址"""
    pass


class LimitExceeded(Exception):
    """超过限制"""
    pass

class SerializationError(Exception):
    """序列化错误"""


class EncodeError(Exception):
    """序列化编码错误"""
    pass


class DecodeError(Exception):
    """序列化解码错误"""

class SerializerNotInstalled(Exception):
    """还没安装该序列化类型"""
    pass    