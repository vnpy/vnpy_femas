import platform

from setuptools import Extension, setup


def get_ext_modules() -> list:
    """
    获取三方模块
    Windows需要编译封装接口
    Linux和Mac由于缺乏二进制库支持无法使用
    """
    if platform.system() != "Windows":
        return

    extra_compile_flags = ["-O2", "-MT"]
    extra_link_args = []
    runtime_library_dirs = []

    vnfemasmd = Extension(
        "vnpy_femas.api.vnfemasmd",
        [
            "vnpy_femas/api/vnfemas/vnfemasmd/vnfemasmd.cpp",
        ],
        include_dirs=["vnpy_femas/api/include",
                      "vnpy_femas/api/vnfemas"],
        define_macros=[],
        undef_macros=[],
        library_dirs=["vnpy_femas/api/libs", "vnpy_femas/api"],
        libraries=["USTPmduserapiAF", "USTPtraderapiAF"],
        extra_compile_args=extra_compile_flags,
        extra_link_args=extra_link_args,
        runtime_library_dirs=runtime_library_dirs,
        depends=[],
        language="cpp",
    )

    vnfemastd = Extension(
        "vnpy_femas.api.vnfemastd",
        [
            "vnpy_femas/api/vnfemas/vnfemastd/vnfemastd.cpp",
        ],
        include_dirs=["vnpy_femas/api/include",
                      "vnpy_femas/api/vnfemas"],
        define_macros=[],
        undef_macros=[],
        library_dirs=["vnpy_femas/api/libs", "vnpy_femas/api"],
        libraries=["USTPmduserapiAF", "USTPtraderapiAF"],
        extra_compile_args=extra_compile_flags,
        extra_link_args=extra_link_args,
        runtime_library_dirs=runtime_library_dirs,
        depends=[],
        language="cpp",
    )

    return [vnfemastd, vnfemasmd]


setup(
    ext_modules=get_ext_modules(),
)
