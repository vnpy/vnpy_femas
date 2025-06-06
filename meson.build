project(
  'vnpy_femas',
  'cpp',
  version: '1.0.3',
  license: 'MIT',
  meson_version: '>=1.7.0',
  default_options: [
    'buildtype=release',
    'cpp_std=c++17',
    'warning_level=2',
  ],
)

# 导入文件系统和Python模块
fs = import('fs')
py = import('python').find_installation(pure: false)
py_dep = py.dependency()

# 获取pybind11路径
python_cmd = host_machine.system() == 'windows' ? 'python' : 'python3'
pybind11_include_dir = run_command(python_cmd, '-c', 'import pybind11; print(pybind11.get_include())', check: true).stdout().strip()
message('使用pybind11路径: ' + pybind11_include_dir)

# 获取编译器信息
cpp = meson.get_compiler('cpp')
compiler_id = cpp.get_id()

# 输出构建目标系统信息
message('构建目标系统: ' + host_machine.system())

# 初始化变量
thostmduserapi_lib = []
thosttraderapi_lib = []
extra_cpp_args = []
extra_link_args = []

# 初始化库变量
thostmduserapi_lib = []
thosttraderapi_lib = []

# 初始化其他编译选项
extra_link_args = []
install_rpath_value = ''

# 设置Windows特定编译选项
# Windows编译器设置
add_project_arguments('/MT', language : 'cpp')

# 设置库目录
lib_dir = meson.current_source_dir() / 'vnpy_femas/api/libs'
api_dir = meson.current_source_dir() / 'vnpy_femas/api'

# 设置include目录
include_dirs = include_directories(
  'vnpy_femas/api/include',
  'vnpy_femas/api/vnfemas',
  pybind11_include_dir,
)

# 定义CTP库
thostmduserapi_lib = cpp.find_library('USTPmduserapiAF', 
                                     dirs: [lib_dir, api_dir],
                                     required: true)
thosttraderapi_lib = cpp.find_library('USTPtraderapiAF', 
                                     dirs: [lib_dir, api_dir],
                                     required: true)

# 创建MD模块扩展
md_module = py.extension_module(
  'vnfemasmd',
  sources: ['vnpy_femas/api/vnfemas/vnfemasmd/vnfemasmd.cpp'],
  include_directories: include_dirs,
  dependencies: [py_dep, thostmduserapi_lib],
  cpp_args: extra_cpp_args,
  link_args: extra_link_args,
  install: true,
  subdir: 'vnpy_femas/api'
)

# 创建TD模块扩展
td_module = py.extension_module(
  'vnfemastd',
  sources: ['vnpy_femas/api/vnfemas/vnfemastd/vnfemastd.cpp'],
  include_directories: include_dirs,
  dependencies: [py_dep, thosttraderapi_lib],
  cpp_args: extra_cpp_args,
  link_args: extra_link_args,
  install: true,
  subdir: 'vnpy_femas/api'
)

# 安装Python源代码
python_files = [
  ['vnpy_femas/__init__.py', 'vnpy_femas'],
  ['vnpy_femas/api/__init__.py', 'vnpy_femas/api'],
  ['vnpy_femas/api/femas_constant.py', 'vnpy_femas/api'],
  ['vnpy_femas/gateway/__init__.py', 'vnpy_femas/gateway'],
  ['vnpy_femas/gateway/femas_gateway.py', 'vnpy_femas/gateway'],
]

foreach file : python_files
  py.install_sources(
    [file[0]],
    pure: false,
    subdir: file[1]
  )
endforeach

# 安装API动态库文件
api_libs = [
  'vnpy_femas/api/USTPmduserapiAF.dll',
  'vnpy_femas/api/USTPtraderapiAF.dll',
]

install_data(
  api_libs,
  install_dir: py.get_install_dir() / 'vnpy_femas/api'
)
