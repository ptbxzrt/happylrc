# source help.sh

add_to_ld_library_path() {
    for dir in "$@"; do
        # 检查 $LD_LIBRARY_PATH 是否已包含该目录
        if [[ ":$LD_LIBRARY_PATH:" != *":$dir:"* ]]; then
            export LD_LIBRARY_PATH="$dir:$LD_LIBRARY_PATH"
        fi
    done
}

# 要添加的目录列表
directories=("$HOME/happylrc/3rd_party/gf-complete/lib" "$HOME/happylrc/3rd_party/yalantinglibs/lib" "$HOME/happylrc/3rd_party/Jerasure/lib" "$HOME/happylrc/3rd_party/hiredis/lib" "$HOME/happylrc/3rd_party/redis-plus-plus/lib")

# 调用函数来添加路径
add_to_ld_library_path "${directories[@]}"

build_dir="build"

# 检查build文件夹是否存在
if [ -d "$build_dir" ]; then
    echo "build文件夹已经存在,进入该文件夹"
    cd "$build_dir"
else
    echo "build文件夹不存在,创建并进入"
    mkdir "$build_dir"
    cd "$build_dir"
fi

cmake .. -DCMAKE_BUILD_TYPE=Debug

cmake --build . -j8

cd ..
