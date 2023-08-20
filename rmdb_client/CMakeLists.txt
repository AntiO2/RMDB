cmake_minimum_required(VERSION 3.16)
project(rmdb_client)

add_subdirectory(generate_data)
add_subdirectory(src)
set(CMAKE_CXX_STANDARD 20)
find_package(Threads REQUIRED)

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(rmdb_client
        pthread readline
        )
## 测试部分
add_custom_target(hello
        COMMAND echo "Hello, world!")

set(RMDB_TEST_SQL_DIR ${PROJECT_SOURCE_DIR}/sql)
file(GLOB_RECURSE RMDB_TEST_SQL_SOURCES "${RMDB_TEST_SQL_DIR}/*.sql")


foreach(RMDB_TEST_SQL_SOURCE ${RMDB_TEST_SQL_SOURCES})
    get_filename_component(rmdb_sql_filename ${RMDB_TEST_SQL_SOURCE} NAME) # 获得测试的文件名
    string(REPLACE ".sql" "" rmdb_sql_name ${rmdb_sql_filename}) # 去除后缀
    set(rmdb_sql_name ${rmdb_sql_name}_test) # 加上_test后缀


    add_custom_target(${rmdb_sql_name}
            COMMAND echo "launch rmdb client add load sql file ${rmdb_sql_filename}"
            COMMAND ./rmdb_client <  ${RMDB_TEST_SQL_DIR}/${rmdb_sql_filename}
            EXCLUDE_FROM_ALL true
            DEPENDS rmdb_client)

    message("create test target: ${rmdb_sql_name}")
endforeach()

# message("runtime output dir:-> ${CMAKE_BINARY_DIR}")