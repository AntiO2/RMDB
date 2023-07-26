add_executable(concurrency_test_main concurrency_test_main.cpp
        concurrency_test.cpp
        regress_test.cpp)
file(GLOB_RECURSE CON_TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*_test.sql")
foreach(CON_TEST ${CON_TEST_SOURCES})
    get_filename_component(con_test_filename ${CON_TEST} NAME)
    string(REPLACE ".sql" "" con_test_name ${con_test_filename})
    message("detect test: ${con_test_name} cp ${CMAKE_CURRENT_SOURCE_DIR}/${con_test_filename} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${con_test_filename}")
    add_custom_target(${con_test_name}
            COMMAND
             cp ${CMAKE_CURRENT_SOURCE_DIR}/${con_test_filename} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${con_test_filename})
    add_dependencies(concurrency_test_main
            ${con_test_name})
endforeach()