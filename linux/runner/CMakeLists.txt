cmake_minimum_required(VERSION 3.13)
project(runner LANGUAGES C CXX)

# Define the application target. To change its name, change BINARY_NAME in the
# top-level CMakeLists.txt, not the value here, or `flutter run` will no longer
# work.
#
# Any new source files that you add to the application should be added here.
add_executable(${BINARY_NAME}
  "main.cc"
  "my_application.cc"
  "${FLUTTER_MANAGED_DIR}/generated_plugin_registrant.cc"
)

set(SPH2PIPE_DIR "${CMAKE_SOURCE_DIR}/../vendor/sph2pipe")
set(SPH2PIPE_SOURCES
  "${SPH2PIPE_DIR}/shorten_x.c"
  "${SPH2PIPE_DIR}/sph2pipe_shorten_decode.c"
)
target_sources(${BINARY_NAME} PRIVATE ${SPH2PIPE_SOURCES})
target_include_directories(${BINARY_NAME} PRIVATE "${SPH2PIPE_DIR}")
set_source_files_properties(${SPH2PIPE_SOURCES} PROPERTIES COMPILE_FLAGS "-std=gnu89 -w")

# Apply the standard set of build settings. This can be removed for applications
# that need different build settings.
apply_standard_settings(${BINARY_NAME})

# Add preprocessor definitions for the application ID.
add_definitions(-DAPPLICATION_ID="${APPLICATION_ID}")

# Add dependency libraries. Add any application-specific dependencies here.
target_link_libraries(${BINARY_NAME} PRIVATE flutter)
target_link_libraries(${BINARY_NAME} PRIVATE PkgConfig::GTK)
target_link_options(${BINARY_NAME} PRIVATE "-Wl,--export-dynamic")

target_include_directories(${BINARY_NAME} PRIVATE "${CMAKE_SOURCE_DIR}")
