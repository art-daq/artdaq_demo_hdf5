# ======================================================================
#  artdaq-demo-hdf5 main build file
#
#  cd .../path/to/build/directory
#  source .../path/to/artdaq-demo-hdf5/ups/setup_for_development
#  buildtool
# ======================================================================


cmake_minimum_required (VERSION 3.19 FATAL_ERROR)

find_package(cetmodules 3.08.00 REQUIRED)

project(artdaq_demo_hdf5 VERSION 1.04.01)

include(CetCMakeEnv)
cet_cmake_env()

# Find our own CMake modules, and tell everyone else where to find them.
cet_cmake_module_directories(Modules BINARY)

cet_set_compiler_flags(DIAGS VIGILANT
 WERROR
 NO_UNDEFINED
 EXTRA_FLAGS -pedantic
 )
 
# Debug streamer.
string(TOUPPER ${CMAKE_BUILD_TYPE} BTYPE_UC)
if(BTYPE_UC STREQUAL DEBUG)
  add_definitions(-DDEBUGME)
	if(DEFINED ENV{USE_ASAN})
		if($ENV{USE_ASAN} GREATER 0)
			message("!!! Address Sanitizer Activated!!!")
			add_compile_options(-fsanitize=address -Wno-unused-command-line-argument)
			add_link_options(-lasan)
		endif()
	endif()
	if(DEFINED ENV{USE_TSAN})
		if($ENV{USE_TSAN} GREATER 0)
			message("!!! Thread Sanitizer Activated!!!")
			add_compile_options(-fsanitize=thread -Wno-unused-command-line-argument)
			add_link_options(-ltsan)
		endif()
	endif()
	if(DEFINED ENV{USE_UBSAN})
		if($ENV{USE_UBSAN} GREATER 0)
			message("!!! Undefined Sanitizer Activated!!!")
			add_compile_options(-fsanitize=undefined -Wno-unused-command-line-argument)
			add_link_options(-lubsan)
		endif()
	endif()
	if(DEFINED ENV{USE_GCOV})
		add_compile_options(-fprofile-arcs -ftest-coverage --coverage)
		add_link_options(--coverage)
	endif()
endif()

#cet_report_compiler_flags()

find_package(art 3.09.03 REQUIRED)
find_package(artdaq_utilities 1.07.02 REQUIRED)
find_package(artdaq_core 3.09.00 REQUIRED)
find_package(artdaq 3.12.00 REQUIRED)
find_package( hdf5 1.12.0 REQUIRED )
 
cet_register_export_set(SET_NAME artdaq_demo_hdf5 NAMESPACE artdaq_demo_hdf5)

if( ${BTYPE_UC} MATCHES "DEBUG" )
set(HDF5_C_LIBRARIES hdf5_hl_debug hdf5_debug)
else()
set(HDF5_C_LIBRARIES hdf5_hl hdf5)
endif()
include_directories($ENV{HDF5_INC})
link_directories($ENV{HDF5_LIB})

# source
add_subdirectory(artdaq-demo-hdf5)

# testing
#add_subdirectory(test)

# tools
#add_subdirectory(tools)

# doc - Documentation
add_subdirectory(doc)

cet_cmake_config()

