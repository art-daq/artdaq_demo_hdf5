cet_register_export_set(SET_NAME PluginTypes NAMESPACE artdaq_plugin_types)

cet_make(
  LIBRARIES
  artdaq_core::artdaq-core_Data
  artdaq_core::artdaq-core_Utilities
)


cet_make_library(LIBRARY_NAME dataset INTERFACE
				 EXPORT_SET PluginTypes
				 SOURCE FragmentDataset.hh
				 LIBRARIES INTERFACE
				 art_plugin_support::plugin_config_macros
				 art_plugin_support::support_macros
				 artdaq_demo_hdf5::artdaq-demo-hdf5_HDF5
				 ${HDF5_C_LIBRARIES})

include(BasicPlugin)
foreach(type IN ITEMS dataset)
  cet_make_plugin_builder(${type} ${type} Modules
    LIBRARIES REG artdaq_plugin_types::${type})
endforeach()

cet_collect_plugin_builders(Modules DatasetPlugins)
include(DatasetPlugins)

# 03/23/2020, ELF: Removing the hep-hpc builds, they are write-only and based on a dev product.
#add_subdirectory(hep-hpc)

set(USE_HIGHFIVE ON)
if(${USE_HIGHFIVE}) 
    add_subdirectory(highFive)
endif()

set(USE_H5CPP OFF)
if(${USE_H5CPP})
    add_subdirectory(h5cpp)
endif()
  

install_headers()
install_source()
