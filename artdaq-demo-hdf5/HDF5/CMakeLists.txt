cet_make_library(
  LIBRARY_NAME artdaq-demo-hdf5_HDF5
  SOURCE
  FragmentDataset.cc
  MakeDatasetPlugin.cc
  LIBRARIES
  fhiclcpp
  ${CETLIB}
  ${CETLIB_EXCEPT}
  artdaq_core::artdaq-core_Data
  artdaq_core::artdaq-core_Utilities
  ${MF_MESSAGELOGGER}
)

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
