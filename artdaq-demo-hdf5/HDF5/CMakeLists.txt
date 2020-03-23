art_make(BASENAME_ONLY
  LIBRARY_NAME artdaq-demo-hdf5_HDF5
  NO_PLUGINS
  LIB_LIBRARIES
  fhiclcpp
  ${CETLIB}
  ${CETLIB_EXCEPT}
  artdaq-core_Utilities
  ${MF_MESSAGELOGGER}
)

add_subdirectory(hep-hpc)

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
