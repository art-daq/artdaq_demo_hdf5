
if( NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/HighFive )
    message("Cloning HighFive into ${CMAKE_CURRENT_BINARY_DIR}/HighFive")
    execute_process(COMMAND git clone --branch v2.2.1 https://github.com/BlueBrain/HighFive.git WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    include_directories(${CMAKE_CURRENT_BINARY_DIR}/HighFive)
endif()

basic_plugin(highFiveNtupleDataset "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq_core::artdaq-core_Data
 artdaq_DAQdata
)

basic_plugin(highFiveGroupedDataset "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq_core::artdaq-core_Data
 artdaq_DAQdata
)

basic_plugin(highFiveGeoCmpltPDSPSample "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq_core::artdaq-core_Data
 artdaq_DAQdata
)

basic_plugin(highFiveGeoSplitPDSPSample "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq_core::artdaq-core_Data
 artdaq_DAQdata
)

basic_plugin(highFiveTimeBasedPDSPSample "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq_core::artdaq-core_Data
 artdaq_DAQdata
)
