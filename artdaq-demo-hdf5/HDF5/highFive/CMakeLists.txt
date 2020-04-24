
simple_plugin(highFiveNtupleDataset "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq-core_Data
 artdaq_DAQdata
)

simple_plugin(highFiveGroupedDataset "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 canvas
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq-core_Data
 artdaq_DAQdata
  art_Framework_Services_Registry
  art_Framework_IO
  art_Utilities
)

simple_plugin(highFiveGeoCmpltPDSPSample "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 canvas
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq-core_Data
 artdaq_DAQdata
  art_Framework_Services_Registry
  art_Framework_IO
  art_Utilities
)

simple_plugin(highFiveGeoSplitPDSPSample "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 canvas
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq-core_Data
 artdaq_DAQdata
  art_Framework_Services_Registry
  art_Framework_IO
  art_Utilities
)

simple_plugin(highFiveTimeBasedPDSPSample "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 canvas
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq-core_Data
 artdaq_DAQdata
  art_Framework_Services_Registry
  art_Framework_IO
  art_Utilities
)
