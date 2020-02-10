
simple_plugin(highFiveNtupleDataset "dataset" 
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq-core_Data
 artdaq_DAQdata
)
