simple_plugin(FragmentNtuple "dataset" 
hep_hpc_hdf5
  ${HDF5_C_LIBRARIES}
 fhiclcpp
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
)

simple_plugin(ToyFragmentNtuple "dataset"
hep_hpc_hdf5
 ${HDF5_C_LIBRARIES}
 fhiclcpp
 cetlib
 cetlib_except
 artdaq-demo-hdf5_HDF5
 artdaq-demo-hdf5_HDF5_hep-hpc_FragmentNtuple_dataset
)