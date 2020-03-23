include(artdaq_doxygen)

create_doxygen_documentation(
EXCLUDE
artdaq-demo-hdf5/HDF5/highFive/HighFive
DEPENDS artdaq_demo artdaq artdaq_core_demo artdaq_core artdaq_utilities)
