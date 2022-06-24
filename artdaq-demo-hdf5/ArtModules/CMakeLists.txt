
include(BuildPlugins) # Art Module and Source

cet_build_plugin(HDFFileOutput art::module LIBRARIES REG
  artdaq-demo-hdf5_HDF5
  art::Framework_IO
)

cet_build_plugin(HDFFileInput art::source LIBRARIES REG
  artdaq-demo-hdf5_HDF5
  artdaq::DAQdata
)

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
