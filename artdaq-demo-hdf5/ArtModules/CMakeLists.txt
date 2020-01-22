

simple_plugin(ToyHDFFileOutput "module"
  artdaq_ArtModules
  art_Framework_IO
  art_Framework_Services_Registry
  art_Utilities
  MF_MessageLogger
  fhiclcpp
  cetlib
  ${CETLIB_EXCEPT}
  ${ROOT_RIO}
  hep_hpc_hdf5
  ${HDF5_C_LIBRARIES}
)

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
