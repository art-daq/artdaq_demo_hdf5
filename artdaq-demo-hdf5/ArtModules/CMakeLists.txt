
simple_plugin(HDFFileOutput "module"
  artdaq_ArtModules
  art_Framework_IO
  art_Framework_Services_Registry
  art_Utilities
  MF_MessageLogger
  fhiclcpp
  cetlib
  ${CETLIB_EXCEPT}
  ${ROOT_RIO}
  artdaq-demo-hdf5_HDF5
)

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
