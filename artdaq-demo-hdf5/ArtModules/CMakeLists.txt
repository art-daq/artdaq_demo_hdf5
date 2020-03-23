
simple_plugin(HDFFileOutput "module"
  artdaq-demo-hdf5_HDF5
  artdaq_DAQdata
  art_Framework_IO
)

simple_plugin(HDFFileInput "source"
  artdaq-demo-hdf5_HDF5
  artdaq_DAQdata
  artdaq-utilities_Plugins  
  art_Framework_Services_Registry
)

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
