
#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

artdaq::hdf5::FragmentDataset::FragmentDataset(fhicl::ParameterSet const& ps, std::string mode)
    : nWordsPerRow_(ps.get<size_t>("nWordsPerRow", 10240))
{
	if (mode.find("rite") != std::string::npos || mode == "1")
		mode_ = FragmentDatasetMode::Write;
	else
		mode_ = FragmentDatasetMode::Read;
}