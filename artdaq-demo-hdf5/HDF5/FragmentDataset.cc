
#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

artdaq::hdf5::FragmentDataset::FragmentDataset(fhicl::ParameterSet const& , std::string mode)
{
	if (mode.find("rite") != std::string::npos || mode == "1")
		mode_ = FragmentDatasetMode::Write;
	else
		mode_ = FragmentDatasetMode::Read;
}