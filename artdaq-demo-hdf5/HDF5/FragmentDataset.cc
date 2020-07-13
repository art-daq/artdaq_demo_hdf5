#include "tracemf.h"
#define TRACE_NAME "FragmentDataset"

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

artdaq::hdf5::FragmentDataset::FragmentDataset(fhicl::ParameterSet const& /*unused*/, const std::string& mode)
{
	TLOG(TLVL_DEBUG) << "FragmentDataset CONSTRUCTOR Begin";
	if (mode.find("rite") != std::string::npos || mode == "1")
	{
		mode_ = FragmentDatasetMode::Write;
	}
	else
	{
		mode_ = FragmentDatasetMode::Read;
	}

	TLOG(TLVL_DEBUG) << "FragmentDataset CONSTRUCTOR End";
}