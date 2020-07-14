#include "tracemf.h"
#define TRACE_NAME "FragmentDataset"

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

artdaq::hdf5::FragmentDataset::FragmentDataset(fhicl::ParameterSet const& ps, const std::string& mode)
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

	auto unidentified_instance_name = ps.get<std::string>("unidentified_instance_name", "unidentified");
	auto extraTypes = ps.get<std::vector<std::pair<artdaq::Fragment::type_t, std::string>>>("fragment_type_map", std::vector<std::pair<artdaq::Fragment::type_t, std::string>>());
	auto fragmentNameHelperPluginType = ps.get<std::string>("helper_plugin", "ArtdaqFragmentNameHelper");

	nameHelper_ = artdaq::makeNameHelper(fragmentNameHelperPluginType, unidentified_instance_name, extraTypes);

	TLOG(TLVL_DEBUG) << "FragmentDataset CONSTRUCTOR End";
}