#include "artdaq-demo-hdf5/HDF5/MakeDatasetPlugin.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "cetlib/BasicPluginFactory.h"
#include "fhiclcpp/ParameterSet.h"

#include <sstream>

namespace artdaq {
namespace hdf5 {
std::unique_ptr<artdaq::hdf5::FragmentDataset>
MakeDatasetPlugin(const fhicl::ParameterSet& pset,
                  std::string plugin_label)
{
	static cet::BasicPluginFactory bpf("dataset", "make");

	fhicl::ParameterSet dataset_pset;

	try
	{
		dataset_pset = pset.get<fhicl::ParameterSet>(plugin_label);
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg
		    << "Error in artdaq::MakeDatasetPlugin: Unable to find the Dataset plugin parameters in the FHiCL code \"" << dataset_pset.to_string()
		    << "\"; FHiCL table with label \"" << plugin_label
		    << "\" may not exist, or if it does, one or more parameters may be missing.";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	try
	{
		auto Dataset =
		    bpf.makePlugin<std::unique_ptr<FragmentDataset>,
		                   const fhicl::ParameterSet&>(
		        dataset_pset.get<std::string>("datasetPluginType"),
		        dataset_pset);

		return Dataset;
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg
		    << "Unable to create Dataset plugin using the FHiCL parameters \""
		    << dataset_pset.to_string()
		    << "\"";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return nullptr;
}
}  // namespace hdf5
}  // namespace artdaq
