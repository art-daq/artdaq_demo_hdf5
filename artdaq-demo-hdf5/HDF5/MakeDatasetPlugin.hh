#ifndef artdaq_TransferPlugins_MakeTransferPlugin_hh
#define artdaq_TransferPlugins_MakeTransferPlugin_hh

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

#include "fhiclcpp/fwd.h"

#include <memory>
#include <string>

namespace artdaq {
namespace hdf5 {
/**
 * \brief Load a FragmentDataset plugin
 * \param pset ParameterSet used to configure the FragmentDataset
 * \param plugin_label Name of the plugin
 * \return Pointer to the new FragmentDataset instance
 */
std::unique_ptr<FragmentDataset>
MakeDatasetPlugin(const fhicl::ParameterSet& pset,
                  const std::string& plugin_label);
}  // namespace hdf5
}  // namespace artdaq

#endif
