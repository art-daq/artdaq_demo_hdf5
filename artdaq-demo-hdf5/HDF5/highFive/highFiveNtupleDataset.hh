#ifndef artdaq_demo_hdf5_HDF5_highFive_highFiveNtupleDataset_hh
#define artdaq_demo_hdf5_HDF5_highFive_highFiveNtupleDataset_hh 1

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

#include "artdaq-demo-hdf5/HDF5/highFive/HighFive/include/highfive/H5File.hpp"
#include "artdaq-demo-hdf5/HDF5/highFive/highFiveDatasetHelper.hh"

#include <unordered_map>

namespace artdaq {
namespace hdf5 {

class HighFiveNtupleDataset : public FragmentDataset
{
public:
	HighFiveNtupleDataset(fhicl::ParameterSet const& ps);

	virtual ~HighFiveNtupleDataset() noexcept;

	void insertOne(Fragment const& frag) override;
	
	void insertHeader(detail::RawEventHeader const& hdr) override;
	
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() override;

	std::unique_ptr<artdaq::detail::RawEventHeader> getEventHeader(artdaq::Fragment::sequence_id_t const&) override;

private:
	std::unique_ptr<HighFive::File> file_;
	size_t headerIndex_;
	size_t fragmentIndex_;
	size_t nWordsPerRow_;

	std::unordered_map<std::string, std::unique_ptr<HighFiveDatasetHelper>> fragment_datasets_;
	std::unordered_map<std::string, std::unique_ptr<HighFiveDatasetHelper>> event_datasets_;
};
}  // namespace hdf5
}  // namespace artdaq
#endif  //artdaq_demo_hdf5_HDF5_highFive_HighFiveNtupleDataset_hh
