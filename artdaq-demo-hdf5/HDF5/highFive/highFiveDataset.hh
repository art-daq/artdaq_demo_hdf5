#ifndef artdaq_demo_hdf5_HDF5_highFive_highFiveDataset_hh
#define artdaq_demo_hdf5_HDF5_highFive_highFiveDataset_hh 1

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

#include "artdaq-demo-hdf5/HDF5/highFive/HighFive/include/highfive/H5File.hpp"


namespace artdaq {
namespace hdf5 {

class HighFiveDataset : public FragmentDataset
{
public:
	HighFiveDataset(fhicl::ParameterSet const& ps);

	virtual ~HighFiveDataset();

	void insert(artdaq::Fragment const& frag) override;

	void insert(artdaq::detail::RawEventHeader const& hdr) override;

	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() override;

	std::unique_ptr<artdaq::detail::RawEventHeader> GetEventHeader(artdaq::Fragment::sequence_id_t const&) override;

private:
	std::unique_ptr<HighFive::File> file_;
	size_t headerIndex_;
	size_t fragmentIndex_;
};
}  // namespace hdf5
}  // namespace artdaq
#endif  //artdaq_demo_hdf5_HDF5_highFive_highFiveDataset_hh
