#ifndef artdaq_demo_hdf5_ArtModules_detail_ToyFragmentNtuple
#define artdaq_demo_hdf5_ArtModules_detail_ToyFragmentNtuple 1

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"
#include "artdaq-demo-hdf5/HDF5/hep-hpc/FragmentNtuple.hh"

#include "artdaq-core-demo/Overlays/FragmentType.hh"
#include "artdaq-core-demo/Overlays/ToyFragment.hh"

namespace artdaq {
namespace hdf5 {
class ToyFragmentNtuple : public FragmentDataset
{
public:
	ToyFragmentNtuple(fhicl::ParameterSet const& ps);

	virtual ~ToyFragmentNtuple();

	void insertOne(artdaq::Fragment const& f) override;
	
	void insertHeader(artdaq::detail::RawEventHeader const& evtHdr) override;
	
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() override
	{
		TLOG(TLVL_ERROR) << "ToyFragmentNtuple is not capable of reading!";
		return std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>>();
	}

	std::unique_ptr<artdaq::detail::RawEventHeader> GetEventHeader(artdaq::Fragment::sequence_id_t const&) override
	{
		TLOG(TLVL_ERROR) << "ToyFragmentNtuple is not capable of reading!";
		return nullptr;
	}

private:
	hep_hpc::hdf5::Ntuple<uint64Column, uint16Column, uint64Column, uint8Column, uint16Column, uint8Column, uint32Column, uint8Column, uint32Column, uint32Column, uint16Column> output_;
	FragmentNtuple fragments_;
	size_t nWordsPerRow_;
};
}  // namespace hdf5
}  // namespace artdaq
#endif  //artdaq_demo_hdf5_ArtModules_detail_ToyFragmentNtuple
