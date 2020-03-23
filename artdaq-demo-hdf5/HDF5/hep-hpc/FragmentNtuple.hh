#ifndef artdaq_demo_hdf5_ArtModules_detail_FragmentNtuple
#define artdaq_demo_hdf5_ArtModules_detail_FragmentNtuple 1

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-braces"
#include "hep_hpc/hdf5/Column.hpp"
#include "hep_hpc/hdf5/Ntuple.hpp"
#pragma GCC diagnostic pop

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

namespace artdaq {
namespace hdf5 {

typedef hep_hpc::hdf5::Column<uint8_t, 1> uint8Column;
typedef hep_hpc::hdf5::Column<uint16_t, 1> uint16Column;
typedef hep_hpc::hdf5::Column<uint32_t, 1> uint32Column;
typedef hep_hpc::hdf5::Column<uint64_t, 1> uint64Column;
typedef hep_hpc::hdf5::Column<artdaq::RawDataType, 1> dataColumn;

class FragmentNtuple : public FragmentDataset
{
public:
	FragmentNtuple(fhicl::ParameterSet const& ps, hep_hpc::hdf5::File const& file);

	FragmentNtuple(fhicl::ParameterSet const& ps);

	virtual ~FragmentNtuple();

	void insertOne(artdaq::Fragment const& frag) override;

	void insertHeader(artdaq::detail::RawEventHeader const& hdr) override;

	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() override
	{
		TLOG(TLVL_ERROR) << "FragmentNtuple is not capable of reading!";
		return std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>>();
	}

	std::unique_ptr<artdaq::detail::RawEventHeader> getEventHeader(artdaq::Fragment::sequence_id_t const&) override
	{
		TLOG(TLVL_ERROR) << "FragmentNtuple is not capable of reading!";
		return nullptr;
	}

private:
	size_t nWordsPerRow_;
	hep_hpc::hdf5::Ntuple<uint64Column, uint16Column, uint64Column, uint8Column, uint64Column, uint64Column, dataColumn> fragments_;
	hep_hpc::hdf5::Ntuple<uint32Column, uint32Column, uint32Column, uint64Column, uint8Column> eventHeaders_;
};
}  // namespace hdf5
}  // namespace artdaq
#endif  //artdaq_demo_hdf5_ArtModules_detail_FragmentNtuple
