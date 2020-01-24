#ifndef artdaq_demo_hdf5_ArtModules_detail_FragmentNtuple
#define artdaq_demo_hdf5_ArtModules_detail_FragmentNtuple 1

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

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

	void insert(artdaq::Fragment const& frag);

	void insert(artdaq::detail::RawEventHeader const& hdr);

private:
	size_t nWordsPerRow_;
	hep_hpc::hdf5::Ntuple<uint64Column, uint16Column, uint64Column, uint8Column, uint64Column, uint64Column, dataColumn> fragments_;
	hep_hpc::hdf5::Ntuple<uint32Column, uint32Column, uint32Column, uint64Column, uint8Column> eventHeaders_;
};
}  // namespace hdf5
}  // namespace artdaq
#endif  //artdaq_demo_hdf5_ArtModules_detail_FragmentNtuple
