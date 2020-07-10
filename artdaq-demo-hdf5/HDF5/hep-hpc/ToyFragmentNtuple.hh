#ifndef artdaq_demo_hdf5_ArtModules_detail_ToyFragmentNtuple
#define artdaq_demo_hdf5_ArtModules_detail_ToyFragmentNtuple 1

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"
#include "artdaq-demo-hdf5/HDF5/hep-hpc/FragmentNtuple.hh"

#include "artdaq-core-demo/Overlays/FragmentType.hh"
#include "artdaq-core-demo/Overlays/ToyFragment.hh"

namespace artdaq {
namespace hdf5 {
/** 
 * @brief An implementation of FragmentDataset for ToyFragment. Uses a FragmentNtuple dataset plugin for any Fragments which are not ToyFragments.
 */
class ToyFragmentNtuple : public FragmentDataset
{
public:
	/**
	 * @brief ToyFragmentNtuple Constructor
	 * @param ps ParameterSet containing configuration for ToyFragmentNtuple
	 *
	 * ToyFragmentNtuple accepts the following Parameters:
	 * "mode" (Default: "write"): Mode to use for this FragmentDataset (only "write" mode is supported)
     * "nWordsPerRow" (Default: 10240): Number of ADC words to report on each row of the Ntuple
     * "fileName" (Default: "toyFragments.hdf5"): File name to use for output
	 */
	ToyFragmentNtuple(fhicl::ParameterSet const& ps);

	/**
	 * @brief ToyFragmentNtuple Destructor
	 */
	virtual ~ToyFragmentNtuple();

	/**
	 * @brief Insert a Fragment into the Fragment Ntuple Dataset (write it to the HDF5 file)
	 * @param f Fragment to insert
	 * 
	 * If the Fragment is not a ToyFragment, it will be inserted into the FragmentNtuple datset
	 */
	void insertOne(artdaq::Fragment const& f, std::string instance_name) override;

	/**
	 * @brief Insert a RawEventHeader into the Event Header Ntuple Dataset (write it to the HDF5 file)
	 * @param evtHdr RawEventHeader to insert
	 *
	 * This function simply forwards to FragmentNtuple::insertHeader
	 */
	void insertHeader(artdaq::detail::RawEventHeader const& evtHdr) override;

	/**
	 * @brief Read the next event from the Dataset (HDF5 file)
	 * @returns A Map of Fragment::type_t and pointers to Fragments, suitable for ArtdaqInput
	 *
	 * This function is not valid for this dataset plugin; it will log an error message and return an empty map
	 */
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() override
	{
		TLOG(TLVL_ERROR) << "ToyFragmentNtuple is not capable of reading!";
		return std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>>();
	}

	/**
	 * @brief Read a RawEventHeader from the Dataset (HDF5 file)
	 * @return Pointer to a RawEventHeader if a match was found in the Dataset, nullptr otherwise
	 *
	 * This function is not valid for this dataset plugin; it will log an error message and return nullptr
	 */
	std::unique_ptr<artdaq::detail::RawEventHeader> getEventHeader(artdaq::Fragment::sequence_id_t const&) override
	{
		TLOG(TLVL_ERROR) << "ToyFragmentNtuple is not capable of reading!";
		return nullptr;
	}

private:
	size_t nWordsPerRow_;
	hep_hpc::hdf5::Ntuple<uint64Column, uint16Column, uint64Column, uint8Column, uint16Column, uint8Column, uint32Column, uint8Column, uint32Column, uint32Column, uint16Column> output_;
	FragmentNtuple fragments_;
};
}  // namespace hdf5
}  // namespace artdaq
#endif  //artdaq_demo_hdf5_ArtModules_detail_ToyFragmentNtuple
